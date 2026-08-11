/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.occurrent.rewrite;

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.*;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Migrates a saga timer's name from {@code String} to {@code TimerName} (ADR 121). Two kinds of construction are
 * rewritten because the recipe can prove what the old code meant. A {@code String} handed to
 * {@code SagaTimeout}'s second argument, or to the timer-name argument of {@code SagaEffect}'s three timer-effect
 * records, {@code StartTimeout}, {@code StartTimeoutAt} and {@code CancelTimeout}, becomes
 * {@code TimerName.parse(name)}, which is the value that string already named. A {@code timerName()} read off any
 * of those same four types, into a declared {@code String}, gains {@code .encode()}. Every other read of
 * {@code timerName()} is flagged with a review comment rather than rewritten, since the recipe cannot see what the
 * surrounding code wants the name for, the same best-effort-plus-marker shape as
 * {@link MigrateEventStoreWriteStreamToList}. Deconstructing a {@code SagaEffect}
 * timer record against a {@code String} component is left alone entirely, because a record pattern's binding type
 * is a judgement about the code that follows it and the compiler points at every one. Java only, a Kotlin caller
 * needs the manual steps in doc/migration/upgrading-to-0.33.0.md.
 */
public class MigrateSagaTimerName extends Recipe {

    // The marker tag is what makes re-runs idempotent: addReviewComment embeds it and isReviewComment looks for it,
    // so both must agree. Keeping it a single constant means a reworded marker can never silently stop being
    // recognized.
    private static final String MARKER_TAG = "Occurrent 0.33 upgrade";
    // A read of timerName() is usually an argument rather than a statement of its own, so the comment lands inline
    // and is kept short for it. What a reader does about it, including that a logging read needs nothing at all, is
    // in the guide the comment points at.
    private static final String REVIEW_MARKER =
            " TODO [" + MARKER_TAG + "]: timerName() is a TimerName now, call encode() for the string." +
            " See doc/migration/upgrading-to-0.33.0.md. ";

    private static final String SAGA_TIMEOUT = "org.occurrent.dsl.saga.SagaTimeout";
    private static final String TIMER_NAME = "org.occurrent.dsl.saga.TimerName";
    private static final String START_TIMEOUT = "org.occurrent.dsl.saga.SagaEffect$StartTimeout";
    private static final String START_TIMEOUT_AT = "org.occurrent.dsl.saga.SagaEffect$StartTimeoutAt";
    private static final String CANCEL_TIMEOUT = "org.occurrent.dsl.saga.SagaEffect$CancelTimeout";

    // SagaTimeout and the three timer-effect records each declare their own timerName() accessor rather than
    // sharing one, so a read off any of the four needs the same treatment.
    private static final List<MethodMatcher> TIMER_NAME_ACCESSORS = List.of(
            new MethodMatcher(SAGA_TIMEOUT + " timerName()"),
            new MethodMatcher(START_TIMEOUT + " timerName()"),
            new MethodMatcher(START_TIMEOUT_AT + " timerName()"),
            new MethodMatcher(CANCEL_TIMEOUT + " timerName()"));
    private static final MethodMatcher TIMER_NAME_PARSE = new MethodMatcher(TIMER_NAME + " parse(java.lang.String)");
    private static final MethodMatcher TIMER_NAME_ENCODE = new MethodMatcher(TIMER_NAME + " encode()");

    // JavaTemplate parses each template with its own throwaway JavaParser, unrelated to whatever classpath the
    // source set being migrated carries, so TimerName has to be taught to that parser directly. This shape only has
    // to match the real interface closely enough for parse and encode to bind.
    private static final String TIMER_NAME_TYPE_STUB = """
            package org.occurrent.dsl.saga;
            public interface TimerName {
                static TimerName parse(String name) {
                    return null;
                }

                String encode();
            }
            """;

    @Override
    public String getDisplayName() {
        return "Migrate a saga timer's name from `String` to `TimerName`";
    }

    @Override
    public String getDescription() {
        return "Rewrites a `String` handed to `SagaTimeout`'s second argument, or to the timer-name argument of " +
               "`SagaEffect.StartTimeout`, `StartTimeoutAt` or `CancelTimeout`, into `TimerName.parse(name)`, and " +
               "appends `encode()` to a `timerName()` read into a declared `String` (ADR 121). Any other read of " +
               "`timerName()` is flagged with a review comment instead, since the recipe cannot see what the " +
               "surrounding code wants the name for, and deconstructing a `SagaEffect` timer record against a " +
               "`String` component is left for a human. See doc/migration/upgrading-to-0.33.0.md. Java only, a " +
               "Kotlin caller needs the manual steps instead.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {

            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass n = super.visitNewClass(newClass, ctx);

                if (!isJavaSource()) {
                    return n;
                }

                // sagaId, timerName for SagaTimeout; timerName, after/at for the two duration-and-instant timer
                // effects; timerName alone for CancelTimeout. The index is where the String argument sits that a
                // 0.32.0 caller could have passed, and the arity guards against a call this isn't, rather than a
                // stray constructor overload on one of these four types.
                JavaType type = rawClassTypeOf(n);
                if (TypeUtils.isOfClassType(type, SAGA_TIMEOUT)) {
                    return migrateTimerNameArgument(n, 1, 2);
                }
                if (TypeUtils.isOfClassType(type, START_TIMEOUT) || TypeUtils.isOfClassType(type, START_TIMEOUT_AT)) {
                    return migrateTimerNameArgument(n, 0, 2);
                }
                if (TypeUtils.isOfClassType(type, CANCEL_TIMEOUT)) {
                    return migrateTimerNameArgument(n, 0, 1);
                }
                return n;
            }

            private J.NewClass migrateTimerNameArgument(J.NewClass n, int timerNameIndex, int arity) {
                List<Expression> args = n.getArguments();
                if (args.size() != arity) {
                    return n;
                }

                // Only the string form is the 0.32.0 call. An argument already carrying a TimerName, whether written
                // by hand or wrapped by an earlier run of this recipe, is not String-typed and falls through here,
                // which is what keeps a second cycle a no-op.
                Expression timerName = args.get(timerNameIndex);
                if (!TypeUtils.isString(timerName.getType()) || TIMER_NAME_PARSE.matches(timerName)) {
                    return n;
                }

                maybeAddImport(TIMER_NAME);
                // The argument goes into the template stripped of its own whitespace and the result is given that
                // whitespace back, so the rewritten call keeps the spacing the original had around its comma.
                Expression parsed = JavaTemplate.builder("TimerName.parse(#{any(java.lang.String)})")
                        .imports(TIMER_NAME)
                        .javaParser(JavaParser.fromJavaVersion().dependsOn(TIMER_NAME_TYPE_STUB))
                        .build()
                        .apply(new Cursor(getCursor(), timerName), timerName.getCoordinates().replace(),
                                timerName.withPrefix(Space.EMPTY));

                List<Expression> newArgs = new ArrayList<>(args);
                newArgs.set(timerNameIndex, parsed.withPrefix(timerName.getPrefix()));
                return n.withArguments(newArgs);
            }

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

                if (!isJavaSource() || TIMER_NAME_ACCESSORS.stream().noneMatch(accessor -> accessor.matches(m))) {
                    return m;
                }

                // A call this recipe already rewrote is the select of an encode() call. Leaving it alone is what
                // stops a second cycle appending a review comment to a name it had just finished migrating.
                Cursor parent = getCursor().getParentTreeCursor();
                if (parent.getValue() instanceof J.MethodInvocation enclosing && TIMER_NAME_ENCODE.matches(enclosing)) {
                    return m;
                }

                if (readIntoAString(parent)) {
                    J.MethodInvocation encoded = JavaTemplate.builder("#{any(" + TIMER_NAME + ")}.encode()")
                            .javaParser(JavaParser.fromJavaVersion().dependsOn(TIMER_NAME_TYPE_STUB))
                            .build()
                            .apply(new Cursor(getCursor(), m), m.getCoordinates().replace(), m.withPrefix(Space.EMPTY));
                    return encoded.withPrefix(m.getPrefix());
                }

                return alreadyMarked(m) ? m : addReviewComment(m);
            }

            // rewrite-kotlin represents a Kotlin call with the same J nodes the Java LST uses, and K.CompilationUnit
            // is not a J.CompilationUnit, so this is what keeps the Java-syntax templates off a Kotlin source. A
            // Kotlin caller is left to the manual steps in doc/migration/upgrading-to-0.33.0.md.
            private boolean isJavaSource() {
                return getCursor().firstEnclosing(J.CompilationUnit.class) != null;
            }

            // A mismatched constructor argument (a String where TimerName is now expected, exactly the 0.32.0 call
            // this recipe exists to fix) leaves type inference for a generic diamond constructor with nothing to
            // resolve against, and J.NewClass.getClazz().getType() comes back Unknown for the whole parameterized
            // type, not just the constructor. The raw type underneath the diamond, SagaEffect.StartTimeout on its
            // own without <>, is resolved independently of the constructor call and stays attributed either way.
            private JavaType rawClassTypeOf(J.NewClass n) {
                TypeTree clazz = n.getClazz();
                if (clazz instanceof J.ParameterizedType parameterized) {
                    NameTree rawClazz = parameterized.getClazz();
                    return rawClazz == null ? null : rawClazz.getType();
                }
                return typeOf(clazz);
            }

            private JavaType typeOf(TypeTree clazz) {
                return clazz == null ? null : clazz.getType();
            }

            // The three positions where the wanted type is written down in the source rather than inferred from a
            // resolved signature. Everything else, an argument to a method, a receiver, an operand, is a judgement
            // the recipe declines to make, because the call now compiles against an Object parameter in some of
            // those positions and does not compile at all in others.
            private boolean readIntoAString(Cursor parent) {
                Object enclosing = parent.getValue();
                if (enclosing instanceof J.VariableDeclarations.NamedVariable) {
                    return parent.getParentTreeCursor().getValue() instanceof J.VariableDeclarations declaration
                           && isString(declaration.getTypeExpression());
                }
                if (enclosing instanceof J.Assignment assignment) {
                    return TypeUtils.isString(assignment.getVariable().getType());
                }
                if (enclosing instanceof J.Return) {
                    return returnsString(parent);
                }
                return false;
            }

            // The method a return belongs to is the first one up the cursor, but only when nothing that carries its
            // own return type sits in between. A return inside a lambda or an anonymous class answers to that body,
            // not to the method the body is written in.
            private boolean returnsString(Cursor parent) {
                Iterator<Object> path = parent.getPath();
                while (path.hasNext()) {
                    Object node = path.next();
                    if (node instanceof J.Lambda || node instanceof J.NewClass) {
                        return false;
                    }
                    if (node instanceof J.MethodDeclaration declaration) {
                        return isString(declaration.getReturnTypeExpression());
                    }
                }
                return false;
            }

            private boolean isString(TypeTree type) {
                return type != null && TypeUtils.isString(type.getType());
            }

            private boolean alreadyMarked(J.MethodInvocation m) {
                return m.getPrefix().getComments().stream().anyMatch(this::isReviewComment);
            }

            private boolean isReviewComment(Comment comment) {
                return comment instanceof TextComment text && text.getText().contains(MARKER_TAG);
            }

            private J.MethodInvocation addReviewComment(J.MethodInvocation m) {
                Space prefix = m.getPrefix();
                List<Comment> comments = new ArrayList<>(prefix.getComments());
                // Re-indenting after the comment only makes sense when the call already starts a line. Otherwise a
                // single space keeps the comment and the call apart on the one line they share.
                String whitespace = prefix.getWhitespace();
                String suffix = whitespace.contains("\n") ? whitespace : " ";
                comments.add(new TextComment(true, REVIEW_MARKER, suffix, Markers.EMPTY));
                return m.withPrefix(prefix.withComments(comments));
            }
        };
    }
}
