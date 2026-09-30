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
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.RandomizeIdVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JContainer;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.internal.ListUtils;
import org.openrewrite.marker.Markers;

import java.util.Collections;
import java.util.List;

/**
 * Changes a Java {@code void cancelSubscription(String)} that implements the reactor {@code CancellableSubscriptions}
 * or {@code DcbSubscriptionModel} to return {@code Mono<Void>}, the return type both interfaces declare from 0.34.0.
 * Each {@code return;} in the method itself becomes {@code return Mono.empty();}, and a body that can run off its end
 * gets {@code return Mono.empty();} as its last statement. That keeps what the method did before, which is cleaning
 * up synchronously and reporting nothing. See doc/migration/upgrading-to-0.34.0.md for an implementation that deletes
 * stored state asynchronously, which has to return a {@code Mono} that completes once that delete has.
 */
public class MigrateReactorCancelSubscriptionReturnType extends Recipe {

    private static final String CANCELLABLE_SUBSCRIPTIONS = "org.occurrent.subscription.api.reactor.CancellableSubscriptions";
    private static final String DCB_SUBSCRIPTION_MODEL = "org.occurrent.subscription.api.reactor.DcbSubscriptionModel";
    private static final String MONO = "reactor.core.publisher.Mono";
    private static final String TARGET = "reactorCancelSubscriptionReturningVoid";

    // Parsed with a stub of Mono because this parser does not see the classpath of the source being migrated
    private static final String TYPED_RETURN_SOURCE = """
            package reactor.core.publisher;
            public abstract class Mono<T> {
                public static <T> Mono<T> empty() {
                    return null;
                }
                static Mono<Void> cancelled() {
                    return Mono.empty();
                }
            }
            """;

    @Override
    public String getDisplayName() {
        return "Return `Mono<Void>` from a reactor `cancelSubscription(String)`";
    }

    @Override
    public String getDescription() {
        return "The reactor `CancellableSubscriptions.cancelSubscription(String)` and " +
               "`DcbSubscriptionModel.cancelSubscription(String)` return `Mono<Void>` from 0.34.0. This changes a Java " +
               "implementation that returns `void` to return `Mono<Void>`, turns each `return;` in it into " +
               "`return Mono.empty();`, and adds `return Mono.empty();` at the end of a body that can run off its " +
               "end. An implementation that deletes stored state asynchronously still has to return a `Mono` that " +
               "completes once that delete has, see doc/migration/upgrading-to-0.34.0.md. Java only, a Kotlin " +
               "implementation needs the manual steps instead.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            private J.Return typedReturn;

            @Override
            public J.MethodDeclaration visitMethodDeclaration(J.MethodDeclaration method, ExecutionContext ctx) {
                if (!isVoidReactorCancelSubscription(method)) {
                    return super.visitMethodDeclaration(method, ctx);
                }
                getCursor().putMessage(TARGET, true);
                J.MethodDeclaration md = super.visitMethodDeclaration(method, ctx);

                J.Block body = md.getBody();
                if (body != null && canRunOffTheEnd(body)) {
                    List<Statement> statements = body.getStatements();
                    if (statements.isEmpty()) {
                        J.Block withReturn = body.withStatements(List.of(returnMonoEmpty(Space.format("\n"))));
                        md = md.withBody(autoFormat(withReturn, ctx, new Cursor(getCursor(), md)));
                    } else {
                        Space lastPrefix = statements.get(statements.size() - 1).getPrefix();
                        md = md.withBody(body.withStatements(ListUtils.concat(statements, returnMonoEmpty(Space.format(lastPrefix.getWhitespace())))));
                    }
                }
                maybeAddImport(MONO);
                return md.withReturnTypeExpression(monoOfVoid(md.getReturnTypeExpression() == null ? Space.EMPTY : md.getReturnTypeExpression().getPrefix()))
                        .withMethodType(md.getMethodType() == null ? null : md.getMethodType().withReturnType(monoOfVoidType()));
            }

            @Override
            public J.Return visitReturn(J.Return aReturn, ExecutionContext ctx) {
                J.Return r = super.visitReturn(aReturn, ctx);
                if (r.getExpression() != null) {
                    return r;
                }
                // A return inside a lambda or a local class belongs to that code rather than to the method itself
                Cursor owner = getCursor().dropParentUntil(parent -> parent instanceof J.MethodDeclaration
                                                                     || parent instanceof J.Lambda
                                                                     || parent instanceof J.ClassDeclaration
                                                                     || parent instanceof J.NewClass
                                                                     || parent == Cursor.ROOT_VALUE);
                if (!(owner.getValue() instanceof J.MethodDeclaration) || owner.getMessage(TARGET) == null) {
                    return r;
                }
                return returnMonoEmpty(r.getPrefix()).withMarkers(r.getMarkers());
            }

            private boolean isVoidReactorCancelSubscription(J.MethodDeclaration method) {
                // rewrite-kotlin reuses J.MethodDeclaration, and the Java template below is wrong for a Kotlin file
                if (getCursor().firstEnclosing(J.CompilationUnit.class) == null) {
                    return false;
                }
                if (!"cancelSubscription".equals(method.getSimpleName())) {
                    return false;
                }
                if (!(method.getReturnTypeExpression() instanceof J.Primitive primitive) || primitive.getType() != JavaType.Primitive.Void) {
                    return false;
                }
                JavaType.Method methodType = method.getMethodType();
                if (methodType == null || methodType.getParameterTypes().size() != 1 || !TypeUtils.isString(methodType.getParameterTypes().get(0))) {
                    return false;
                }
                JavaType.FullyQualified declaringType = methodType.getDeclaringType();
                return TypeUtils.isAssignableTo(CANCELLABLE_SUBSCRIPTIONS, declaringType)
                       || TypeUtils.isAssignableTo(DCB_SUBSCRIPTION_MODEL, declaringType);
            }

            private boolean canRunOffTheEnd(J.Block body) {
                List<Statement> statements = body.getStatements();
                if (statements.isEmpty()) {
                    return true;
                }
                Statement last = statements.get(statements.size() - 1);
                return !(last instanceof J.Return) && !(last instanceof J.Throw);
            }

            private J.Return returnMonoEmpty(Space prefix) {
                if (typedReturn == null) {
                    J.CompilationUnit cu = (J.CompilationUnit) JavaParser.fromJavaVersion().build()
                            .parse(new InMemoryExecutionContext(), TYPED_RETURN_SOURCE)
                            .findFirst()
                            .orElseThrow();
                    J.MethodDeclaration cancelled = (J.MethodDeclaration) cu.getClasses().get(0).getBody().getStatements().get(1);
                    typedReturn = (J.Return) cancelled.getBody().getStatements().get(0);
                }
                J.Return copy = (J.Return) new RandomizeIdVisitor<Integer>().visitNonNull(typedReturn, 0);
                return copy.withPrefix(prefix);
            }

            private J.ParameterizedType monoOfVoid(Space prefix) {
                JavaType.FullyQualified monoType = JavaType.ShallowClass.build(MONO);
                JavaType.FullyQualified voidType = JavaType.ShallowClass.build("java.lang.Void");
                J.Identifier mono = new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY, Collections.emptyList(), "Mono", monoType, null);
                J.Identifier voidArgument = new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY, Collections.emptyList(), "Void", voidType, null);
                return new J.ParameterizedType(Tree.randomId(), prefix, Markers.EMPTY, mono,
                        JContainer.build(Space.EMPTY, List.of(JRightPadded.build(voidArgument)), Markers.EMPTY), monoOfVoidType());
            }

            private JavaType.Parameterized monoOfVoidType() {
                return new JavaType.Parameterized(null, JavaType.ShallowClass.build(MONO), List.of(JavaType.ShallowClass.build("java.lang.Void")));
            }
        };
    }
}
