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

import org.jspecify.annotations.Nullable;
import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.ListUtils;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.RandomizeIdVisitor;
import org.openrewrite.java.tree.Comment;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JContainer;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.java.tree.TextComment;
import org.openrewrite.java.tree.TypeTree;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Changes a Java {@code void cancelSubscription(String)} that implements the reactor {@code CancellableSubscriptions}
 * or {@code DcbSubscriptionModel} to return {@code Mono<Void>}, the return type both interfaces declare from 0.34.0.
 * It changes the body in one of three ways, each chosen so that the result compiles, and keeps what the method did
 * before, which is cleaning up synchronously and reporting nothing:
 * <ul>
 *     <li>A body that ends in a call to the {@code cancelSubscription(String)} of a model it wraps, or of its
 *     superclass, returns what that call returns.</li>
 *     <li>A body that is empty, or ends in a statement that completes whenever it is reached, such as a method call
 *     or an assignment, gets {@code return Mono.empty();} as its last statement.</li>
 *     <li>Any other body, one ending in an {@code if}, a loop, a {@code try} or a {@code switch} for example, moves
 *     unchanged into a new private {@code void} method, which the method calls before it returns
 *     {@code Mono.empty()}. Whether such a body can run off its end takes the compiler's own analysis to decide, and
 *     moving it keeps the result compiling either way. The new method is named {@code doCancelSubscription}, or
 *     {@code doCancelSubscription2} and so on when code in the class can already call a method of that name without
 *     a qualifier. A class with a supertype the parser cannot see gets
 *     {@code cancelSubscriptionBodyBeforeOccurrent0340} instead, numbered the same way, since that supertype can have
 *     a public {@code doCancelSubscription(String)} that nothing in the class calls, and a private method of the same
 *     name and parameters would not compile.</li>
 * </ul>
 * A body ending in a {@code return} or a {@code throw} stays in place too. Each {@code return;} of a body that stays in
 * place becomes {@code return Mono.empty();}. A declaration with no body, abstract or in an interface that extends
 * either one, changes only its return type. A call to a wrapped
 * {@code cancelSubscription(String)} that the result does not return gets a {@code TODO} comment, since the
 * {@code Mono} returned then completes without waiting for that call's cleanup. See
 * doc/migration/upgrading-to-0.34.0.md for an implementation that deletes stored state asynchronously, which has to
 * return a {@code Mono} that completes once that delete has.
 */
public class MigrateReactorCancelSubscriptionReturnType extends Recipe {

    private static final String CANCELLABLE_SUBSCRIPTIONS = "org.occurrent.subscription.api.reactor.CancellableSubscriptions";
    private static final String DCB_SUBSCRIPTION_MODEL = "org.occurrent.subscription.api.reactor.DcbSubscriptionModel";
    private static final String MONO = "reactor.core.publisher.Mono";
    private static final String TARGET = "reactorCancelSubscriptionReturningVoid";
    private static final String HELPERS = "reactorCancelSubscriptionHelpers";
    private static final String HELPER_NAME = "doCancelSubscription";
    // For an owner with a supertype this parser cannot see, since any method that type declares can then have the name
    private static final String HELPER_NAME_BESIDE_AN_UNSEEN_SUPERTYPE = "cancelSubscriptionBodyBeforeOccurrent0340";
    private static final String RETURN_THE_WRAPPED_CANCEL = " TODO: return the Mono of the cancelSubscription call this method makes, so that the Mono returned here waits for its cleanup";

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

    private enum WrappedCancel {NONE, KNOWN, UNKNOWN}

    @Override
    public String getDisplayName() {
        return "Return `Mono<Void>` from a reactor `cancelSubscription(String)`";
    }

    @Override
    public String getDescription() {
        return "The reactor `CancellableSubscriptions.cancelSubscription(String)` and " +
               "`DcbSubscriptionModel.cancelSubscription(String)` return `Mono<Void>` from 0.34.0. This changes a Java " +
               "implementation that returns `void` to return `Mono<Void>`. A body ending in a call to the wrapped " +
               "model's or the superclass's `cancelSubscription(String)` returns that call. A body ending in a " +
               "statement that completes whenever it is reached gets `return Mono.empty();` at its end, and any other body that " +
               "does not end in a `return` or a `throw` moves into a new private `void` method that the method calls before returning " +
               "`Mono.empty()`, named so that it hides no method the class could already call, and so that no " +
               "method of a supertype the parser cannot see plausibly has the name. A declaration " +
               "with no body only changes its return type. A wrapped `cancelSubscription(String)` call that is not " +
               "returned gets a `TODO` " +
               "comment. An implementation that deletes stored state asynchronously still has to return a `Mono` " +
               "that completes once that delete has, see doc/migration/upgrading-to-0.34.0.md. Java only, a Kotlin " +
               "implementation needs the manual steps instead.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            private J.Return typedReturn;

            @Override
            public J.ClassDeclaration visitClassDeclaration(J.ClassDeclaration classDecl, ExecutionContext ctx) {
                J.ClassDeclaration cd = super.visitClassDeclaration(classDecl, ctx);
                Map<UUID, J.MethodDeclaration> helpers = getCursor().getMessage(HELPERS);
                return helpers == null ? cd : cd.withBody(withHelpers(cd.getBody(), helpers));
            }

            @Override
            public J.NewClass visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
                J.NewClass nc = super.visitNewClass(newClass, ctx);
                Map<UUID, J.MethodDeclaration> helpers = getCursor().getMessage(HELPERS);
                return helpers == null || nc.getBody() == null ? nc : nc.withBody(withHelpers(nc.getBody(), helpers));
            }

            @Override
            public J.MethodDeclaration visitMethodDeclaration(J.MethodDeclaration method, ExecutionContext ctx) {
                if (!isVoidReactorCancelSubscription(method)) {
                    return super.visitMethodDeclaration(method, ctx);
                }
                if (method.getBody() == null) {
                    // Abstract or declared by an interface, so only the return type changes
                    maybeAddImport(MONO);
                    return returningMonoOfVoid(super.visitMethodDeclaration(method, ctx));
                }
                List<Statement> original = method.getBody().getStatements();
                Statement last = original.isEmpty() ? null : original.get(original.size() - 1);
                boolean returnsTheWrappedCancel = last instanceof J.MethodInvocation invocation && wrappedCancel(invocation) == WrappedCancel.KNOWN;
                boolean flagged = wrappedCancelCalls(method.getBody()) > (returnsTheWrappedCancel ? 1 : 0);

                J.MethodDeclaration md;
                if (last == null || last instanceof J.Return || last instanceof J.Throw || returnsTheWrappedCancel || completesWheneverReached(last)) {
                    getCursor().putMessage(TARGET, true);
                    md = super.visitMethodDeclaration(method, ctx);
                    J.Block body = md.getBody();
                    List<Statement> statements = body.getStatements();
                    if (statements.isEmpty()) {
                        J.Block withReturn = body.withStatements(List.of(returnMonoEmpty(Space.format("\n"))));
                        md = md.withBody(autoFormat(withReturn, ctx, new Cursor(getCursor(), md)));
                    } else if (returnsTheWrappedCancel) {
                        md = md.withBody(body.withStatements(ListUtils.mapLast(statements, statement -> returnTheCall((J.MethodInvocation) statement, flagged))));
                    } else if (!(last instanceof J.Return) && !(last instanceof J.Throw)) {
                        J.Return returnEmpty = returnMonoEmpty(Space.format(last.getPrefix().getWhitespace()));
                        md = md.withBody(body.withStatements(ListUtils.concat(statements, flagged ? withTodo(returnEmpty) : returnEmpty)));
                    } else if (flagged) {
                        md = md.withBody(body.withStatements(ListUtils.mapFirst(statements, this::withTodo)));
                    }
                } else {
                    // The returns in it stay as they are, since they now return from the new void method
                    md = super.visitMethodDeclaration(method, ctx);
                    Cursor owner = getCursor().dropParentUntil(parent -> parent instanceof J.ClassDeclaration || parent instanceof J.NewClass);
                    Map<UUID, J.MethodDeclaration> helpers = owner.computeMessageIfAbsent(HELPERS, __ -> new LinkedHashMap<>());
                    J.MethodDeclaration helper = helper(md, helperName(owner, helpers.values()));
                    helpers.put(md.getId(), helper);
                    J.Block body = md.getBody().withStatements(List.of(callTo(helper, md), returnMonoEmpty(Space.format("\n"))));
                    body = autoFormat(body, ctx, new Cursor(getCursor(), md));
                    md = md.withBody(flagged ? body.withStatements(ListUtils.mapFirst(body.getStatements(), this::withTodo)) : body);
                }
                maybeAddImport(MONO);
                return returningMonoOfVoid(md);
            }

            private J.MethodDeclaration returningMonoOfVoid(J.MethodDeclaration md) {
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
                // rewrite-kotlin reuses J.MethodDeclaration, and what this builds is Java
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
                return isReactorCancellable(methodType.getDeclaringType());
            }

            private boolean isReactorCancellable(JavaType type) {
                return TypeUtils.isAssignableTo(CANCELLABLE_SUBSCRIPTIONS, type) || TypeUtils.isAssignableTo(DCB_SUBSCRIPTION_MODEL, type);
            }

            // UNKNOWN when the call has no type, so it may or may not return the Mono of a wrapped model
            private WrappedCancel wrappedCancel(J.MethodInvocation invocation) {
                if (!"cancelSubscription".equals(invocation.getSimpleName())
                    || invocation.getArguments().size() != 1 || invocation.getArguments().get(0) instanceof J.Empty) {
                    return WrappedCancel.NONE;
                }
                JavaType.Method type = invocation.getMethodType();
                if (type == null) {
                    return WrappedCancel.UNKNOWN;
                }
                return isReactorCancellable(type.getDeclaringType()) ? WrappedCancel.KNOWN : WrappedCancel.NONE;
            }

            private int wrappedCancelCalls(J.Block body) {
                List<J.MethodInvocation> calls = new ArrayList<>();
                new JavaIsoVisitor<List<J.MethodInvocation>>() {
                    @Override
                    public J.MethodInvocation visitMethodInvocation(J.MethodInvocation invocation, List<J.MethodInvocation> found) {
                        if (wrappedCancel(invocation) != WrappedCancel.NONE) {
                            found.add(invocation);
                        }
                        return super.visitMethodInvocation(invocation, found);
                    }
                }.visit(body, calls);
                return calls.size();
            }

            // A statement the compiler lets complete whenever it can be reached, so a return after it is reachable too
            private boolean completesWheneverReached(Statement statement) {
                return statement instanceof J.MethodInvocation
                       || statement instanceof J.Assignment
                       || statement instanceof J.AssignmentOperation
                       || statement instanceof J.Unary
                       || statement instanceof J.NewClass
                       || statement instanceof J.VariableDeclarations
                       || statement instanceof J.Empty
                       || statement instanceof J.ClassDeclaration
                       || statement instanceof J.Assert;
            }

            private Statement returnTheCall(J.MethodInvocation call, boolean flagged) {
                J.MethodInvocation returning = call.withPrefix(Space.SINGLE_SPACE)
                        .withMethodType(call.getMethodType() == null ? null : call.getMethodType().withReturnType(monoOfVoidType()));
                J.Return aReturn = new J.Return(Tree.randomId(), call.getPrefix(), Markers.EMPTY, returning);
                return flagged ? withTodo(aReturn) : aReturn;
            }

            private <S extends Statement> S withTodo(S statement) {
                Space prefix = statement.getPrefix();
                String whitespace = prefix.getWhitespace();
                String indent = whitespace.substring(whitespace.lastIndexOf('\n') + 1);
                List<Comment> comments = new ArrayList<>();
                comments.add(new TextComment(false, RETURN_THE_WRAPPED_CANCEL, "\n" + indent, Markers.EMPTY));
                comments.addAll(prefix.getComments());
                return statement.withPrefix(Space.build(whitespace, comments));
            }

            // A method added to the owner hides every method of the same name that code in the owner calls without a
            // qualifier, one an enclosing class has or a static import brings in for example. Such a call would then run
            // the new method or stop compiling, so the name is one none of them use.
            //
            // A supertype of the owner this parser cannot see can have a method of any name that nothing here calls, and
            // a private method of the same name and parameters would narrow its access and stop compiling. The name is
            // then one that no code written by hand plausibly has.
            private String helperName(Cursor owner, Iterable<J.MethodDeclaration> chosen) {
                Set<String> taken = new HashSet<>();
                boolean everySupertypeOfTheOwnerSeen = memberNames(owner.getValue(), taken);
                for (Cursor enclosing = owner.getParent(); enclosing != null; enclosing = enclosing.getParent()) {
                    if (enclosing.getValue() instanceof J.ClassDeclaration || enclosing.getValue() instanceof J.NewClass) {
                        memberNames(enclosing.getValue(), taken);
                    }
                }
                J.CompilationUnit compilationUnit = owner.firstEnclosing(J.CompilationUnit.class);
                if (compilationUnit != null) {
                    compilationUnit.getImports().stream().filter(J.Import::isStatic).forEach(anImport -> staticallyImportedNames(anImport, taken));
                }
                // Covers a call the names above miss, one a static import on demand of a type this parser cannot see
                // resolves for example
                new JavaIsoVisitor<Set<String>>() {
                    @Override
                    public J.MethodInvocation visitMethodInvocation(J.MethodInvocation invocation, Set<String> names) {
                        if (invocation.getSelect() == null) {
                            names.add(invocation.getSimpleName());
                        }
                        return super.visitMethodInvocation(invocation, names);
                    }
                }.visit((J) owner.getValue(), taken);
                chosen.forEach(helper -> taken.add(helper.getSimpleName()));
                String base = everySupertypeOfTheOwnerSeen ? HELPER_NAME : HELPER_NAME_BESIDE_AN_UNSEEN_SUPERTYPE;
                String name = base;
                for (int suffix = 2; taken.contains(name); suffix++) {
                    name = base + suffix;
                }
                return name;
            }

            // Answers whether this parser could see every supertype, and so every method name the class inherits
            private boolean memberNames(Object classOrAnonymousClass, Set<String> names) {
                J.Block body = classOrAnonymousClass instanceof J.ClassDeclaration cd ? cd.getBody() : ((J.NewClass) classOrAnonymousClass).getBody();
                JavaType type = classOrAnonymousClass instanceof J.ClassDeclaration cd ? cd.getType() : ((J.NewClass) classOrAnonymousClass).getType();
                if (body != null) {
                    body.getStatements().stream()
                            .filter(J.MethodDeclaration.class::isInstance)
                            .forEach(statement -> names.add(((J.MethodDeclaration) statement).getSimpleName()));
                }
                boolean namedSupertypesSeen = namedSupertypes(classOrAnonymousClass).stream().allMatch(this::seen);
                return inheritedMethodNames(type, names, new HashSet<>()) && namedSupertypesSeen;
            }

            // What the declaration names after extends and implements, or after new for an anonymous class
            private List<TypeTree> namedSupertypes(Object classOrAnonymousClass) {
                List<TypeTree> named = new ArrayList<>();
                if (classOrAnonymousClass instanceof J.ClassDeclaration cd) {
                    if (cd.getExtends() != null) {
                        named.add(cd.getExtends());
                    }
                    if (cd.getImplements() != null) {
                        named.addAll(cd.getImplements());
                    }
                } else if (((J.NewClass) classOrAnonymousClass).getClazz() != null) {
                    named.add(((J.NewClass) classOrAnonymousClass).getClazz());
                }
                return named;
            }

            private boolean seen(TypeTree typeTree) {
                return typeTree.getType() != null && !(typeTree.getType() instanceof JavaType.Unknown);
            }

            private void staticallyImportedNames(J.Import anImport, Set<String> names) {
                String name = anImport.getQualid().getSimpleName();
                if (!"*".equals(name)) {
                    names.add(name);
                    return;
                }
                inheritedMethodNames(anImport.getQualid().getTarget().getType(), names, new HashSet<>());
            }

            // Answers false when a type in the hierarchy is one this parser cannot see, whose methods it cannot name
            private boolean inheritedMethodNames(@Nullable JavaType type, Set<String> names, Set<String> visited) {
                if (type instanceof JavaType.Unknown) {
                    return false;
                }
                JavaType.FullyQualified fullyQualified = TypeUtils.asFullyQualified(type);
                if (fullyQualified == null || !visited.add(fullyQualified.getFullyQualifiedName())) {
                    return true;
                }
                fullyQualified.getMethods().forEach(method -> names.add(method.getName()));
                boolean seen = inheritedMethodNames(fullyQualified.getSupertype(), names, visited);
                for (JavaType.FullyQualified anInterface : fullyQualified.getInterfaces()) {
                    seen &= inheritedMethodNames(anInterface, names, visited);
                }
                return seen;
            }

            // The method as it was, with its body, parameters and throws clause, made private and renamed
            private J.MethodDeclaration helper(J.MethodDeclaration method, String name) {
                J.MethodDeclaration copy = (J.MethodDeclaration) new RandomizeIdVisitor<Integer>().visitNonNull(method, 0);
                JavaType.Method type = copy.getMethodType() == null ? null : copy.getMethodType().withName(name);
                String whitespace = method.getPrefix().getWhitespace();
                String indent = whitespace.substring(whitespace.lastIndexOf('\n') + 1);
                J.Modifier privateModifier = new J.Modifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY, null, J.Modifier.Type.Private, Collections.emptyList());
                return copy.withPrefix(Space.format("\n\n" + indent))
                        .withLeadingAnnotations(Collections.emptyList())
                        .withModifiers(List.of(privateModifier))
                        .withName(copy.getName().withSimpleName(name).withType(type))
                        .withMethodType(type);
            }

            private J.MethodInvocation callTo(J.MethodDeclaration helper, J.MethodDeclaration method) {
                J.VariableDeclarations parameter = (J.VariableDeclarations) method.getParameters().get(0);
                J.Identifier argument = parameter.getVariables().get(0).getName().withId(Tree.randomId()).withPrefix(Space.EMPTY);
                J.Identifier name = new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY, Collections.emptyList(), helper.getSimpleName(), helper.getMethodType(), null);
                return new J.MethodInvocation(Tree.randomId(), Space.format("\n"), Markers.EMPTY, null, null, name,
                        JContainer.build(Space.EMPTY, List.of(JRightPadded.<Expression>build(argument)), Markers.EMPTY), helper.getMethodType());
            }

            private J.Block withHelpers(J.Block body, Map<UUID, J.MethodDeclaration> helpers) {
                List<Statement> statements = new ArrayList<>();
                for (Statement statement : body.getStatements()) {
                    statements.add(statement);
                    J.MethodDeclaration helper = helpers.get(statement.getId());
                    if (helper != null) {
                        statements.add(helper);
                    }
                }
                return body.withStatements(statements);
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
