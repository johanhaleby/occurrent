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
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.tree.Flag;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeUtils;

import java.util.List;

import static org.openrewrite.java.tree.J.ClassDeclaration.Kind.Type.Interface;

/**
 * Finds a class implementing the blocking or reactor {@code CheckpointStorage} that is missing the three-argument
 * {@code save} or {@code writeVersion} ADR 116 added, and inserts a stub for each missing member plus a review
 * comment, so the class compiles again. This is only the signature half of the 0.33.0 break, and the recipe cannot
 * know whether a store can evaluate a write condition for real, so the generated members are a best-effort answer,
 * not a finished one.
 * <p>
 * A class with its own two-argument {@code save}, the shape every genuine 0.32.0 implementer has, since the
 * two-argument method was abstract before 0.33.0, gets a three-argument {@code save} that delegates {@code any()}
 * to it and refuses only a stronger condition, with {@link UnsupportedOperationException} (a
 * {@link reactor.core.publisher.Mono#error(Throwable)} carrying one on the reactor stack). That is the same
 * permanent shape doc/migration/upgrading-to-0.33.0.md documents for a store that cannot evaluate a condition, not
 * a stopgap, and it is what keeps a wrapper-managed checkpoint write, which calls the three-argument {@code save}
 * with {@code any()} whenever no write-version source answers, working as soon as the stub is generated.
 * <p>
 * A class reachable only through {@code CheckpointStorage}'s own two-argument default (a partial hand-migration
 * that deleted its override, say) would recurse if it got that same delegating stub. The default calls the
 * three-argument method with {@code any()}, straight back into the generated stub, a {@link StackOverflowError} on
 * the first checkpoint write. Such a class gets the old always-refusing {@code save} instead, the shape this recipe
 * generated before delegation was added, refusing {@code any()} too since there is nothing safe to delegate it to.
 * That keeps the generated code from calling back into itself on any inheritance shape, at the cost of a checkpoint
 * write that always fails until the class gets its own two-argument {@code save}.
 * <p>
 * {@code writeVersion} always answers empty, for both shapes above. A store that evaluates a condition for real
 * still gets a review comment and is left marked for a manual pass, the same best-effort-plus-marker shape as
 * {@link MigrateEventStoreWriteStreamToList}. Java only, rewrite-kotlin has no recipe for inserting a member into a
 * class body, so a Kotlin implementer still needs the manual steps in
 * doc/migration/upgrading-to-0.33.0.md.
 */
public class AddCheckpointStorageConditionalWriteStubs extends Recipe {

    private static final String MARKER_TAG = "Occurrent 0.33 upgrade";

    private static final String BLOCKING_STORAGE = "org.occurrent.subscription.api.blocking.CheckpointStorage";
    private static final String REACTOR_STORAGE = "org.occurrent.subscription.api.reactor.CheckpointStorage";
    private static final String CHECKPOINT = "org.occurrent.subscription.Checkpoint";
    private static final String CHECKPOINT_WRITE_CONDITION = "org.occurrent.subscription.CheckpointWriteCondition";
    private static final String OPTIONAL_LONG = "java.util.OptionalLong";
    private static final String MONO = "reactor.core.publisher.Mono";

    private static final List<JavaType> SAVE_2_ARG_PARAMS =
            List.of(JavaType.ShallowClass.build("java.lang.String"), JavaType.ShallowClass.build(CHECKPOINT));
    private static final List<JavaType> SAVE_3_ARG_PARAMS =
            List.of(JavaType.ShallowClass.build("java.lang.String"), JavaType.ShallowClass.build(CHECKPOINT),
                    JavaType.ShallowClass.build(CHECKPOINT_WRITE_CONDITION));
    private static final List<JavaType> WRITE_VERSION_PARAMS =
            List.of(JavaType.ShallowClass.build("java.lang.String"));

    private static final String BLOCKING_SAVE_STUB = """
            /* TODO [%s]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                    throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                }
                return save(subscriptionId, checkpoint);
            }
            """.formatted(MARKER_TAG);

    // No own two-argument save to delegate to, only CheckpointStorage's own default, which calls this method right
    // back for any(). Delegating here the way the usual stub does would recurse, so this refuses unconditionally
    // instead, the shape this recipe generated before #731 added delegation.
    private static final String BLOCKING_SAVE_STUB_NO_OWN_TWO_ARG_SAVE = """
            /* TODO [%s]: this class has no own two-argument save, only the CheckpointStorage default, which calls this method for any(), so delegating any() here would recurse. This always refuses instead, even any(). Give the class its own two-argument save, or evaluate `condition` for real here. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ". It has no two-argument save to fall back on, so even any() is refused.");
            }
            """.formatted(MARKER_TAG);

    private static final String BLOCKING_WRITE_VERSION_STUB = """
            /* TODO [%s]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public OptionalLong writeVersion(String subscriptionId) {
                return OptionalLong.empty();
            }
            """.formatted(MARKER_TAG);

    private static final String REACTOR_SAVE_STUB = """
            /* TODO [%s]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                    return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
                }
                return save(subscriptionId, checkpoint);
            }
            """.formatted(MARKER_TAG);

    // Same no-own-save fallback as the blocking stub above, refusing unconditionally instead of delegating.
    private static final String REACTOR_SAVE_STUB_NO_OWN_TWO_ARG_SAVE = """
            /* TODO [%s]: this class has no own two-argument save, only the CheckpointStorage default, which calls this method for any(), so delegating any() here would recurse. This always refuses instead, even any(). Give the class its own two-argument save, or evaluate `condition` for real here. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ". It has no two-argument save to fall back on, so even any() is refused."));
            }
            """.formatted(MARKER_TAG);

    private static final String REACTOR_WRITE_VERSION_STUB = """
            /* TODO [%s]: this always answers an empty Mono, correct if this storage cannot evaluate a condition. Signal the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
            @Override
            public Mono<Long> writeVersion(String subscriptionId) {
                return Mono.empty();
            }
            """.formatted(MARKER_TAG);

    // JavaTemplate parses each stub with its own throwaway JavaParser, unrelated to whatever classpath the source
    // set being migrated carries, so a type it references (beyond the JDK) has to be taught to that parser
    // directly. These shapes only have to match the real types closely enough for a signature to bind, plus
    // CheckpointWriteCondition.Any and Mono.empty, which the stub bodies now reach through.
    private static final String CHECKPOINT_TYPE_STUB = """
            package org.occurrent.subscription;
            public interface Checkpoint {
            }
            """;
    private static final String CHECKPOINT_WRITE_CONDITION_TYPE_STUB = """
            package org.occurrent.subscription;
            public interface CheckpointWriteCondition {
                record Any() implements CheckpointWriteCondition {
                }
            }
            """;
    private static final String MONO_TYPE_STUB = """
            package reactor.core.publisher;
            public abstract class Mono<T> {
                public static <T> Mono<T> error(Throwable error) {
                    return null;
                }

                public static <T> Mono<T> empty() {
                    return null;
                }
            }
            """;

    @Override
    public String getDisplayName() {
        return "Stub the `CheckpointStorage` conditional write members";
    }

    @Override
    public String getDescription() {
        return "Finds a class implementing the blocking or reactor `CheckpointStorage` that is missing the " +
               "three-argument `save` or `writeVersion` added for a fenced checkpoint write (ADR 116), and " +
               "inserts a stub for each missing member, marked with a review comment, so the class compiles " +
               "again. A class with its own two-argument `save` gets a three-argument `save` that delegates " +
               "`any()` to it and refuses a stronger condition. A class with no own two-argument `save`, only " +
               "the interface default, gets one that always refuses, since delegating there would call back " +
               "into the interface default and recurse. `writeVersion` always answers empty, the permanent " +
               "shape for a store that cannot evaluate a condition. Evaluating a condition for real is still a " +
               "manual pass, see doc/migration/upgrading-to-0.33.0.md. Java only, rewrite-kotlin has no recipe " +
               "for inserting a member into a class body, so a Kotlin implementer needs the manual steps instead.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            @Override
            public J.ClassDeclaration visitClassDeclaration(J.ClassDeclaration classDecl, ExecutionContext ctx) {
                J.ClassDeclaration cd = super.visitClassDeclaration(classDecl, ctx);

                // rewrite-kotlin represents an ordinary Kotlin class with this same J.ClassDeclaration, so a type
                // check alone is not enough to stay Java-only. The inserted template is Java method syntax, wrong
                // for a Kotlin file, so require the enclosing source to be a genuine Java compilation unit and
                // leave a Kotlin implementer to the manual steps in doc/migration/upgrading-to-0.33.0.md.
                if (getCursor().firstEnclosing(J.CompilationUnit.class) == null) {
                    return cd;
                }

                // Only a concrete class has to answer every member. An interface or an abstract class is free to
                // stay abstract, and whichever concrete class eventually extends it is visited on its own.
                if (cd.getKind() == Interface || cd.hasModifier(J.Modifier.Type.Abstract)) {
                    return cd;
                }

                if (TypeUtils.isAssignableTo(BLOCKING_STORAGE, cd.getType())) {
                    String saveTemplate = hasOwnConcreteImplementation(cd, BLOCKING_STORAGE, "save", SAVE_2_ARG_PARAMS)
                            ? BLOCKING_SAVE_STUB
                            : BLOCKING_SAVE_STUB_NO_OWN_TWO_ARG_SAVE;
                    cd = stub(cd, BLOCKING_STORAGE, "save", SAVE_3_ARG_PARAMS, saveTemplate, CHECKPOINT, CHECKPOINT_WRITE_CONDITION);
                    cd = stub(cd, BLOCKING_STORAGE, "writeVersion", WRITE_VERSION_PARAMS, BLOCKING_WRITE_VERSION_STUB, OPTIONAL_LONG);
                } else if (TypeUtils.isAssignableTo(REACTOR_STORAGE, cd.getType())) {
                    String saveTemplate = hasOwnConcreteImplementation(cd, REACTOR_STORAGE, "save", SAVE_2_ARG_PARAMS)
                            ? REACTOR_SAVE_STUB
                            : REACTOR_SAVE_STUB_NO_OWN_TWO_ARG_SAVE;
                    cd = stub(cd, REACTOR_STORAGE, "save", SAVE_3_ARG_PARAMS, saveTemplate, CHECKPOINT, CHECKPOINT_WRITE_CONDITION, MONO);
                    cd = stub(cd, REACTOR_STORAGE, "writeVersion", WRITE_VERSION_PARAMS, REACTOR_WRITE_VERSION_STUB, MONO);
                }
                return cd;
            }

            // Inserts template as the class's last member unless `methodName`/`paramTypes` is already concretely
            // implemented below `capabilityInterfaceFqn`, which is what makes a second run over an already-stubbed
            // (or hand-migrated) class a no-op, and what keeps an inherited implementation from an in-source
            // abstract base from being overridden by a generated stub.
            private J.ClassDeclaration stub(J.ClassDeclaration cd, String capabilityInterfaceFqn, String methodName,
                                             List<JavaType> paramTypes, String template, String... imports) {
                if (hasOwnConcreteImplementation(cd, capabilityInterfaceFqn, methodName, paramTypes)) {
                    return cd;
                }

                for (String fqn : imports) {
                    maybeAddImport(fqn);
                }
                // contextSensitive binds the inserted method's owner to the class it lands in, which is what lets
                // hasOwnConcreteImplementation's own-body check recognise it as already declared on a later cycle
                // instead of inserting a second copy.
                J.Block newBody = JavaTemplate.builder(template)
                        .contextSensitive()
                        .imports(imports)
                        .javaParser(JavaParser.fromJavaVersion()
                                .dependsOn(CHECKPOINT_TYPE_STUB, CHECKPOINT_WRITE_CONDITION_TYPE_STUB, MONO_TYPE_STUB))
                        .build()
                        .apply(new Cursor(getCursor(), cd.getBody()), cd.getBody().getCoordinates().lastStatement());
                return cd.withBody(newBody);
            }

            // Whether `methodName`/`paramTypes` is concretely implemented for `cd`, either on `cd` itself or on a
            // supertype other than `capabilityInterfaceFqn`. `cd`'s own body is walked directly rather than through
            // `cd.getType()`, since a method this same visit (or an earlier cycle) inserted into the body is not
            // reflected by `cd.getType()`'s method list, only by the body itself. The supertype chain, in contrast,
            // is never mutated by this recipe (an abstract class is never stubbed), so its type model stays
            // reliable, and TypeUtils.findDeclaredMethod there finds either an override, or, once nothing
            // overrides the method, `capabilityInterfaceFqn`'s own declaration (abstract for the three-argument
            // save and writeVersion, a non-abstract default for the two-argument save). Filtering that out, by
            // owner and by the abstract flag, is what tells a supertype override from nothing overriding the
            // method at all.
            private boolean hasOwnConcreteImplementation(J.ClassDeclaration cd, String capabilityInterfaceFqn, String methodName,
                                                           List<JavaType> paramTypes) {
                boolean declaredOnClassItself = cd.getBody().getStatements().stream()
                        .filter(J.MethodDeclaration.class::isInstance)
                        .map(J.MethodDeclaration.class::cast)
                        .map(J.MethodDeclaration::getMethodType)
                        .anyMatch(mt -> mt != null && methodName.equals(mt.getName()) && sameParameterTypes(mt.getParameterTypes(), paramTypes));
                if (declaredOnClassItself) {
                    return true;
                }

                JavaType.FullyQualified fq = TypeUtils.asFullyQualified(cd.getType());
                if (fq == null) {
                    return false;
                }
                if (concretelyDeclaredBelow(fq.getSupertype(), capabilityInterfaceFqn, methodName, paramTypes)) {
                    return true;
                }
                // cd's own directly-implemented interfaces, not only reachable through a superclass: a class that
                // implements a capability interface of its own (extending CheckpointStorage with a default for
                // this member) rather than CheckpointStorage directly has its real implementation here.
                for (JavaType.FullyQualified i : fq.getInterfaces()) {
                    if (concretelyDeclaredBelow(i, capabilityInterfaceFqn, methodName, paramTypes)) {
                        return true;
                    }
                }
                return false;
            }

            // An interface default method carries both the Abstract and the Default flag in this type model, not
            // Default alone, so excluding every Abstract-flagged method would also exclude a genuine default. Only
            // a method that is Abstract without also being Default has no body to fall back on.
            private boolean concretelyDeclaredBelow(JavaType.@Nullable FullyQualified type, String capabilityInterfaceFqn,
                                                      String methodName, List<JavaType> paramTypes) {
                return TypeUtils.findDeclaredMethod(type, methodName, paramTypes)
                        .filter(m -> !m.getFlags().contains(Flag.Abstract) || m.getFlags().contains(Flag.Default))
                        .filter(m -> !capabilityInterfaceFqn.equals(m.getDeclaringType().getFullyQualifiedName()))
                        .isPresent();
            }

            private boolean sameParameterTypes(List<JavaType> actual, List<JavaType> expected) {
                if (actual.size() != expected.size()) {
                    return false;
                }
                for (int i = 0; i < actual.size(); i++) {
                    if (!TypeUtils.isOfType(actual.get(i), expected.get(i))) {
                        return false;
                    }
                }
                return true;
            }
        };
    }
}
