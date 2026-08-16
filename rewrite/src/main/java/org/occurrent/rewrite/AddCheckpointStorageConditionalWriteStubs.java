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
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;

import static org.openrewrite.java.tree.J.ClassDeclaration.Kind.Type.Interface;

/**
 * Finds a class implementing the blocking or reactor {@code CheckpointStorage} that is missing the three-argument
 * {@code save} or {@code writeVersion} ADR 116 added, and inserts a stub for each missing member plus a review
 * comment, so the class compiles again. This is only the signature half of the 0.33.0 break. The recipe cannot know
 * whether a store can evaluate a write condition for real, so the generated {@code save} delegates {@code any()} to
 * the class's own two-argument {@code save} and refuses only a stronger condition, with
 * {@link UnsupportedOperationException} (a {@link reactor.core.publisher.Mono#error(Throwable)} carrying one on the
 * reactor stack), and {@code writeVersion} always answers empty. That is the same permanent shape
 * doc/migration/upgrading-to-0.33.0.md documents for a store that cannot evaluate a condition, not a stopgap, and
 * it is what keeps a wrapper-managed checkpoint write, which calls the three-argument {@code save} with {@code
 * any()} whenever no write-version source answers, working the moment the stub lands. A store that can evaluate a
 * real condition still gets a review comment and is left marked for a manual pass, the same best-effort-plus-marker
 * shape as {@link MigrateEventStoreWriteStreamToList}. Java only, rewrite-kotlin has no recipe for inserting a
 * member into a class body, so a Kotlin implementer still needs the manual steps in
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

    // matchOverrides=true also matches a class whose implementation of the method comes from an in-source
    // supertype, not only a class that names CheckpointStorage on its own implements clause.
    private static final MethodMatcher BLOCKING_SAVE = new MethodMatcher(
            BLOCKING_STORAGE + " save(java.lang.String, " + CHECKPOINT + ", " + CHECKPOINT_WRITE_CONDITION + ")", true);
    private static final MethodMatcher BLOCKING_WRITE_VERSION = new MethodMatcher(
            BLOCKING_STORAGE + " writeVersion(java.lang.String)", true);
    private static final MethodMatcher REACTOR_SAVE = new MethodMatcher(
            REACTOR_STORAGE + " save(java.lang.String, " + CHECKPOINT + ", " + CHECKPOINT_WRITE_CONDITION + ")", true);
    private static final MethodMatcher REACTOR_WRITE_VERSION = new MethodMatcher(
            REACTOR_STORAGE + " writeVersion(java.lang.String)", true);

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
               "again. The stub delegates `any()` to the class's own two-argument `save`, refuses a stronger " +
               "condition, and answers `writeVersion` empty, the permanent shape for a store that cannot " +
               "evaluate a condition. Evaluating a condition for real is still a manual pass, see " +
               "doc/migration/upgrading-to-0.33.0.md. Java only, rewrite-kotlin has no recipe for inserting a " +
               "member into a class body, so a Kotlin implementer needs the manual steps instead.";
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
                    cd = stub(cd, BLOCKING_SAVE, BLOCKING_SAVE_STUB, CHECKPOINT, CHECKPOINT_WRITE_CONDITION);
                    cd = stub(cd, BLOCKING_WRITE_VERSION, BLOCKING_WRITE_VERSION_STUB, OPTIONAL_LONG);
                } else if (TypeUtils.isAssignableTo(REACTOR_STORAGE, cd.getType())) {
                    cd = stub(cd, REACTOR_SAVE, REACTOR_SAVE_STUB, CHECKPOINT, CHECKPOINT_WRITE_CONDITION, MONO);
                    cd = stub(cd, REACTOR_WRITE_VERSION, REACTOR_WRITE_VERSION_STUB, MONO);
                }
                return cd;
            }

            // Inserts template as the class's last member unless a method matching required is already declared,
            // which is what makes a second run over an already-stubbed (or hand-migrated) class a no-op.
            private J.ClassDeclaration stub(J.ClassDeclaration cd, MethodMatcher required, String template, String... imports) {
                boolean alreadyDeclared = cd.getBody().getStatements().stream()
                        .filter(J.MethodDeclaration.class::isInstance)
                        .map(J.MethodDeclaration.class::cast)
                        .anyMatch(method -> required.matches(method, cd));
                if (alreadyDeclared) {
                    return cd;
                }

                for (String fqn : imports) {
                    maybeAddImport(fqn);
                }
                // contextSensitive binds the inserted method's owner to the class it lands in, which is what lets
                // a later MethodMatcher.matches(method, classDeclaration) recognise it as already declared instead
                // of inserting a second copy on every following cycle.
                J.Block newBody = JavaTemplate.builder(template)
                        .contextSensitive()
                        .imports(imports)
                        .javaParser(JavaParser.fromJavaVersion()
                                .dependsOn(CHECKPOINT_TYPE_STUB, CHECKPOINT_WRITE_CONDITION_TYPE_STUB, MONO_TYPE_STUB))
                        .build()
                        .apply(new Cursor(getCursor(), cd.getBody()), cd.getBody().getCoordinates().lastStatement());
                return cd.withBody(newBody);
            }
        };
    }
}
