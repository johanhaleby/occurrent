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

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Covers {@code MigrateOccurrentRenames_0_33}: {@code ReplayAwareSubscriptionModel} and
 * {@code IntrospectableSubscriptionModel} become {@code ReplayAwareSubscriptions} and
 * {@code IntrospectableSubscriptions} on both the blocking and reactor stacks, and
 * {@code DelegatingSubscriptionModel} becomes {@code SubscriptionModelWrapper}. None of the five ever extended
 * {@code SubscriptionModel}, so the recipe only rewrites references, never the interface declaration itself.
 */
class MigrateOccurrentRenames_0_33Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-0_33.yml", "org.occurrent.MigrateOccurrentRenames_0_33");
    }

    @Test
    void theBlockingReplayAwareSubscriptionModelIsRenamedToReplayAwareSubscriptions() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface ReplayAwareSubscriptionModel {
                            boolean isCatchingUp(String subscriptionId);
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptionModel;

                        class Foo {
                            ReplayAwareSubscriptionModel model;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;

                        class Foo {
                            ReplayAwareSubscriptions model;
                        }
                        """
                )
        );
    }

    @Test
    void theReactorReplayAwareSubscriptionModelIsRenamedToReplayAwareSubscriptions() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.reactor;

                        public interface ReplayAwareSubscriptionModel {
                            boolean isCatchingUp(String subscriptionId);
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptionModel;

                        class Foo {
                            ReplayAwareSubscriptionModel model;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;

                        class Foo {
                            ReplayAwareSubscriptions model;
                        }
                        """
                )
        );
    }

    @Test
    void theBlockingIntrospectableSubscriptionModelIsRenamedToIntrospectableSubscriptions() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Set;

                        public interface IntrospectableSubscriptionModel {
                            Set<String> subscriptionIds();
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;

                        class Foo {
                            IntrospectableSubscriptionModel model;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;

                        class Foo {
                            IntrospectableSubscriptions model;
                        }
                        """
                )
        );
    }

    @Test
    void theReactorIntrospectableSubscriptionModelIsRenamedToIntrospectableSubscriptions() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.reactor;

                        import java.util.Set;

                        public interface IntrospectableSubscriptionModel {
                            Set<String> subscriptionIds();
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel;

                        class Foo {
                            IntrospectableSubscriptionModel model;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;

                        class Foo {
                            IntrospectableSubscriptions model;
                        }
                        """
                )
        );
    }

    @Test
    void theDelegatingSubscriptionModelIsRenamedToSubscriptionModelWrapper() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface DelegatingSubscriptionModel {
                            Object getDelegatedSubscriptionModel();
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;

                        class Foo {
                            DelegatingSubscriptionModel model;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;

                        class Foo {
                            SubscriptionModelWrapper model;
                        }
                        """
                )
        );
    }

    @Test
    void theSubscriptionModelWrapperMethodsMoveWithTheType() {
        // Declared directly as SubscriptionModelWrapper, the published name, rather than routed through the
        // DelegatingSubscriptionModel ChangeType in the same run: OpenRewrite loses track of the interface's
        // kind when ChangeType's ignoreDefinition and a same-file ChangeMethodName both touch one declaration.
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelWrapper {
                            Object getDelegatedSubscriptionModel();

                            default Object getDelegatedSubscriptionModelRecursively() {
                                return getDelegatedSubscriptionModel();
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelWrapper {
                            Object getWrappedSubscriptionModel();

                            default Object getWrappedSubscriptionModelRecursively() {
                                return getWrappedSubscriptionModel();
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;

                        class Foo {
                            void bar(SubscriptionModelWrapper model) {
                                model.getDelegatedSubscriptionModel();
                                model.getDelegatedSubscriptionModelRecursively();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;

                        class Foo {
                            void bar(SubscriptionModelWrapper model) {
                                model.getWrappedSubscriptionModel();
                                model.getWrappedSubscriptionModelRecursively();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void aRealUpgradeRenamesTheTypeAndItsMethodsTogether() {
        // DelegatingSubscriptionModel is supplied as a compiled dependency, the way a user's classpath actually
        // looks after adding the 0.33.0 jar, rather than as a rewritten source. This is the real upgrade case,
        // one user source using the old type and both old method names, migrated in a single recipe run.
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface DelegatingSubscriptionModel {
                            Object getDelegatedSubscriptionModel();

                            default Object getDelegatedSubscriptionModelRecursively() {
                                return getDelegatedSubscriptionModel();
                            }
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;

                        class Foo {
                            void bar(DelegatingSubscriptionModel model) {
                                model.getDelegatedSubscriptionModel();
                                model.getDelegatedSubscriptionModelRecursively();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;

                        class Foo {
                            void bar(SubscriptionModelWrapper model) {
                                model.getWrappedSubscriptionModel();
                                model.getWrappedSubscriptionModelRecursively();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void theIntrospectableSubscriptionModelConformanceTckBaseClassIsRenamed() {
        // Published in occurrent-tck-subscription-blocking since 0.32.0, so an external implementer's own
        // conformance test extends it. Supplied as a compiled dependency for the same reason as the combined
        // type-and-method case above.
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.tck.subscription.blocking;

                        public abstract class IntrospectableSubscriptionModelConformance {
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.tck.subscription.blocking.IntrospectableSubscriptionModelConformance;

                        class FooConformanceTest extends IntrospectableSubscriptionModelConformance {
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.tck.subscription.blocking.IntrospectableSubscriptionsConformance;

                        class FooConformanceTest extends IntrospectableSubscriptionsConformance {
                        }
                        """
                )
        );
    }

    @Test
    void aRealUpgradeRenamesReplayAwareSubscriptionsOfToFindIn() {
        // ReplayAwareSubscriptionModel and its of(Object) are supplied as a compiled dependency, the way a user's
        // classpath looks before the upgrade, rather than as a rewritten source. ReplayAwareSubscriptions, the
        // rename target, is also supplied with its real narrowed findIn(SubscriptionModelCapability) signature,
        // so the migrated "after" example is type-attributed against the actual post-upgrade contract rather than
        // an unresolved type the parser cannot check.
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelCapability {
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;

                        public interface ReplayAwareSubscriptionModel {
                            boolean isCatchingUp(String subscriptionId);

                            static Optional<ReplayAwareSubscriptionModel> of(Object subscriptionModel) {
                                return subscriptionModel instanceof ReplayAwareSubscriptionModel r ? Optional.of(r) : Optional.empty();
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;

                        public interface ReplayAwareSubscriptions extends SubscriptionModelCapability {
                            boolean isCatchingUp(String subscriptionId);

                            static Optional<ReplayAwareSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
                                return subscriptionModel instanceof ReplayAwareSubscriptions r ? Optional.of(r) : Optional.empty();
                            }
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptionModel;
                        import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;

                        class Foo {
                            void bar(SubscriptionModelCapability model) {
                                ReplayAwareSubscriptionModel.of(model);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
                        import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;

                        class Foo {
                            void bar(SubscriptionModelCapability model) {
                                ReplayAwareSubscriptions.findIn(model);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void aRealUpgradeRenamesIntrospectableSubscriptionsOfToFindIn() {
        // Same shape as the ReplayAwareSubscriptions case above, for IntrospectableSubscriptionModel's of(Object):
        // IntrospectableSubscriptions, the rename target, is supplied too, with its real narrowed
        // findIn(SubscriptionModelCapability) signature, so the "after" example is checked against it.
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelCapability {
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;
                        import java.util.Set;

                        public interface IntrospectableSubscriptionModel {
                            Set<String> subscriptionIds();

                            static Optional<IntrospectableSubscriptionModel> of(Object subscriptionModel) {
                                return subscriptionModel instanceof IntrospectableSubscriptionModel i ? Optional.of(i) : Optional.empty();
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;
                        import java.util.Set;

                        public interface IntrospectableSubscriptions extends SubscriptionModelCapability {
                            Set<String> subscriptionIds();

                            static Optional<IntrospectableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
                                return subscriptionModel instanceof IntrospectableSubscriptions i ? Optional.of(i) : Optional.empty();
                            }
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;
                        import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;

                        class Foo {
                            void bar(SubscriptionModelCapability model) {
                                IntrospectableSubscriptionModel.of(model);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
                        import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;

                        class Foo {
                            void bar(SubscriptionModelCapability model) {
                                IntrospectableSubscriptions.findIn(model);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void anObjectTypedArgumentAtTheRenamedReplayAwareCallSiteGetsAReviewComment() {
        // model is typed Object, the real 0.32.0 shape of a caller that resolved its subscription model through a
        // broader static type than SubscriptionModelCapability. of(Object) accepted that under 0.32.0, so this
        // compiled. findIn(SubscriptionModelCapability) will not, so the rename alone would leave a bare compile
        // error with no pointer back to the migration guide. The recipe cannot narrow model's declared type without
        // knowing what it actually holds, so it flags the call instead.
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelCapability {
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;

                        public interface ReplayAwareSubscriptionModel {
                            boolean isCatchingUp(String subscriptionId);

                            static Optional<ReplayAwareSubscriptionModel> of(Object subscriptionModel) {
                                return subscriptionModel instanceof ReplayAwareSubscriptionModel r ? Optional.of(r) : Optional.empty();
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;

                        public interface ReplayAwareSubscriptions extends SubscriptionModelCapability {
                            boolean isCatchingUp(String subscriptionId);

                            static Optional<ReplayAwareSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
                                return subscriptionModel instanceof ReplayAwareSubscriptions r ? Optional.of(r) : Optional.empty();
                            }
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptionModel;

                        class Foo {
                            void bar(Object model) {
                                ReplayAwareSubscriptionModel.of(model);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;

                        class Foo {
                            void bar(Object model) {
                                /* TODO [Occurrent 0.33 upgrade]: this argument is typed Object, and findIn(SubscriptionModelCapability) will not accept that once this call is renamed from of(Object). Type it as SubscriptionModelCapability (or a narrower capability) so this compiles again. See doc/migration/upgrading-to-0.33.0.md. */
                                ReplayAwareSubscriptions.findIn(model);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void anObjectTypedArgumentAtTheRenamedIntrospectableCallSiteGetsAReviewComment() {
        // Same shape as the ReplayAwareSubscriptions case above, for IntrospectableSubscriptionModel's of(Object).
        rewriteRun(
                spec -> spec.parser(JavaParser.fromJavaVersion().dependsOn(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModelCapability {
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;
                        import java.util.Set;

                        public interface IntrospectableSubscriptionModel {
                            Set<String> subscriptionIds();

                            static Optional<IntrospectableSubscriptionModel> of(Object subscriptionModel) {
                                return subscriptionModel instanceof IntrospectableSubscriptionModel i ? Optional.of(i) : Optional.empty();
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.api.blocking;

                        import java.util.Optional;
                        import java.util.Set;

                        public interface IntrospectableSubscriptions extends SubscriptionModelCapability {
                            Set<String> subscriptionIds();

                            static Optional<IntrospectableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
                                return subscriptionModel instanceof IntrospectableSubscriptions i ? Optional.of(i) : Optional.empty();
                            }
                        }
                        """
                )),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel;

                        class Foo {
                            void bar(Object model) {
                                IntrospectableSubscriptionModel.of(model);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;

                        class Foo {
                            void bar(Object model) {
                                /* TODO [Occurrent 0.33 upgrade]: this argument is typed Object, and findIn(SubscriptionModelCapability) will not accept that once this call is renamed from of(Object). Type it as SubscriptionModelCapability (or a narrower capability) so this compiles again. See doc/migration/upgrading-to-0.33.0.md. */
                                IntrospectableSubscriptions.findIn(model);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void theRenamesApplyInKotlinToo() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription.api.blocking

                        interface IntrospectableSubscriptionModel {
                            fun subscriptionIds(): Set<String>
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel

                        class Foo(val model: IntrospectableSubscriptionModel)
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions

                        class Foo(val model: IntrospectableSubscriptions)
                        """
                )
        );
    }
}
