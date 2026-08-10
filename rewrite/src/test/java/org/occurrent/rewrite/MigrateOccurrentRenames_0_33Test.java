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
