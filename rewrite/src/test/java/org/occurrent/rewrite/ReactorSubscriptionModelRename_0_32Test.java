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
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Covers {@code MigrateOccurrentRenames_0_32}, which renames the reactor subscription primitive
 * {@code SubscriptionModel} to {@code FluxSubscriptionModel}. The blocking type keeps its name, and a case here asserts
 * that it is left alone. The two share a simple name, so a recipe matching on the simple name rather than the fully
 * qualified one would rewrite blocking code that has nothing to migrate.
 */
class ReactorSubscriptionModelRename_0_32Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-0_32.yml", "org.occurrent.MigrateOccurrentRenames_0_32");
    }

    @Test
    void theReactorSubscriptionModelIsRenamedToFluxSubscriptionModel() {
        rewriteRun(
                // The declaration is left as it is (the recipe uses ignoreDefinition: true); only references move.
                java(
                        """
                        package org.occurrent.subscription.api.reactor;

                        public interface SubscriptionModel {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.SubscriptionModel;

                        class Foo {
                            SubscriptionModel subscriptionModel;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;

                        class Foo {
                            FluxSubscriptionModel subscriptionModel;
                        }
                        """
                )
        );
    }

    @Test
    void theBlockingSubscriptionModelIsLeftAlone() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface SubscriptionModel {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionModel;

                        class Bar {
                            SubscriptionModel subscriptionModel;
                        }
                        """
                )
        );
    }

    @Test
    void theReactorSubscriptionModelIsRenamedInKotlinToo() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription.api.reactor

                        interface SubscriptionModel
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.api.reactor.SubscriptionModel

                        class Foo(val subscriptionModel: SubscriptionModel)
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.api.reactor.FluxSubscriptionModel

                        class Foo(val subscriptionModel: FluxSubscriptionModel)
                        """
                )
        );
    }
}
