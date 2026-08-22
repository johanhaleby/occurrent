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
 * Covers {@code MigrateSubscriptionHandleRename_0_35}: {@code org.occurrent.subscription.api.blocking.Subscription}
 * and its reactor twin become {@code SubscriptionHandle} (ADR 127 decision 2). Neither interface declares any other
 * member the recipe could disturb, so each case only has to show the import and the type usage move together.
 */
class MigrateSubscriptionHandleRename_0_35Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-subscription-handle-0_35.yml",
                "org.occurrent.MigrateSubscriptionHandleRename_0_35");
    }

    @Test
    void theBlockingSubscriptionIsRenamedToSubscriptionHandle() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.blocking;

                        public interface Subscription {
                            String id();
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.Subscription;

                        class Foo {
                            Subscription handle;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.SubscriptionHandle;

                        class Foo {
                            SubscriptionHandle handle;
                        }
                        """
                )
        );
    }

    @Test
    void theReactorSubscriptionIsRenamedToSubscriptionHandle() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.api.reactor;

                        public interface Subscription {
                            String id();
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.Subscription;

                        class Foo {
                            Subscription handle;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.SubscriptionHandle;

                        class Foo {
                            SubscriptionHandle handle;
                        }
                        """
                )
        );
    }

    @Test
    void theRenameAppliesInKotlinToo() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription.api.blocking

                        interface Subscription {
                            fun id(): String
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.api.blocking.Subscription

                        class Foo(val handle: Subscription)
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.api.blocking.SubscriptionHandle

                        class Foo(val handle: SubscriptionHandle)
                        """
                )
        );
    }
}
