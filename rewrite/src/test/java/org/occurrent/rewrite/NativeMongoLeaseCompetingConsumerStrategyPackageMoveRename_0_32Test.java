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
 * Covers the other half of {@code MigrateOccurrentRenames_0_32}: {@code NativeMongoLeaseCompetingConsumerStrategy}
 * moves out of {@code org.occurrent.subscription.mongodb.spring.blocking}, where it was published by mistake, and
 * into {@code org.occurrent.subscription.mongodb.nativedriver.blocking}, the package every other native-driver
 * subscription type uses. See issue #534.
 */
class NativeMongoLeaseCompetingConsumerStrategyPackageMoveRename_0_32Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-0_32.yml", "org.occurrent.MigrateOccurrentRenames_0_32");
    }

    @Test
    void theStrategyMovesFromTheSpringPackageToTheNativeDriverPackage() {
        rewriteRun(
                // The declaration is left as it is (the recipe uses ignoreDefinition: true); only references move.
                java(
                        """
                        package org.occurrent.subscription.mongodb.spring.blocking;

                        public class NativeMongoLeaseCompetingConsumerStrategy {
                            public static NativeMongoLeaseCompetingConsumerStrategy withDefaults(Object db) {
                                return new NativeMongoLeaseCompetingConsumerStrategy();
                            }

                            public static class Builder {
                                public Builder(Object db, String collection) {
                                }

                                public Builder leaseTime(java.time.Duration duration) {
                                    return this;
                                }

                                public NativeMongoLeaseCompetingConsumerStrategy build() {
                                    return new NativeMongoLeaseCompetingConsumerStrategy();
                                }
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.spring.blocking.NativeMongoLeaseCompetingConsumerStrategy;

                        class Foo {
                            NativeMongoLeaseCompetingConsumerStrategy strategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(null);

                            NativeMongoLeaseCompetingConsumerStrategy build() {
                                return new NativeMongoLeaseCompetingConsumerStrategy.Builder(null, "locks")
                                        .leaseTime(java.time.Duration.ofSeconds(1))
                                        .build();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy;

                        class Foo {
                            NativeMongoLeaseCompetingConsumerStrategy strategy = NativeMongoLeaseCompetingConsumerStrategy.withDefaults(null);

                            NativeMongoLeaseCompetingConsumerStrategy build() {
                                return new NativeMongoLeaseCompetingConsumerStrategy.Builder(null, "locks")
                                        .leaseTime(java.time.Duration.ofSeconds(1))
                                        .build();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void theStrategyMovesFromTheSpringPackageToTheNativeDriverPackageInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription.mongodb.spring.blocking

                        class NativeMongoLeaseCompetingConsumerStrategy {
                            companion object {
                                @JvmStatic
                                fun withDefaults(db: Any): NativeMongoLeaseCompetingConsumerStrategy {
                                    return NativeMongoLeaseCompetingConsumerStrategy()
                                }
                            }
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.mongodb.spring.blocking.NativeMongoLeaseCompetingConsumerStrategy

                        class Foo(val strategy: NativeMongoLeaseCompetingConsumerStrategy)
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoLeaseCompetingConsumerStrategy

                        class Foo(val strategy: NativeMongoLeaseCompetingConsumerStrategy)
                        """
                )
        );
    }
}
