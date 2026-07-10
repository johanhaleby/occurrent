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

import static org.openrewrite.java.Assertions.java;

class CheckpointStorageConfigRenameTest extends MechanicalRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        // The stubbed config class self-references its own renamed type; see the base class constant.
        super.defaults(spec);
        spec.typeValidationOptions(RELAXED_FOR_SELF_REFERENCING_RENAME);
    }

    @Test
    void subscriptionPositionStorageConfigAndItsStaticBuilderMethodsAreRenamed() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.blocking.durable.catchup;

                        public class SubscriptionPositionStorageConfig {
                            public static SubscriptionPositionStorageConfig useSubscriptionPositionStorage(Object config) {
                                return null;
                            }

                            public static SubscriptionPositionStorageConfig dontUseSubscriptionPositionStorage() {
                                return null;
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.blocking.durable.catchup;

                        public class SubscriptionPositionStorageConfig {
                            public static CheckpointStorageConfig useCheckpointStorage(Object config) {
                                return null;
                            }

                            public static CheckpointStorageConfig dontUseCheckpointStorage() {
                                return null;
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig;

                        class Foo {
                            void run() {
                                SubscriptionPositionStorageConfig.useSubscriptionPositionStorage(null);
                                SubscriptionPositionStorageConfig.dontUseSubscriptionPositionStorage();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig;

                        class Foo {
                            void run() {
                                CheckpointStorageConfig.useCheckpointStorage(null);
                                CheckpointStorageConfig.dontUseCheckpointStorage();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void nestedUseSubscriptionPositionInStorageAndItsMethodsAreRenamed() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription.blocking.durable.catchup;

                        public class SubscriptionPositionStorageConfig {
                            public interface UseSubscriptionPositionInStorage {
                                UseSubscriptionPositionInStorage andPersistSubscriptionPositionDuringCatchupPhaseWhen(Object predicate);
                                UseSubscriptionPositionInStorage andPersistSubscriptionPositionDuringCatchupPhaseForEveryNEvents(int n);
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription.blocking.durable.catchup;

                        public class SubscriptionPositionStorageConfig {
                            public interface UseSubscriptionPositionInStorage {
                                CheckpointStorageConfig.UseCheckpointInStorage andPersistCheckpointDuringCatchupPhaseWhen(Object predicate);
                                CheckpointStorageConfig.UseCheckpointInStorage andPersistCheckpointDuringCatchupPhaseForEveryNEvents(int n);
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.blocking.durable.catchup.SubscriptionPositionStorageConfig.UseSubscriptionPositionInStorage;

                        class Foo {
                            void run(UseSubscriptionPositionInStorage cfg) {
                                cfg.andPersistSubscriptionPositionDuringCatchupPhaseWhen(null);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig;

                        class Foo {
                            void run(CheckpointStorageConfig.UseCheckpointInStorage cfg) {
                                cfg.andPersistCheckpointDuringCatchupPhaseWhen(null);
                            }
                        }
                        """
                )
        );
    }
}
