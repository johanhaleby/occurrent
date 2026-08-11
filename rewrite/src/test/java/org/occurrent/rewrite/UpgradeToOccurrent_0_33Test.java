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
import org.openrewrite.config.Environment;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.occurrent.rewrite.CheckpointStorageStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the umbrella {@code UpgradeToOccurrent_0_33} recipe resolves its sub-recipes through a classpath-scanning
 * Environment, which is what proves the cross-file recipe references actually link. What each sub-recipe does is
 * covered in {@link CheckpointStorageConditionalWriteStubsTest} and {@link MigrateSagaTimerNameTest}, so one case
 * apiece is enough here.
 */
class UpgradeToOccurrent_0_33Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                .scanRuntimeClasspath("org.occurrent")
                .build()
                .activateRecipes("org.occurrent.UpgradeToOccurrent_0_33"));
    }

    @Test
    void stubsTheMissingConditionalWriteMembersOnABlockingImplementer() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always refuses a conditional write. Evaluate `condition` for real, or keep refusing every condition but `any()` if this storage cannot evaluate one. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this never reports a stored version. Return the version a condition is judged against, or OptionalLong.empty() if this storage cannot evaluate one. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                throw new UnsupportedOperationException("This storage does not track a write version.");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void readsTheStringHandedToTheSagaTimeoutConstructorThroughParse() {
        rewriteRun(
                // A 0.32.0 constructor call does not resolve against the 0.33.0 constructor, which is the whole
                // reason it needs migrating, so the parsed call carries no constructor type.
                spec -> spec.parser(JavaParser.fromJavaVersion()
                                .dependsOn(SagaTimerNameStubs.TIMER_NAME, SagaTimerNameStubs.SAGA_TIMEOUT))
                        .typeValidationOptions(TypeValidation.builder().constructorInvocations(false).build()),
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class FireTimer {
                            SagaTimeout paymentTimedOut() {
                                return new SagaTimeout("order-1", "payment");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;
                        import org.occurrent.dsl.saga.TimerName;

                        class FireTimer {
                            SagaTimeout paymentTimedOut() {
                                return new SagaTimeout("order-1", TimerName.parse("payment"));
                            }
                        }
                        """
                )
        );
    }
}
