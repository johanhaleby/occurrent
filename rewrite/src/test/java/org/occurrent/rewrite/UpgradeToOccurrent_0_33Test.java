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
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.CheckpointStorageStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the umbrella {@code UpgradeToOccurrent_0_33} recipe resolves its one sub-recipe through a
 * classpath-scanning Environment, which is what proves the cross-file recipe reference actually links. The stub
 * insertion itself is covered in {@link CheckpointStorageConditionalWriteStubsTest}, so one case is enough here.
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
}
