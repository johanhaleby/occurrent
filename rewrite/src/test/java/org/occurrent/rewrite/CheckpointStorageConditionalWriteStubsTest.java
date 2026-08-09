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

import static org.occurrent.rewrite.CheckpointStorageStubs.*;
import static org.openrewrite.java.Assertions.java;

class CheckpointStorageConditionalWriteStubsTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/checkpoint-storage-stubs-0_33.yml",
                "org.occurrent.MigrateCheckpointStorageConditionalWrite_0_33");
    }

    @Test
    void stubsBothMissingMembersOnABlockingImplementer() {
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
    void stubsBothMissingMembersOnAReactorImplementer() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always refuses a conditional write. Evaluate `condition` for real, or keep refusing every condition but `any()` if this storage cannot evaluate one. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this never reports a stored version. Signal the version a condition is judged against, or an empty Mono if this storage cannot evaluate one. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.error(new UnsupportedOperationException("This storage does not track a write version."));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesABlockingImplementerThatAlreadyHasBothMembersUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                // No change expected: both members are already declared by hand, so the recipe finds nothing missing.
                java(
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
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return checkpoint;
                            }

                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAReactorImplementerThatAlreadyHasBothMembersUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                // No change expected: both members are already declared by hand, so the recipe finds nothing missing.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return Mono.error(new UnsupportedOperationException());
                            }

                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.error(new UnsupportedOperationException());
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCallingCodeUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                // No change expected: this class merely calls the two-argument save, it does not implement the interface.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        class Foo {
                            void run(CheckpointStorage storage, String subscriptionId, Checkpoint checkpoint) {
                                storage.save(subscriptionId, checkpoint);
                            }
                        }
                        """
                )
        );
    }
}
