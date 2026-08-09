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

/**
 * Shared source type-stubs for the {@code CheckpointStorage} conditional-write stub tests. Real Occurrent modules
 * are not on the recipe test classpath, so every type the recipe or the templates it inserts refers to
 * ({@code Checkpoint}, {@code CheckpointWriteCondition}, both {@code CheckpointStorage} interfaces, and reactor's
 * {@code Mono}) is stubbed here, shaped after the real 0.33.0 interfaces.
 */
final class CheckpointStorageStubs {

    private CheckpointStorageStubs() {
    }

    static final String CHECKPOINT = """
            package org.occurrent.subscription;
            public interface Checkpoint {}
            """;

    static final String CHECKPOINT_WRITE_CONDITION = """
            package org.occurrent.subscription;
            public interface CheckpointWriteCondition {}
            """;

    static final String MONO = """
            package reactor.core.publisher;
            public abstract class Mono<T> {
                public static <T> Mono<T> error(Throwable error) {
                    return null;
                }
            }
            """;

    static final String BLOCKING_CHECKPOINT_STORAGE = """
            package org.occurrent.subscription.api.blocking;

            import org.occurrent.subscription.Checkpoint;
            import org.occurrent.subscription.CheckpointWriteCondition;

            import java.util.OptionalLong;

            public interface CheckpointStorage {
                Checkpoint read(String subscriptionId);

                default Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                    return save(subscriptionId, checkpoint, null);
                }

                Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

                OptionalLong writeVersion(String subscriptionId);

                void delete(String subscriptionId);

                boolean exists(String subscriptionId);
            }
            """;

    static final String REACTOR_CHECKPOINT_STORAGE = """
            package org.occurrent.subscription.api.reactor;

            import org.occurrent.subscription.Checkpoint;
            import org.occurrent.subscription.CheckpointWriteCondition;
            import reactor.core.publisher.Mono;

            public interface CheckpointStorage {
                Mono<Checkpoint> read(String subscriptionId);

                default Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                    return save(subscriptionId, checkpoint, null);
                }

                Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition);

                Mono<Long> writeVersion(String subscriptionId);

                Mono<Void> delete(String subscriptionId);
            }
            """;
}
