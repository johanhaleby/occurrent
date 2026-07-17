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

package org.occurrent.dsl.snapshot.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.SnapshotSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The reactive load/resume/persist steps shared by the reactor snapshot executors, mirroring
 * {@code org.occurrent.dsl.snapshot.SnapshotSupport} but composing into a {@link Mono} chain instead of blocking.
 */
final class ReactiveSnapshotSupport {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSnapshotSupport.class);

    private ReactiveSnapshotSupport() {
    }

    /**
     * Resolves the {@link SnapshotSupport.Base} to resume from, reading the latest snapshot from {@code store} and
     * reusing the pure {@link SnapshotSupport#resolveBase} for the schema check.
     */
    static <S extends @Nullable Object> Mono<SnapshotSupport.Base<S>> resolveBase(ReactiveSnapshotStore<S> store, String key, int expectedSchemaVersion, Supplier<? extends S> initialState) {
        return store.findLatest(key)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .map(loaded -> SnapshotSupport.resolveBase(loaded, expectedSchemaVersion, initialState));
    }

    /**
     * Best-effort snapshot save for the reactor executors, which save after the command's events have already committed.
     * A snapshot is a discardable optimization, so a save failure is logged and swallowed rather than propagated: failing
     * here would surface as a command failure even though the write succeeded, and a lost snapshot only means the next
     * replay folds a longer tail.
     */
    static <S extends @Nullable Object, E> Mono<Void> maybeSaveBestEffort(ReactiveSnapshotStore<S> store, String key, int schemaVersion,
                                                                          SnapshotPolicy<S, E> policy, SnapshotDecision<S, E> decision) {
        if (!policy.shouldSnapshot(decision)) {
            return Mono.empty();
        }
        return store.save(key, new Snapshot<>(decision.newState(), decision.newVersion(), schemaVersion))
                .onErrorResume(e -> {
                    log.warn("Best-effort snapshot save failed for key '{}'. The write is committed, the snapshot will be rebuilt from events on the next replay.", key, e);
                    return Mono.empty();
                });
    }
}
