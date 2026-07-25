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

package org.occurrent.dsl.snapshot.blocking.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The reusable, blocking {@link SnapshotStore} save steps shared by the blocking snapshot executors, mirroring the
 * storage-neutral steps in {@code org.occurrent.dsl.snapshot.internal.SnapshotSupport} but living here because they
 * depend on the blocking {@link SnapshotStore} (a reactive store cannot use them).
 */
public final class SnapshotStoreSupport {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStoreSupport.class);

    private SnapshotStoreSupport() {
    }

    /**
     * Writes a snapshot when {@code policy} fires for {@code decision}, tagging it with {@code schemaVersion}. The caller
     * decides whether the save is best-effort (wrap the call) or transactional (run it inside the write transaction).
     *
     * @return {@code true} if a snapshot was written
     */
    public static <S extends @Nullable Object, E> boolean maybeSave(SnapshotStore<S> store, String key, int schemaVersion,
                                                                    SnapshotPolicy<S, E> policy, SnapshotDecision<S, E> decision) {
        requireNonNull(store, "store cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNull(policy, "policy cannot be null");
        requireNonNull(decision, "decision cannot be null");
        if (!policy.shouldSnapshot(decision)) {
            return false;
        }
        store.save(key, new Snapshot<>(decision.newState(), decision.newVersion(), schemaVersion));
        return true;
    }

    /**
     * Best-effort variant of {@link #maybeSave} for the DSL executors, which save the snapshot after the command's
     * events have already committed. A snapshot is a discardable optimization, so a save failure is logged and swallowed
     * rather than propagated: failing here would surface as a command failure even though the write succeeded, and a lost
     * snapshot only means the next replay folds a longer tail. The maintained {@code @Snapshot} path keeps using the
     * throwing {@link #maybeSave} so a durable subscription can retry.
     *
     * @return {@code true} if a snapshot was written, {@code false} if the policy declined it or the save failed
     */
    public static <S extends @Nullable Object, E> boolean maybeSaveBestEffort(SnapshotStore<S> store, String key, int schemaVersion,
                                                                              SnapshotPolicy<S, E> policy, SnapshotDecision<S, E> decision) {
        return maybeSaveBestEffort(store, key, schemaVersion, policy, () -> decision);
    }

    /**
     * Best-effort variant of {@link #maybeSave} that builds the {@link SnapshotDecision} inside the try, so the
     * arithmetic that assembles it (for example {@code SnapshotSupport.requireInt} narrowing {@code eventsSinceSnapshot})
     * is also swallowed on failure rather than thrown. The command's events have already committed by the time the
     * executor calls this, so an exception escaping here would surface a committed command as a failure; building the
     * decision lazily keeps every step after the commit best-effort.
     *
     * @param decisionSupplier supplies the decision to save; evaluated at most once, inside the best-effort boundary
     * @return {@code true} if a snapshot was written, {@code false} if the policy declined it or anything after the
     * commit (the decision build or the save) failed
     */
    public static <S extends @Nullable Object, E> boolean maybeSaveBestEffort(SnapshotStore<S> store, String key, int schemaVersion,
                                                                              SnapshotPolicy<S, E> policy, Supplier<? extends SnapshotDecision<S, E>> decisionSupplier) {
        requireNonNull(decisionSupplier, "decisionSupplier cannot be null");
        try {
            return maybeSave(store, key, schemaVersion, policy, decisionSupplier.get());
        } catch (RuntimeException e) {
            log.warn("Best-effort snapshot save failed for key '{}'. The write is committed, the snapshot will be rebuilt from events on the next replay.", key, e);
            return false;
        }
    }
}
