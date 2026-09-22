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

import org.junit.jupiter.api.Test;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class SnapshotStoreSupportTest {

    private sealed interface LedgerEvent permits Deposited {
    }

    private record Deposited(int amount) implements LedgerEvent {
    }

    private static final SnapshotDecision<Integer, LedgerEvent> DECISION = new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, 1);

    @Test
    void maybe_save_writes_a_tagged_snapshot_when_the_policy_fires() {
        SnapshotStore<Integer> store = SnapshotStore.inMemory();
        SnapshotDecision<Integer, LedgerEvent> decision = new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, 1);

        boolean saved = SnapshotStoreSupport.maybeSave(store, "acc:1", 3, SnapshotPolicy.always(), decision);

        assertThat(saved).isTrue();
        assertThat(store.findLatest("acc:1")).contains(new Snapshot<>(42, 7, 3));
    }

    @Test
    void maybe_save_writes_nothing_when_the_policy_does_not_fire() {
        SnapshotStore<Integer> store = SnapshotStore.inMemory();
        SnapshotDecision<Integer, LedgerEvent> decision = new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, 1);

        boolean saved = SnapshotStoreSupport.maybeSave(store, "acc:1", 3, SnapshotPolicy.never(), decision);

        assertThat(saved).isFalse();
        assertThat(store.findLatest("acc:1")).isEmpty();
    }

    @Test
    void best_effort_save_swallows_a_negative_events_since_snapshot_rather_than_throwing() {
        SnapshotStore<Integer> store = SnapshotStore.inMemory();

        // The supplier assembles the decision with a negative eventsSinceSnapshot, which requireInt rejects. Building it
        // inside the best-effort boundary means the throw is swallowed (the command already committed), not propagated.
        boolean saved = SnapshotStoreSupport.<Integer, LedgerEvent>maybeSaveBestEffort(store, "acc:1", 3, SnapshotPolicy.always(),
                () -> new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, SnapshotSupport.requireInt(-1, "the number of events since the snapshot")));

        assertThat(saved).isFalse();
        assertThat(store.findLatest("acc:1")).isEmpty();
    }

    @Test
    void best_effort_save_returns_false_when_the_store_throws_a_runtime_exception() {
        assertBestEffortSaveReturnsFalse(storeWhoseSaveThrows(new IllegalStateException("snapshot store save failed (test double)")), SnapshotPolicy.always(), () -> DECISION);
    }

    @Test
    void best_effort_save_returns_false_when_the_store_throws_a_checked_exception() {
        assertBestEffortSaveReturnsFalse(storeWhoseSaveThrows(new IOException("snapshot store unreachable (test double)")), SnapshotPolicy.always(), () -> DECISION);
    }

    @Test
    void best_effort_save_returns_false_when_the_store_throws_an_Error() {
        assertBestEffortSaveReturnsFalse(storeWhoseSaveThrows(new StackOverflowError("snapshot store save overflowed (test double)")), SnapshotPolicy.always(), () -> DECISION);
    }

    @Test
    void best_effort_save_returns_false_when_the_policy_throws_a_checked_exception() {
        SnapshotPolicy<Integer, LedgerEvent> throwingPolicy = decision -> {
            throw SnapshotStoreSupportTest.<RuntimeException>sneakyThrow(new IOException("policy failed (test double)"));
        };
        assertBestEffortSaveReturnsFalse(SnapshotStore.inMemory(), throwingPolicy, () -> DECISION);
    }

    @Test
    void best_effort_save_returns_false_when_the_decision_supplier_throws_a_checked_exception() {
        assertBestEffortSaveReturnsFalse(SnapshotStore.inMemory(), SnapshotPolicy.always(), () -> {
            throw SnapshotStoreSupportTest.<RuntimeException>sneakyThrow(new IOException("decision build failed (test double)"));
        });
    }

    @Test
    void best_effort_save_keeps_the_interrupt_when_the_store_throws_an_InterruptedException() {
        try {
            assertBestEffortSaveReturnsFalse(storeWhoseSaveThrows(new InterruptedException("interrupted while saving (test double)")), SnapshotPolicy.always(), () -> DECISION);

            assertThat(Thread.currentThread().isInterrupted()).as("the caller's interrupt flag").isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private static void assertBestEffortSaveReturnsFalse(SnapshotStore<Integer> store, SnapshotPolicy<Integer, LedgerEvent> policy,
                                                         Supplier<SnapshotDecision<Integer, LedgerEvent>> decisionSupplier) {
        AtomicReference<Boolean> saved = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> saved.set(SnapshotStoreSupport.maybeSaveBestEffort(store, "acc:1", 3, policy, decisionSupplier)));

        assertThat(thrown).as("what escaped the best-effort save").isNull();
        assertThat(saved.get()).as("the best-effort save's result").isFalse();
    }

    private static SnapshotStore<Integer> storeWhoseSaveThrows(Throwable failure) {
        return new SnapshotStore<>() {
            @Override
            public Optional<Snapshot<Integer>> findLatest(String key) {
                return Optional.empty();
            }

            @Override
            public void save(String key, Snapshot<Integer> snapshot) {
                throw SnapshotStoreSupportTest.<RuntimeException>sneakyThrow(failure);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }
}
