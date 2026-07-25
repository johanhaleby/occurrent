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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotStoreSupportTest {

    private sealed interface LedgerEvent permits Deposited {
    }

    private record Deposited(int amount) implements LedgerEvent {
    }

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
}
