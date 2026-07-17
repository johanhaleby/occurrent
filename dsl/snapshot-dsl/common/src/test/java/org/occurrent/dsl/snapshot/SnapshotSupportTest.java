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

package org.occurrent.dsl.snapshot;

import org.junit.jupiter.api.Test;
import org.occurrent.dsl.snapshot.LedgerFixture.Deposited;
import org.occurrent.dsl.snapshot.LedgerFixture.LedgerEvent;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotSupportTest {

    @Test
    void resolve_base_uses_the_snapshot_when_the_schema_matches() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 1, () -> 0);

        assertThat(base.state()).isEqualTo(42);
        assertThat(base.version()).isEqualTo(7);
    }

    @Test
    void resolve_base_falls_back_to_initial_state_on_a_schema_mismatch() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(loaded, 2, () -> 0);

        assertThat(base.state()).isEqualTo(0);
        assertThat(base.version()).isEqualTo(0);
    }

    @Test
    void resolve_base_falls_back_to_initial_state_when_no_snapshot_exists() {
        SnapshotSupport.Base<Integer> base = SnapshotSupport.resolveBase(Optional.empty(), 1, () -> 5);

        assertThat(base.state()).isEqualTo(5);
        assertThat(base.version()).isEqualTo(0);
    }

    @Test
    void maybe_save_writes_a_tagged_snapshot_when_the_policy_fires() {
        SnapshotStore<Integer> store = SnapshotStore.inMemory();
        SnapshotDecision<Integer, LedgerEvent> decision = new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, 1);

        boolean saved = SnapshotSupport.maybeSave(store, "acc:1", 3, SnapshotPolicy.always(), decision);

        assertThat(saved).isTrue();
        assertThat(store.findLatest("acc:1")).contains(new Snapshot<>(42, 7, 3));
    }

    @Test
    void maybe_save_writes_nothing_when_the_policy_does_not_fire() {
        SnapshotStore<Integer> store = SnapshotStore.inMemory();
        SnapshotDecision<Integer, LedgerEvent> decision = new SnapshotDecision<>(42, List.of(new Deposited(1)), 7, 1);

        boolean saved = SnapshotSupport.maybeSave(store, "acc:1", 3, SnapshotPolicy.never(), decision);

        assertThat(saved).isFalse();
        assertThat(store.findLatest("acc:1")).isEmpty();
    }

    @Test
    void is_redelivery_is_true_only_when_the_snapshot_already_covers_the_delivered_version_at_a_matching_schema() {
        Optional<Snapshot<Integer>> loaded = Optional.of(new Snapshot<>(42, 7, 1));

        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 7)).isTrue();  // already folded up to 7
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 6)).isTrue();  // an older redelivery
        assertThat(SnapshotSupport.isRedelivery(loaded, 1, 8)).isFalse(); // a new event to fold
        assertThat(SnapshotSupport.isRedelivery(loaded, 2, 7)).isFalse(); // schema mismatch, rebuild instead of skip
        assertThat(SnapshotSupport.isRedelivery(Optional.empty(), 1, 7)).isFalse();
    }
}
