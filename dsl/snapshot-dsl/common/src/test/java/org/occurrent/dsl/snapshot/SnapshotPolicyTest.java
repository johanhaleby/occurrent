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
import org.occurrent.dsl.snapshot.LedgerFixture.BooksClosed;
import org.occurrent.dsl.snapshot.LedgerFixture.Deposited;
import org.occurrent.dsl.snapshot.LedgerFixture.LedgerEvent;
import org.occurrent.dsl.snapshot.LedgerFixture.Withdrawn;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotPolicyTest {

    private static SnapshotDecision<Integer, LedgerEvent> decision(int eventsSinceSnapshot, List<LedgerEvent> newEvents) {
        return new SnapshotDecision<>(0, newEvents, 10, eventsSinceSnapshot);
    }

    @Test
    void every_n_events_fires_at_the_threshold_but_not_before() {
        SnapshotPolicy<Integer, LedgerEvent> policy = SnapshotPolicy.everyNEvents(3);

        assertThat(policy.shouldSnapshot(decision(2, List.of()))).isFalse();
        assertThat(policy.shouldSnapshot(decision(3, List.of()))).isTrue();
        assertThat(policy.shouldSnapshot(decision(4, List.of()))).isTrue();
    }

    @Test
    void every_n_events_rejects_non_positive_n() {
        assertThatThrownBy(() -> SnapshotPolicy.everyNEvents(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SnapshotPolicy.everyNEvents(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void never_never_fires() {
        assertThat(SnapshotPolicy.<Integer, LedgerEvent>never().shouldSnapshot(decision(100, List.of(new Deposited(1))))).isFalse();
    }

    @Test
    void always_fires_only_when_events_were_produced() {
        SnapshotPolicy<Integer, LedgerEvent> policy = SnapshotPolicy.always();

        assertThat(policy.shouldSnapshot(decision(1, List.of(new Deposited(1))))).isTrue();
        assertThat(policy.shouldSnapshot(decision(1, List.of()))).isFalse();
    }

    @Test
    void on_event_fires_when_a_matching_event_was_produced() {
        SnapshotPolicy<Integer, LedgerEvent> policy = SnapshotPolicy.onEvent(BooksClosed.class);

        assertThat(policy.shouldSnapshot(decision(1, List.of(new Deposited(1), new BooksClosed(5))))).isTrue();
        assertThat(policy.shouldSnapshot(decision(1, List.of(new Deposited(1), new Withdrawn(2))))).isFalse();
    }

    @Test
    void when_state_fires_on_the_state_predicate() {
        SnapshotPolicy<Integer, LedgerEvent> policy = SnapshotPolicy.whenState(balance -> balance >= 100);

        assertThat(policy.shouldSnapshot(new SnapshotDecision<>(150, List.of(), 1, 1))).isTrue();
        assertThat(policy.shouldSnapshot(new SnapshotDecision<>(50, List.of(), 1, 1))).isFalse();
    }

    @Test
    void or_fires_when_either_policy_fires() {
        SnapshotPolicy<Integer, LedgerEvent> policy = SnapshotPolicy.<Integer, LedgerEvent>everyNEvents(100).or(SnapshotPolicy.onEvent(BooksClosed.class));

        assertThat(policy.shouldSnapshot(decision(1, List.of(new BooksClosed(5))))).isTrue();
        assertThat(policy.shouldSnapshot(decision(100, List.of(new Deposited(1))))).isTrue();
        assertThat(policy.shouldSnapshot(decision(1, List.of(new Deposited(1))))).isFalse();
    }
}
