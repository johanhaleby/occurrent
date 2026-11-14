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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySagaStateStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();

    private static SagaEnvelope<String> envelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, timers, Map.of(), null, NOW, NOW, null, null);
    }

    private static SagaEnvelope<String> completedEnvelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.COMPLETED, version, timers, Map.of(), null, NOW, NOW, NOW, null);
    }

    private static SagaEnvelope<String> quarantinedEnvelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom");
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, timers, Map.of(), null, NOW, NOW, null, null, true, failure);
    }

    private static SagaEnvelope<String> activeEnvelopeUpdatedAt(String sagaId, Instant updatedAt) {
        return new SagaEnvelope<>(sagaId, "a", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, updatedAt, updatedAt, null, null);
    }

    @Nested
    class CompareAndSave {

        @Test
        void succeeds_inserting_a_new_instance_at_expected_version_0() {
            boolean saved = store.compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            assertThat(saved).isTrue();
        }

        @Test
        void fails_a_second_insert_at_expected_version_0_once_an_instance_exists() {
            store.compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            boolean saved = store.compareAndSave("s1", envelope("s1", "b", 1, List.of()), 0);

            assertThat(saved).isFalse();
        }

        @Test
        void succeeds_updating_an_existing_instance_at_its_current_version() {
            store.compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            boolean saved = store.compareAndSave("s1", envelope("s1", "b", 2, List.of()), 1);

            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(store.find("s1")).contains(envelope("s1", "b", 2, List.of()))
            );
        }

        @Test
        void fails_when_expected_version_no_longer_matches_the_stored_version() {
            store.compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);
            store.compareAndSave("s1", envelope("s1", "b", 2, List.of()), 1);

            boolean saved = store.compareAndSave("s1", envelope("s1", "c", 2, List.of()), 1);

            assertAll(
                    () -> assertThat(saved).isFalse(),
                    () -> assertThat(store.find("s1")).contains(envelope("s1", "b", 2, List.of()))
            );
        }
    }

    @Nested
    class Find {

        @Test
        void returns_the_stored_envelope() {
            SagaEnvelope<String> saved = envelope("s1", "a", 1, List.of());
            store.compareAndSave("s1", saved, 0);

            assertThat(store.find("s1")).contains(saved);
        }

        @Test
        void returns_empty_for_an_unknown_id() {
            assertThat(store.find("unknown")).isEmpty();
        }
    }

    @Nested
    class FindWithDueTimers {

        @Test
        void returns_instances_in_ascending_order_of_their_effective_due_time() {
            long now = NOW.toEpochMilli();
            store.compareAndSave("marked-late", envelope("marked-late", "a", 1, List.of(new TimerEntry("t", now - 5_000, now - 100))), 0);
            store.compareAndSave("fired-latest", envelope("fired-latest", "a", 1, List.of(new TimerEntry("t", now - 500))), 0);
            store.compareAndSave("fired-earliest", envelope("fired-earliest", "a", 1, List.of(new TimerEntry("t", now - 9_000))), 0);
            store.compareAndSave("marked-mid", envelope("marked-mid", "a", 1, List.of(new TimerEntry("t", now - 8_000, now - 3_000))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId)
                    .containsExactly("fired-earliest", "marked-mid", "fired-latest", "marked-late");
        }

        @Test
        void breaks_ties_in_due_time_by_saga_id() {
            long now = NOW.toEpochMilli();
            for (String sagaId : List.of("c", "a", "d", "b")) {
                store.compareAndSave(sagaId, envelope(sagaId, "a", 1, List.of(new TimerEntry("t", now - 1_000))), 0);
            }

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("a", "b", "c", "d");
        }

        @Test
        void keeps_the_instances_that_have_been_due_longest_when_the_limit_truncates() {
            long now = NOW.toEpochMilli();
            for (int i = 0; i < 6; i++) {
                String sagaId = "s" + i;
                store.compareAndSave(sagaId, envelope(sagaId, "a", 1, List.of(new TimerEntry("t", now - 6_000 + i * 1_000L))), 0);
            }

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 3);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("s0", "s1", "s2");
        }

        @Test
        void an_instance_whose_timer_failed_ranks_behind_one_that_became_due_before_the_failure_was_marked() {
            long now = NOW.toEpochMilli();
            store.compareAndSave("failed", envelope("failed", "a", 1, List.of(new TimerEntry("t", now - 9_000, now - 1_000))), 0);
            store.compareAndSave("healthy", envelope("healthy", "a", 1, List.of(new TimerEntry("t", now - 4_000))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 1);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("healthy");
        }

        @Test
        void does_not_return_an_instance_whose_marker_is_still_in_the_future() {
            long now = NOW.toEpochMilli();
            store.compareAndSave("backed-off", envelope("backed-off", "a", 1, List.of(new TimerEntry("t", now - 9_000, now + 1_000))), 0);

            assertThat(store.findWithDueTimers(NOW, 10)).isEmpty();
        }

        @Test
        void returns_active_instances_whose_earliest_timer_is_due() {
            store.compareAndSave("due", envelope("due", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            store.compareAndSave("not-due", envelope("not-due", "a", 1, List.of(new TimerEntry("t", NOW.plusSeconds(60).toEpochMilli()))), 0);
            store.compareAndSave("no-timer", envelope("no-timer", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("due");
        }

        @Test
        void excludes_completed_instances_even_when_a_timer_is_due() {
            store.compareAndSave("completed", completedEnvelope("completed", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 10);

            assertThat(due).isEmpty();
        }

        @Test
        void excludes_quarantined_instances_even_when_a_timer_is_due() {
            store.compareAndSave("quarantined", quarantinedEnvelope("quarantined", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 10);

            assertThat(due).isEmpty();
        }

        @Test
        void respects_the_supplied_limit() {
            store.compareAndSave("a", envelope("a", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            store.compareAndSave("b", envelope("b", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            store.compareAndSave("c", envelope("c", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 2);

            assertThat(due).hasSize(2);
        }

        @Test
        void due_instances_carry_the_lifecycle_timestamps() {
            Instant createdAt = NOW.minusSeconds(120);
            Instant updatedAt = NOW.minusSeconds(30);
            SagaEnvelope<String> saved = new SagaEnvelope<>("due", "a", SagaStatus.ACTIVE, 1,
                    List.of(new TimerEntry("t", NOW.toEpochMilli())), Map.of(), null, createdAt, updatedAt, null, null);
            store.compareAndSave("due", saved, 0);

            SagaEnvelope<String> found = store.findWithDueTimers(NOW, 10).getFirst();

            assertAll(
                    () -> assertThat(found.createdAt()).isEqualTo(createdAt),
                    () -> assertThat(found.updatedAt()).isEqualTo(updatedAt),
                    () -> assertThat(found.completedAt()).isNull()
            );
        }
    }

    /**
     * Mirrors {@code SpringMongoSagaStateStoreMongoTest}'s {@code findByStatus_*} tests, so the contract is verified
     * identically against both store implementations. No test-jar dependency exists between saga-dsl/common and
     * saga-dsl/mongodb-spring, so the assertions are duplicated by hand rather than shared through a common base class;
     * see the planned Technology Compatibility Kit (issue #395).
     */
    @Nested
    class FindByStatus {

        @Test
        void returns_instances_with_the_given_status() {
            store.compareAndSave("active-1", envelope("active-1", "a", 1, List.of()), 0);
            store.compareAndSave("completed-1", completedEnvelope("completed-1", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> active = store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10);

            assertThat(active).extracting(SagaEnvelope::sagaId).containsExactly("active-1");
        }

        /**
         * Guards the {@code status} argument itself. Every other case here queries {@code ACTIVE}, so an implementation
         * that dropped the parameter and hardcoded {@code ACTIVE} the way {@code findWithDueTimers} does would pass all
         * of them.
         */
        @Test
        void selects_on_the_requested_status_rather_than_always_active() {
            store.compareAndSave("active-1", envelope("active-1", "a", 1, List.of()), 0);
            store.compareAndSave("completed-1", completedEnvelope("completed-1", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> completed = store.findByStatus(SagaStatus.COMPLETED, NOW.plusSeconds(1), 10);

            assertThat(completed).extracting(SagaEnvelope::sagaId).containsExactly("completed-1");
        }

        /**
         * Instances saved in one executor tick share an {@code updatedAt}, and a tie group larger than {@code limit} is
         * the whole reason the contract calls {@code limit} a bound rather than a page. Asserts only what both stores can
         * promise: some two of the three, without error. Pinning a particular pair would bake in one store's tiebreak and
         * make the two deterministically disagree.
         */
        @Test
        void truncates_a_tie_group_at_limit_without_failing() {
            Instant sameMillisecond = NOW.minusSeconds(30);
            store.compareAndSave("tie-a", activeEnvelopeUpdatedAt("tie-a", sameMillisecond), 0);
            store.compareAndSave("tie-b", activeEnvelopeUpdatedAt("tie-b", sameMillisecond), 0);
            store.compareAndSave("tie-c", activeEnvelopeUpdatedAt("tie-c", sameMillisecond), 0);

            List<SagaEnvelope<String>> found = store.findByStatus(SagaStatus.ACTIVE, NOW, 2);

            assertAll(
                    () -> assertThat(found).hasSize(2),
                    () -> assertThat(found).extracting(SagaEnvelope::sagaId).doesNotHaveDuplicates(),
                    () -> assertThat(found).extracting(SagaEnvelope::sagaId).isSubsetOf("tie-a", "tie-b", "tie-c")
            );
        }

        @Test
        void updatedBefore_is_exclusive() {
            store.compareAndSave("exact", activeEnvelopeUpdatedAt("exact", NOW), 0);

            assertAll(
                    () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW, 10)).isEmpty(),
                    () -> assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW.plusMillis(1), 10))
                            .extracting(SagaEnvelope::sagaId).containsExactly("exact")
            );
        }

        @Test
        void orders_ascending_by_updatedAt_so_the_stalest_instance_is_first() {
            store.compareAndSave("newest", activeEnvelopeUpdatedAt("newest", NOW), 0);
            store.compareAndSave("oldest", activeEnvelopeUpdatedAt("oldest", NOW.minusSeconds(10)), 0);
            store.compareAndSave("middle", activeEnvelopeUpdatedAt("middle", NOW.minusSeconds(5)), 0);

            List<SagaEnvelope<String>> found = store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10);

            assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle", "newest");
        }

        @Test
        void limit_truncates_after_ordering_to_the_stalest_N_not_an_arbitrary_N() {
            store.compareAndSave("oldest", activeEnvelopeUpdatedAt("oldest", NOW.minusSeconds(10)), 0);
            store.compareAndSave("middle", activeEnvelopeUpdatedAt("middle", NOW.minusSeconds(5)), 0);
            store.compareAndSave("newest", activeEnvelopeUpdatedAt("newest", NOW), 0);

            List<SagaEnvelope<String>> found = store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 2);

            assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle");
        }

        @Test
        void rejects_a_limit_below_1() {
            assertThatThrownBy(() -> store.findByStatus(SagaStatus.ACTIVE, NOW, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("limit must be positive");
        }

        @Test
        void excludes_an_envelope_with_a_null_updatedAt() {
            SagaEnvelope<String> noUpdatedAt = new SagaEnvelope<>("no-updated-at", "a", SagaStatus.ACTIVE, 1,
                    List.of(), Map.of(), null, NOW, null, null, null);
            store.compareAndSave("no-updated-at", noUpdatedAt, 0);

            assertThat(store.findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(60), 10)).isEmpty();
        }
    }

    @Nested
    class LifecycleTimestamps {

        @Test
        void createdAt_updatedAt_and_completedAt_round_trip_through_find() {
            Instant createdAt = NOW.minusSeconds(120);
            Instant updatedAt = NOW.minusSeconds(30);
            Instant completedAt = NOW;
            SagaEnvelope<String> saved = new SagaEnvelope<>("s1", "a", SagaStatus.COMPLETED, 1, List.of(), Map.of(), null,
                    createdAt, updatedAt, completedAt, null);
            store.compareAndSave("s1", saved, 0);

            SagaEnvelope<String> found = store.find("s1").orElseThrow();

            assertAll(
                    () -> assertThat(found.createdAt()).isEqualTo(createdAt),
                    () -> assertThat(found.updatedAt()).isEqualTo(updatedAt),
                    () -> assertThat(found.completedAt()).isEqualTo(completedAt)
            );
        }

        @Test
        void an_active_instance_has_no_completedAt() {
            store.compareAndSave("s2", envelope("s2", "a", 1, List.of()), 0);

            assertThat(store.find("s2")).hasValueSatisfying(e -> assertThat(e.completedAt()).isNull());
        }
    }

    @Nested
    class Delete {

        @Test
        void removes_the_instance() {
            store.compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            store.delete("s1");

            assertThat(store.find("s1")).isEmpty();
        }

        @Test
        void is_a_no_op_for_an_unknown_id() {
            store.delete("unknown");

            assertThat(store.find("unknown")).isEmpty();
        }
    }
}
