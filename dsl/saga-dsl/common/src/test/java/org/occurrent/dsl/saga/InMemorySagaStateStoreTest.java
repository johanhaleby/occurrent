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
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySagaStateStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();
    private final SagaCodec<String> codec = SagaCodec.of(store.stateCodec(), null);

    private boolean compareAndSave(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
        return store.compareAndSave(codec.encode(envelope), expectedVersion);
    }

    private Optional<SagaEnvelope<String>> find(String sagaId) {
        return store.find(sagaId).map(codec::decode);
    }

    private List<SagaEnvelope<String>> findWithDueTimers(Instant now, int limit) {
        return store.findWithDueTimers(now, limit).stream().map(codec::decode).toList();
    }

    private List<SagaEnvelope<String>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        return store.findByStatus(status, updatedBefore, limit).stream().map(codec::decode).toList();
    }

    private static SagaEnvelope<String> envelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, timers, Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> completedEnvelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.COMPLETED, version, timers, Map.of(), null, NOW, NOW, NOW, null, true, null, Map.of(), null);
    }

    private static SagaEnvelope<String> quarantinedEnvelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", null, null);
        return new SagaEnvelope<>(sagaId, state, SagaStatus.QUARANTINED, version, timers, Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), null);
    }

    private static SagaEnvelope<String> releasingEnvelope(String sagaId) {
        SagaFailure failure = new SagaFailure("s@7", 7L, NOW, IllegalStateException.class.getName(), "boom", NOW.plusSeconds(1), "q1");
        return new SagaEnvelope<>(sagaId, "a", SagaStatus.QUARANTINED, 1, List.of(), Map.of(), null, NOW, NOW, null, null, true, failure, Map.of(), 0L);
    }

    private static SagaEnvelope<String> activeEnvelopeUpdatedAt(String sagaId, Instant updatedAt) {
        return new SagaEnvelope<>(sagaId, "a", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, updatedAt, updatedAt, null, null, true, null, Map.of(), null);
    }

    @Nested
    class CompareAndSave {

        @Test
        void succeeds_inserting_a_new_instance_at_expected_version_0() {
            boolean saved = compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            assertThat(saved).isTrue();
        }

        @Test
        void fails_a_second_insert_at_expected_version_0_once_an_instance_exists() {
            compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            boolean saved = compareAndSave("s1", envelope("s1", "b", 1, List.of()), 0);

            assertThat(saved).isFalse();
        }

        @Test
        void succeeds_updating_an_existing_instance_at_its_current_version() {
            compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            boolean saved = compareAndSave("s1", envelope("s1", "b", 2, List.of()), 1);

            assertAll(
                    () -> assertThat(saved).isTrue(),
                    () -> assertThat(find("s1")).contains(envelope("s1", "b", 2, List.of()))
            );
        }

        @Test
        void fails_when_expected_version_no_longer_matches_the_stored_version() {
            compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);
            compareAndSave("s1", envelope("s1", "b", 2, List.of()), 1);

            boolean saved = compareAndSave("s1", envelope("s1", "c", 2, List.of()), 1);

            assertAll(
                    () -> assertThat(saved).isFalse(),
                    () -> assertThat(find("s1")).contains(envelope("s1", "b", 2, List.of()))
            );
        }
    }

    @Nested
    class Find {

        @Test
        void returns_the_stored_envelope() {
            SagaEnvelope<String> saved = envelope("s1", "a", 1, List.of());
            compareAndSave("s1", saved, 0);

            assertThat(find("s1")).contains(saved);
        }

        @Test
        void returns_empty_for_an_unknown_id() {
            assertThat(find("unknown")).isEmpty();
        }
    }

    @Nested
    class FindWithDueTimers {

        @Test
        void returns_instances_in_ascending_order_of_their_effective_due_time() {
            long now = NOW.toEpochMilli();
            compareAndSave("marked-late", envelope("marked-late", "a", 1, List.of(new TimerEntry("t", now - 5_000, now - 100))), 0);
            compareAndSave("fired-latest", envelope("fired-latest", "a", 1, List.of(new TimerEntry("t", now - 500))), 0);
            compareAndSave("fired-earliest", envelope("fired-earliest", "a", 1, List.of(new TimerEntry("t", now - 9_000))), 0);
            compareAndSave("marked-mid", envelope("marked-mid", "a", 1, List.of(new TimerEntry("t", now - 8_000, now - 3_000))), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId)
                    .containsExactly("fired-earliest", "marked-mid", "fired-latest", "marked-late");
        }

        @Test
        void breaks_ties_in_due_time_by_saga_id() {
            long now = NOW.toEpochMilli();
            for (String sagaId : List.of("c", "a", "d", "b")) {
                compareAndSave(sagaId, envelope(sagaId, "a", 1, List.of(new TimerEntry("t", now - 1_000))), 0);
            }

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("a", "b", "c", "d");
        }

        @Test
        void keeps_the_instances_that_have_been_due_longest_when_the_limit_truncates() {
            long now = NOW.toEpochMilli();
            for (int i = 0; i < 6; i++) {
                String sagaId = "s" + i;
                compareAndSave(sagaId, envelope(sagaId, "a", 1, List.of(new TimerEntry("t", now - 6_000 + i * 1_000L))), 0);
            }

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 3);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("s0", "s1", "s2");
        }

        @Test
        void an_instance_whose_timer_failed_ranks_behind_one_that_became_due_before_the_failure_was_marked() {
            long now = NOW.toEpochMilli();
            compareAndSave("failed", envelope("failed", "a", 1, List.of(new TimerEntry("t", now - 9_000, now - 1_000))), 0);
            compareAndSave("healthy", envelope("healthy", "a", 1, List.of(new TimerEntry("t", now - 4_000))), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 1);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("healthy");
        }

        @Test
        void does_not_return_an_instance_whose_marker_is_still_in_the_future() {
            long now = NOW.toEpochMilli();
            compareAndSave("backed-off", envelope("backed-off", "a", 1, List.of(new TimerEntry("t", now - 9_000, now + 1_000))), 0);

            assertThat(findWithDueTimers(NOW, 10)).isEmpty();
        }

        @Test
        void returns_active_instances_whose_earliest_timer_is_due() {
            compareAndSave("due", envelope("due", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            compareAndSave("not-due", envelope("not-due", "a", 1, List.of(new TimerEntry("t", NOW.plusSeconds(60).toEpochMilli()))), 0);
            compareAndSave("no-timer", envelope("no-timer", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 10);

            assertThat(due).extracting(SagaEnvelope::sagaId).containsExactly("due");
        }

        @Test
        void excludes_completed_instances_even_when_a_timer_is_due() {
            compareAndSave("completed", completedEnvelope("completed", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 10);

            assertThat(due).isEmpty();
        }

        @Test
        void excludes_quarantined_instances_even_when_a_timer_is_due() {
            compareAndSave("quarantined", quarantinedEnvelope("quarantined", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 10);

            assertThat(due).isEmpty();
        }

        @Test
        void respects_the_supplied_limit() {
            compareAndSave("a", envelope("a", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            compareAndSave("b", envelope("b", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            compareAndSave("c", envelope("c", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = findWithDueTimers(NOW, 2);

            assertThat(due).hasSize(2);
        }

        @Test
        void due_instances_carry_the_lifecycle_timestamps() {
            Instant createdAt = NOW.minusSeconds(120);
            Instant updatedAt = NOW.minusSeconds(30);
            SagaEnvelope<String> saved = new SagaEnvelope<>("due", "a", SagaStatus.ACTIVE, 1,
                    List.of(new TimerEntry("t", NOW.toEpochMilli())), Map.of(), null, createdAt, updatedAt, null, null, true, null, Map.of(), null);
            compareAndSave("due", saved, 0);

            SagaEnvelope<String> found = findWithDueTimers(NOW, 10).getFirst();

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
            compareAndSave("active-1", envelope("active-1", "a", 1, List.of()), 0);
            compareAndSave("completed-1", completedEnvelope("completed-1", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> active = findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10);

            assertThat(active).extracting(SagaEnvelope::sagaId).containsExactly("active-1");
        }

        /**
         * Guards the {@code status} argument itself. Every other case here queries {@code ACTIVE}, so an implementation
         * that dropped the parameter and hardcoded {@code ACTIVE} the way {@code findWithDueTimers} does would pass all
         * of them.
         */
        @Test
        void selects_on_the_requested_status_rather_than_always_active() {
            compareAndSave("active-1", envelope("active-1", "a", 1, List.of()), 0);
            compareAndSave("completed-1", completedEnvelope("completed-1", "a", 1, List.of()), 0);

            List<SagaEnvelope<String>> completed = findByStatus(SagaStatus.COMPLETED, NOW.plusSeconds(1), 10);

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
            compareAndSave("tie-a", activeEnvelopeUpdatedAt("tie-a", sameMillisecond), 0);
            compareAndSave("tie-b", activeEnvelopeUpdatedAt("tie-b", sameMillisecond), 0);
            compareAndSave("tie-c", activeEnvelopeUpdatedAt("tie-c", sameMillisecond), 0);

            List<SagaEnvelope<String>> found = findByStatus(SagaStatus.ACTIVE, NOW, 2);

            assertAll(
                    () -> assertThat(found).hasSize(2),
                    () -> assertThat(found).extracting(SagaEnvelope::sagaId).doesNotHaveDuplicates(),
                    () -> assertThat(found).extracting(SagaEnvelope::sagaId).isSubsetOf("tie-a", "tie-b", "tie-c")
            );
        }

        @Test
        void updatedBefore_is_exclusive() {
            compareAndSave("exact", activeEnvelopeUpdatedAt("exact", NOW), 0);

            assertAll(
                    () -> assertThat(findByStatus(SagaStatus.ACTIVE, NOW, 10)).isEmpty(),
                    () -> assertThat(findByStatus(SagaStatus.ACTIVE, NOW.plusMillis(1), 10))
                            .extracting(SagaEnvelope::sagaId).containsExactly("exact")
            );
        }

        @Test
        void orders_ascending_by_updatedAt_so_the_stalest_instance_is_first() {
            compareAndSave("newest", activeEnvelopeUpdatedAt("newest", NOW), 0);
            compareAndSave("oldest", activeEnvelopeUpdatedAt("oldest", NOW.minusSeconds(10)), 0);
            compareAndSave("middle", activeEnvelopeUpdatedAt("middle", NOW.minusSeconds(5)), 0);

            List<SagaEnvelope<String>> found = findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 10);

            assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle", "newest");
        }

        @Test
        void limit_truncates_after_ordering_to_the_stalest_N_not_an_arbitrary_N() {
            compareAndSave("oldest", activeEnvelopeUpdatedAt("oldest", NOW.minusSeconds(10)), 0);
            compareAndSave("middle", activeEnvelopeUpdatedAt("middle", NOW.minusSeconds(5)), 0);
            compareAndSave("newest", activeEnvelopeUpdatedAt("newest", NOW), 0);

            List<SagaEnvelope<String>> found = findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(1), 2);

            assertThat(found).extracting(SagaEnvelope::sagaId).containsExactly("oldest", "middle");
        }

        @Test
        void rejects_a_limit_below_1() {
            assertThatThrownBy(() -> findByStatus(SagaStatus.ACTIVE, NOW, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("limit must be positive");
        }

        @Test
        void excludes_an_envelope_with_a_null_updatedAt() {
            SagaEnvelope<String> noUpdatedAt = new SagaEnvelope<>("no-updated-at", "a", SagaStatus.ACTIVE, 1,
                    List.of(), Map.of(), null, NOW, null, null, null, true, null, Map.of(), null);
            compareAndSave("no-updated-at", noUpdatedAt, 0);

            assertThat(findByStatus(SagaStatus.ACTIVE, NOW.plusSeconds(60), 10)).isEmpty();
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
                    createdAt, updatedAt, completedAt, null, true, null, Map.of(), null);
            compareAndSave("s1", saved, 0);

            SagaEnvelope<String> found = find("s1").orElseThrow();

            assertAll(
                    () -> assertThat(found.createdAt()).isEqualTo(createdAt),
                    () -> assertThat(found.updatedAt()).isEqualTo(updatedAt),
                    () -> assertThat(found.completedAt()).isEqualTo(completedAt)
            );
        }

        @Test
        void an_active_instance_has_no_completedAt() {
            compareAndSave("s2", envelope("s2", "a", 1, List.of()), 0);

            assertThat(find("s2")).hasValueSatisfying(e -> assertThat(e.completedAt()).isNull());
        }
    }

    @Nested
    class Delete {

        @Test
        void removes_the_instance() {
            compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);

            store.delete("s1");

            assertThat(find("s1")).isEmpty();
        }

        @Test
        void is_a_no_op_for_an_unknown_id() {
            store.delete("unknown");

            assertThat(find("unknown")).isEmpty();
        }
    }

    @Nested
    class FindReleasingSagaIds {

        @Test
        void lists_the_releasing_instances_in_ascending_id_order_up_to_the_limit() {
            compareAndSave("c", releasingEnvelope("c"), 0);
            compareAndSave("a", releasingEnvelope("a"), 0);
            compareAndSave("d", releasingEnvelope("d"), 0);
            compareAndSave("b", releasingEnvelope("b"), 0);
            compareAndSave("0", envelope("0", "x", 1, List.of()), 0);
            compareAndSave("1", quarantinedEnvelope("1", "x", 1, List.of()), 0);

            assertAll(
                    () -> assertThat(store.findReleasingSagaIds(null, 10)).containsExactly("a", "b", "c", "d"),
                    () -> assertThat(store.findReleasingSagaIds(null, 3)).containsExactly("a", "b", "c")
            );
        }

        @Test
        void lists_only_the_ids_strictly_after_the_given_one_in_ascending_order_up_to_the_limit() {
            for (String id : List.of("c", "a", "d", "b")) {
                compareAndSave(id, releasingEnvelope(id), 0);
            }

            assertAll(
                    () -> assertThat(store.findReleasingSagaIds("a", 10)).containsExactly("b", "c", "d"),
                    () -> assertThat(store.findReleasingSagaIds("a", 2)).containsExactly("b", "c"),
                    () -> assertThat(store.findReleasingSagaIds("b", 1)).containsExactly("c"),
                    () -> assertThat(store.findReleasingSagaIds("d", 10)).isEmpty()
            );
        }

        @Test
        void lists_ids_in_string_order_whatever_order_the_backing_map_iterates_them_in() {
            // The map iterates these as x10, x1, y, x2
            for (String id : List.of("x1", "x10", "x2", "y")) {
                compareAndSave(id, releasingEnvelope(id), 0);
            }

            assertAll(
                    () -> assertThat(store.findReleasingSagaIds(null, 10)).containsExactly("x1", "x10", "x2", "y"),
                    () -> assertThat(store.findReleasingSagaIds(null, 2)).containsExactly("x1", "x10"),
                    () -> assertThat(store.findReleasingSagaIds("x1", 10)).containsExactly("x10", "x2", "y")
            );
        }

        @Test
        void lists_the_ids_after_one_that_is_not_there_and_is_not_releasing() {
            compareAndSave("a", releasingEnvelope("a"), 0);
            compareAndSave("c", releasingEnvelope("c"), 0);
            compareAndSave("b", envelope("b", "x", 1, List.of()), 0);

            assertAll(
                    () -> assertThat(store.findReleasingSagaIds("b", 10)).containsExactly("c"),
                    () -> assertThat(store.findReleasingSagaIds("bb", 10)).containsExactly("c")
            );
        }

        @Test
        void cannot_be_read_with_a_limit_below_1() {
            assertThatThrownBy(() -> store.findReleasingSagaIds(null, 0)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class SkippedInputRecords {

        @Test
        void are_read_in_seq_order_whatever_order_they_were_written_in() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(7, "b@2"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(-1, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "c@3"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(3, "c@3"), new SkippedInput(7, "b@2"));
        }

        @Test
        void with_the_same_seq_are_read_in_key_order_whatever_order_they_were_written_in() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(-1, "b@2"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(-1, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "g@7"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "f@6"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "e@5"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "d@4"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(3, "c@3"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(
                    new SkippedInput(-1, "a@1"), new SkippedInput(-1, "b@2"),
                    new SkippedInput(3, "c@3"), new SkippedInput(3, "d@4"), new SkippedInput(3, "e@5"), new SkippedInput(3, "f@6"), new SkippedInput(3, "g@7"));
        }

        @Test
        void keep_the_first_record_of_a_key_and_its_seq() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(2, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(5, "a@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(2, "a@1"));
        }

        @Test
        void are_read_for_one_saga_and_quarantine_id_only() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q2", new SkippedInput(0, "b@1"));
            store.recordSkippedInput("s2", "q1", new SkippedInput(0, "c@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(0, "a@1"));
        }

        @Test
        void are_read_up_to_the_limit_from_the_lowest_seq() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(2, "c@3"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(1, "b@2"));

            assertThat(store.findSkippedInputs("s1", "q1", 2)).containsExactly(new SkippedInput(0, "a@1"), new SkippedInput(1, "b@2"));
        }

        @Test
        void cannot_be_read_with_a_limit_below_1() {
            assertThatThrownBy(() -> store.findSkippedInputs("s1", "q1", 0)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void get_a_record_of_a_key_that_has_none_when_recorded_at_the_head() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "b@2"));

            store.recordSkippedInputAtHead("s1", "q1", new SkippedInput(-1, "a@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(0, "b@2"));
        }

        @Test
        void move_the_record_of_a_key_to_the_seq_it_is_recorded_at_the_head_with_and_keep_one_record_of_it() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(4, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(5, "b@2"));

            store.recordSkippedInputAtHead("s1", "q1", new SkippedInput(-1, "a@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(-1, "a@1"), new SkippedInput(5, "b@2"));
        }

        @Test
        void keep_the_head_record_of_a_key_that_another_caller_records_at_a_later_seq_at_once() throws Exception {
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                for (int round = 0; round < 50; round++) {
                    String sagaId = "s" + round;
                    CountDownLatch go = new CountDownLatch(1);
                    List<Future<?>> writers = List.of(
                            executor.submit(() -> {
                                go.await();
                                store.recordSkippedInputAtHead(sagaId, "q1", new SkippedInput(-1, "a@1"));
                                return null;
                            }),
                            executor.submit(() -> {
                                go.await();
                                store.recordSkippedInput(sagaId, "q1", new SkippedInput(7, "a@1"));
                                return null;
                            }));
                    go.countDown();
                    for (Future<?> writer : writers) {
                        writer.get(10, TimeUnit.SECONDS);
                    }

                    assertThat(store.findSkippedInputs(sagaId, "q1", 10)).hasSize(1).singleElement()
                            .isEqualTo(new SkippedInput(-1, "a@1"));
                }
            } finally {
                executor.shutdownNow();
            }
        }

        @Test
        void are_deleted_exactly_when_the_key_and_the_seq_both_match() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q1", new SkippedInput(1, "b@2"));

            store.deleteSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(1, "b@2"));
        }

        @Test
        void keep_a_record_of_the_same_key_with_another_seq_when_deleted_exactly() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(-1, "a@1"));

            store.deleteSkippedInput("s1", "q1", new SkippedInput(3, "a@1"));

            assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(-1, "a@1"));
        }

        @Test
        void keep_a_record_of_the_same_key_and_seq_under_another_quarantine_id_or_instance_when_deleted_exactly() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q2", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s2", "q1", new SkippedInput(0, "a@1"));

            store.deleteSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));

            assertAll(
                    () -> assertThat(store.findSkippedInputs("s1", "q1", 10)).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("s1", "q2", 10)).containsExactly(new SkippedInput(0, "a@1")),
                    () -> assertThat(store.findSkippedInputs("s2", "q1", 10)).containsExactly(new SkippedInput(0, "a@1"))
            );
        }

        @Test
        void are_deleted_with_the_instance_under_its_quarantine_id_only_and_only_for_that_instance() {
            compareAndSave("s1", releasingEnvelope("s1"), 0);
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q2", new SkippedInput(0, "b@1"));
            store.recordSkippedInput("s2", "q1", new SkippedInput(0, "c@1"));

            store.delete("s1");

            assertAll(
                    () -> assertThat(find("s1")).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("s1", "q1", 10)).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("s1", "q2", 10)).containsExactly(new SkippedInput(0, "b@1")),
                    () -> assertThat(store.findSkippedInputs("s2", "q1", 10)).containsExactly(new SkippedInput(0, "c@1"))
            );
        }

        @Test
        void are_kept_when_the_instance_deleted_has_no_quarantine_id_or_there_is_none() {
            compareAndSave("s1", envelope("s1", "a", 1, List.of()), 0);
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));

            store.delete("s1");
            store.delete("missing");

            assertAll(
                    () -> assertThat(find("s1")).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("s1", "q1", 10)).containsExactly(new SkippedInput(0, "a@1"))
            );
        }

        @Test
        void are_deleted_for_one_quarantine_id_only_by_the_bulk_delete() {
            store.recordSkippedInput("s1", "q1", new SkippedInput(0, "a@1"));
            store.recordSkippedInput("s1", "q2", new SkippedInput(0, "b@1"));
            store.recordSkippedInput("s2", "q1", new SkippedInput(0, "c@1"));

            store.deleteSkippedInputs("s1", "q1");

            assertAll(
                    () -> assertThat(store.findSkippedInputs("s1", "q1", 10)).isEmpty(),
                    () -> assertThat(store.findSkippedInputs("s1", "q2", 10)).containsExactly(new SkippedInput(0, "b@1")),
                    () -> assertThat(store.findSkippedInputs("s2", "q1", 10)).containsExactly(new SkippedInput(0, "c@1"))
            );
        }

        // One writer records seq 0, 1, 2 and so on, each call completing before the next starts, so any one read of the
        // records must hold a prefix of them and its lowest seq is 0 as soon as it holds any. Other writers fill the store with
        // records of other instances, which is what a read of one instance iterates past when all records share one map
        @Test
        void read_the_lowest_seq_recorded_before_the_read_while_other_skips_record_concurrently() throws Exception {
            int recorded = 10_000;
            for (int i = 0; i < 20_000; i++) {
                store.recordSkippedInput("other-" + i, "q1", new SkippedInput(i, "x@" + i));
            }
            AtomicLong completed = new AtomicLong();
            AtomicReference<String> violation = new AtomicReference<>();
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                Future<?> writer = executor.submit(() -> {
                    go.await();
                    for (int i = 0; i < recorded; i++) {
                        store.recordSkippedInput("s1", "q1", new SkippedInput(i, "s@" + i));
                        completed.incrementAndGet();
                    }
                    return null;
                });
                List<Future<Object>> noise = List.of(1, 2).stream().map(thread -> executor.submit(() -> {
                    go.await();
                    for (int i = 0; i < recorded && completed.get() < recorded; i++) {
                        store.recordSkippedInput("noise-" + thread + "-" + i, "q1", new SkippedInput(i, "n@" + i));
                    }
                    return null;
                })).toList();
                Future<?> reader = executor.submit(() -> {
                    go.await();
                    while (completed.get() < recorded) {
                        long recordedBeforeTheRead = completed.get();
                        List<SkippedInput> lowest = store.findSkippedInputs("s1", "q1", 1);
                        if (recordedBeforeTheRead > 0 && lowest.isEmpty()) {
                            violation.compareAndSet(null, "empty after " + recordedBeforeTheRead + " records");
                        } else if (!lowest.isEmpty() && lowest.getFirst().seq() != 0) {
                            violation.compareAndSet(null, "lowest was " + lowest.getFirst() + " after " + recordedBeforeTheRead + " records");
                        }
                        List<SkippedInput> all = store.findSkippedInputs("s1", "q1", Integer.MAX_VALUE);
                        for (int i = 0; i < all.size(); i++) {
                            if (all.get(i).seq() != i) {
                                violation.compareAndSet(null, "read " + all.size() + " records and the one at " + i + " was " + all.get(i));
                                break;
                            }
                        }
                    }
                    return null;
                });
                go.countDown();
                writer.get(20, TimeUnit.SECONDS);
                reader.get(20, TimeUnit.SECONDS);
                for (Future<?> future : noise) {
                    future.get(20, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdownNow();
            }

            assertThat(violation.get()).as("a read of the records of one instance").isNull();
        }
    }

    @Nested
    class CompareAndSaveWithoutState {

        @Test
        void leaves_an_instance_that_stores_no_state_and_no_current_step_without_both_when_the_given_fields_hold_them() {
            compareAndSave("s1", stepped("s1", null, 1), 0);
            StoredSaga given = withCurrentStep(codec.encode(stepped("s1", "given-state", 2)), "given-step");
            assertThat(given.encoded().fields()).containsKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP);

            boolean saved = store.compareAndSaveWithoutState(given, 1);

            assertThat(saved).isTrue();
            assertThat(store.find("s1")).hasValueSatisfying(stored -> assertThat(stored.encoded().fields())
                    .doesNotContainKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP));
        }

        @Test
        void keeps_the_state_and_current_step_of_the_stored_instance_over_the_ones_in_the_given_fields() {
            store.compareAndSave(withCurrentStep(codec.encode(stepped("s1", "stored-state", 1)), "stored-step"), 0);
            Map<String, Object> before = store.find("s1").orElseThrow().encoded().fields();
            assertThat(before).containsKeys(SagaCodec.STATE, SagaCodec.CURRENT_STEP);

            boolean saved = store.compareAndSaveWithoutState(withCurrentStep(codec.encode(stepped("s1", "given-state", 2)), "given-step"), 1);

            assertThat(saved).isTrue();
            Map<String, Object> after = store.find("s1").orElseThrow().encoded().fields();
            assertAll(
                    () -> assertThat(after).containsEntry(SagaCodec.STATE, before.get(SagaCodec.STATE)),
                    () -> assertThat(after).containsEntry(SagaCodec.CURRENT_STEP, "stored-step"));
        }
    }

    private static SagaEnvelope<String> stepped(String sagaId, String state, long version) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW, NOW, null, null, true, null, Map.of(), null);
    }

    // The step a flow saga stores next to its state, which the codec writes only for a flow state
    private static StoredSaga withCurrentStep(StoredSaga saga, String currentStep) {
        Map<String, Object> fields = new LinkedHashMap<>(saga.encoded().fields());
        fields.put(SagaCodec.CURRENT_STEP, currentStep);
        return new StoredSaga(saga.sagaId(), saga.version(), new EncodedSaga(fields), saga.index());
    }
}
