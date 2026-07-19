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
import org.occurrent.dsl.saga.SagaEnvelope.Status;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySagaStateStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaStateStoreTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();

    private static SagaEnvelope<String> envelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, Status.ACTIVE, version, timers, Map.of(), null, NOW, NOW, null);
    }

    private static SagaEnvelope<String> completedEnvelope(String sagaId, String state, long version, List<TimerEntry> timers) {
        return new SagaEnvelope<>(sagaId, state, Status.COMPLETED, version, timers, Map.of(), null, NOW, NOW, NOW);
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
        void respects_the_supplied_limit() {
            store.compareAndSave("a", envelope("a", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            store.compareAndSave("b", envelope("b", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);
            store.compareAndSave("c", envelope("c", "a", 1, List.of(new TimerEntry("t", NOW.toEpochMilli()))), 0);

            List<SagaEnvelope<String>> due = store.findWithDueTimers(NOW, 2);

            assertThat(due).hasSize(2);
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
