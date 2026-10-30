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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySagaStateStore held events")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaHeldEventsTest {

    private static final String SAGA_ID = "order-1";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();

    private static SagaEnvelope<String> envelope(long version, SagaStatus status, boolean started) {
        return new SagaEnvelope<>(SAGA_ID, "state", status, version, List.of(), Map.of(), null, NOW, NOW, null, null, started, null,
                Map.of(), new SagaEnvelope.StartMark("orders", 1));
    }

    private void saveActiveStartedInstance() {
        assertThat(store.compareAndSave(SAGA_ID, envelope(1, SagaStatus.ACTIVE, true), 0)).isTrue();
    }

    private static CloudEvent cloudEvent(String id, String data) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.Event")
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static HeldEvent heldEvent(String key) {
        return heldEvent(key, false, null);
    }

    private static HeldEvent heldEvent(String key, boolean startEvent, @Nullable Instant failingSince) {
        return new HeldEvent(SAGA_ID, new HeldEventRef(key, "orders", 5L, 50L, "test.Event", NOW, startEvent, failingSince), cloudEvent("id-" + key, "{\"key\":\"" + key + "\"}"));
    }

    @Nested
    @DisplayName("when an event is held")
    class WhenAnEventIsHeld {

        @Test
        void hold_answers_true_for_an_event_that_was_not_held() {
            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }

        @Test
        void hold_answers_false_for_a_second_event_with_the_same_key() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isFalse();
        }

        @Test
        void a_second_hold_of_the_same_key_leaves_the_first_copy_in_place() {
            // Given
            HeldEvent first = heldEvent("orders@5", true, NOW);
            store.hold(SAGA_ID, first);

            // When
            store.hold(SAGA_ID, heldEvent("orders@5", false, null));

            // Then
            assertThat(store.held(SAGA_ID, List.of("orders@5"))).containsExactly(first);
        }

        @Test
        void the_same_key_can_be_held_for_two_instances() {
            // Given
            store.hold("order-1", heldEvent("orders@5"));

            // When
            boolean inserted = store.hold("order-2", heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }

        @Test
        void heldIndex_lists_the_reference_of_every_event_held_for_the_instance() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            HeldEvent second = heldEvent("orders@6", true, NOW.plusSeconds(1));
            store.hold(SAGA_ID, first);
            store.hold(SAGA_ID, second);
            store.hold("another-instance", heldEvent("orders@7"));

            // When
            List<HeldEventRef> index = store.heldIndex(SAGA_ID);

            // Then
            assertThat(index).containsExactlyInAnyOrder(first.ref(), second.ref());
        }

        @Test
        void heldIndex_is_empty_for_an_instance_that_holds_nothing() {
            // When
            List<HeldEventRef> index = store.heldIndex(SAGA_ID);

            // Then
            assertThat(index).isEmpty();
        }

        @Test
        void held_returns_the_events_with_the_requested_keys_and_their_bodies() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            HeldEvent second = heldEvent("orders@6");
            store.hold(SAGA_ID, first);
            store.hold(SAGA_ID, second);
            store.hold(SAGA_ID, heldEvent("orders@7"));

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5", "orders@6"));

            // Then
            assertThat(held).containsExactlyInAnyOrder(first, second);
        }

        @Test
        void held_leaves_out_a_key_that_is_not_held() {
            // Given
            HeldEvent first = heldEvent("orders@5");
            store.hold(SAGA_ID, first);

            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5", "orders@99"));

            // Then
            assertThat(held).containsExactly(first);
        }

        @Test
        void held_is_empty_for_an_instance_that_holds_nothing() {
            // When
            List<HeldEvent> held = store.held(SAGA_ID, List.of("orders@5"));

            // Then
            assertThat(held).isEmpty();
        }
    }

    @Nested
    @DisplayName("when a held event is released")
    class WhenAHeldEventIsReleased {

        @Test
        void release_removes_only_the_event_with_that_key() {
            // Given
            HeldEvent kept = heldEvent("orders@6");
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.hold(SAGA_ID, kept);

            // When
            store.release(SAGA_ID, "orders@5");

            // Then
            assertThat(store.heldIndex(SAGA_ID)).containsExactly(kept.ref());
        }

        @Test
        void release_of_an_event_that_is_not_held_changes_nothing() {
            // Given
            HeldEvent held = heldEvent("orders@5");
            store.hold(SAGA_ID, held);

            // When
            store.release(SAGA_ID, "orders@99");

            // Then
            assertThat(store.heldIndex(SAGA_ID)).containsExactly(held.ref());
        }

        @Test
        void release_of_the_last_event_leaves_the_instance_holding_nothing() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.release(SAGA_ID, "orders@5");

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void an_event_can_be_held_again_after_it_was_released() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.release(SAGA_ID, "orders@5");

            // When
            boolean inserted = store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(inserted).isTrue();
        }
    }

    @Nested
    @DisplayName("when a held event is recorded as failing")
    class WhenAHeldEventIsRecordedAsFailing {

        private final Instant firstFailure = NOW.plusSeconds(10);

        @Test
        void failing_sets_the_instant_on_a_held_event_that_has_none() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(firstFailure)),
                    () -> assertThat(store.held(SAGA_ID, List.of("orders@5"))).singleElement().satisfies(held -> assertThat(held.ref().failingSince()).isEqualTo(firstFailure)));
        }

        @Test
        void a_second_call_does_not_move_the_instant() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure.plusSeconds(30));

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(firstFailure));
        }

        @Test
        void a_copy_held_with_an_instant_keeps_it_when_failing_is_called() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5", false, NOW));

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isEqualTo(NOW));
        }

        @Test
        void failing_changes_nothing_but_the_instant() {
            // Given
            HeldEvent held = heldEvent("orders@5", true, null);
            store.hold(SAGA_ID, held);

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            HeldEvent after = store.held(SAGA_ID, List.of("orders@5")).get(0);
            HeldEventRef ref = held.ref();
            assertThat(after).isEqualTo(new HeldEvent(SAGA_ID, new HeldEventRef(ref.key(), ref.streamId(), ref.version(), ref.position(), ref.type(), ref.heldAt(), true, firstFailure), held.cloudEvent()));
        }

        @Test
        void failing_leaves_the_other_copies_of_the_instance_and_the_same_key_of_another_instance_alone() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.hold(SAGA_ID, heldEvent("orders@6"));
            store.hold("another-instance", heldEvent("orders@5"));

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex(SAGA_ID)).filteredOn(ref -> ref.key().equals("orders@6")).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isNull()),
                    () -> assertThat(store.heldIndex("another-instance")).singleElement().satisfies(ref -> assertThat(ref.failingSince()).isNull()));
        }

        @Test
        void failing_does_nothing_for_a_key_that_is_not_held() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.failing(SAGA_ID, "orders@99", firstFailure);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertAll(
                    () -> assertThat(ref.key()).isEqualTo("orders@5"),
                    () -> assertThat(ref.failingSince()).isNull()));
        }

        @Test
        void failing_does_not_insert_an_event_for_an_instance_that_holds_nothing() {
            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void failing_does_not_insert_a_copy_after_the_event_was_released() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));
            store.release(SAGA_ID, "orders@5");

            // When
            store.failing(SAGA_ID, "orders@5", firstFailure);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }
    }

    @Nested
    @DisplayName("when a held event is read back")
    class WhenAHeldEventIsReadBack {

        @Test
        void startEvent_and_failingSince_round_trip_through_heldIndex_and_held() {
            // Given
            HeldEvent held = heldEvent("orders@5", true, NOW.plusSeconds(3));
            store.hold(SAGA_ID, held);

            // Then
            assertAll(
                    () -> assertThat(store.heldIndex(SAGA_ID)).containsExactly(held.ref()),
                    () -> assertThat(store.held(SAGA_ID, List.of("orders@5"))).containsExactly(held));
        }

        @Test
        void a_copy_held_with_neither_reads_back_as_not_a_start_event_and_not_failing() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // Then
            assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertAll(
                    () -> assertThat(ref.startEvent()).isFalse(),
                    () -> assertThat(ref.failingSince()).isNull()));
        }
    }

    @Nested
    @DisplayName("when the instance is deleted")
    class WhenTheInstanceIsDeleted {

        @Test
        void delete_removes_the_events_held_for_the_instance() {
            // Given
            saveActiveStartedInstance();
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void delete_leaves_the_events_held_for_another_instance() {
            // Given
            saveActiveStartedInstance();
            HeldEvent other = heldEvent("orders@5");
            store.hold("another-instance", other);

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex("another-instance")).containsExactly(other.ref());
        }
    }

    @Nested
    @DisplayName("when the held events are listed without those waiting for a lower start")
    class WhenTheHeldEventsAreListedWithoutThoseWaitingForALowerStart {

        private final SagaEnvelope.StartMark start = new SagaEnvelope.StartMark("orders", 5);

        @BeforeEach
        void holdOneCopyOfEachKind() {
            store.hold(SAGA_ID, copy("orders@3", "orders", 3L, false));
            store.hold(SAGA_ID, copy("orders@4", "orders", 4L, false));
            store.hold(SAGA_ID, copy("orders@5", "orders", 5L, false));
            store.hold(SAGA_ID, copy("orders@7", "orders", 7L, false));
            store.hold(SAGA_ID, copy("orders@2", "orders", 2L, true));
            store.hold(SAGA_ID, copy("other@3", "other", 3L, false));
            store.hold(SAGA_ID, copy("keyless", null, null, false));
        }

        @Test
        void a_non_start_copy_on_the_starts_stream_below_the_start_is_left_out() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).doesNotContain("orders@3", "orders@4");
        }

        @Test
        void a_non_start_copy_at_the_starts_version_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@5");
        }

        @Test
        void a_non_start_copy_above_the_start_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@7");
        }

        @Test
        void a_copy_flagged_as_a_start_event_below_the_start_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("orders@2");
        }

        @Test
        void a_copy_of_another_stream_below_the_starts_version_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("other@3");
        }

        @Test
        void a_copy_with_no_stream_key_is_returned() {
            // When
            List<String> keys = keysWithoutWaiting();

            // Then
            assertThat(keys).contains("keyless");
        }

        @Test
        void the_events_returned_are_those_of_heldIndex_less_the_waiting_ones() {
            // When
            List<HeldEventRef> listed = store.heldIndexWithoutWaiting(SAGA_ID, start);

            // Then
            assertThat(listed).containsExactlyInAnyOrderElementsOf(store.heldIndex(SAGA_ID).stream()
                    .filter(ref -> !ref.key().equals("orders@3") && !ref.key().equals("orders@4")).toList());
        }

        private List<String> keysWithoutWaiting() {
            return store.heldIndexWithoutWaiting(SAGA_ID, start).stream().map(HeldEventRef::key).toList();
        }

        private HeldEvent copy(String key, @Nullable String streamId, @Nullable Long version, boolean startEvent) {
            return new HeldEvent(SAGA_ID, new HeldEventRef(key, streamId, version, null, "test.Event", NOW, startEvent, null), cloudEvent("id-" + key, "{}"));
        }
    }
}
