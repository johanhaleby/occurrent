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
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("InMemorySagaStateStore held events")
@DisplayNameGeneration(ReplaceUnderscores.class)
class InMemorySagaHeldEventsTest {

    private static final String SAGA_ID = "order-1";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemorySagaStateStore<String> store = new InMemorySagaStateStore<>();

    private static SagaEnvelope<String> envelope(long version, SagaStatus status, boolean started, boolean heldEventsChecked, Set<String> selfHeldKeys) {
        return new SagaEnvelope<>(SAGA_ID, "state", status, version, List.of(), Map.of(), null, NOW, NOW, null, null, started, null,
                Map.of(), heldEventsChecked, Set.of("drained"), selfHeldKeys, new SagaEnvelope.StartMark("orders", 1, 10L));
    }

    private void saveActiveStartedInstance(boolean heldEventsChecked, Set<String> selfHeldKeys) {
        assertThat(store.compareAndSave(SAGA_ID, envelope(1, SagaStatus.ACTIVE, true, heldEventsChecked, selfHeldKeys), 0)).isTrue();
    }

    private static CloudEvent cloudEvent(String id, String data) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.Event")
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static HeldEvent heldEvent(String key) {
        return heldEvent(key, false, false);
    }

    private static HeldEvent heldEvent(String key, boolean selfHeld, boolean outstanding) {
        return new HeldEvent(SAGA_ID, new HeldEventRef(key, "orders", 5L, 50L, "test.Event", NOW, selfHeld, outstanding), cloudEvent("id-" + key, "{\"key\":\"" + key + "\"}"));
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
            HeldEvent first = heldEvent("orders@5", true, false);
            store.hold(SAGA_ID, first);

            // When
            store.hold(SAGA_ID, heldEvent("orders@5", false, true));

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
            HeldEvent second = heldEvent("orders@6", true, false);
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
    @DisplayName("when an event is marked outstanding")
    class WhenAnEventIsMarkedOutstanding {

        @Test
        void markOutstanding_answers_true_and_sets_the_flag_on_a_held_event() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5", true, false));

            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(marked).isTrue(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.outstanding()).isTrue())
            );
        }

        @Test
        void markOutstanding_changes_nothing_but_the_flag() {
            // Given
            HeldEvent held = heldEvent("orders@5", true, false);
            store.hold(SAGA_ID, held);

            // When
            store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            HeldEvent after = store.held(SAGA_ID, List.of("orders@5")).get(0);
            HeldEventRef ref = held.ref();
            assertThat(after).isEqualTo(new HeldEvent(SAGA_ID, new HeldEventRef(ref.key(), ref.streamId(), ref.version(), ref.position(), ref.type(), ref.heldAt(), true, true), held.cloudEvent()));
        }

        @Test
        void markOutstanding_answers_false_for_a_key_that_is_not_held() {
            // Given
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@99");

            // Then
            assertAll(
                    () -> assertThat(marked).isFalse(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).singleElement().satisfies(ref -> assertThat(ref.outstanding()).isFalse())
            );
        }

        @Test
        void markOutstanding_does_not_insert_an_event_for_an_instance_that_holds_nothing() {
            // When
            boolean marked = store.markOutstanding(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(marked).isFalse(),
                    () -> assertThat(store.heldIndex(SAGA_ID)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the instance is marked as holding events")
    class WhenTheInstanceIsMarkedAsHoldingEvents {

        @Test
        void markHeldEvents_returns_the_envelope_as_it_was_before_the_write() {
            // Given
            saveActiveStartedInstance(true, Set.of("orders@3"));

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isPresent(),
                    () -> assertThat(before.orElseThrow().version()).isEqualTo(1),
                    () -> assertThat(before.orElseThrow().heldEventsChecked()).isTrue(),
                    () -> assertThat(before.orElseThrow().selfHeldKeys()).containsExactly("orders@3")
            );
        }

        @Test
        void markHeldEvents_unsets_heldEventsChecked() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, null);

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().heldEventsChecked()).isFalse();
        }

        @Test
        void markHeldEvents_adds_the_key_to_the_selfHeldKeys_and_keeps_the_ones_already_there() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3"));

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactlyInAnyOrder("orders@3", "orders@5");
        }

        @Test
        void markHeldEvents_without_a_key_leaves_the_selfHeldKeys_as_they_were() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3"));

            // When
            store.markHeldEvents(SAGA_ID, null);

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactly("orders@3");
        }

        @Test
        void markHeldEvents_adding_a_key_already_there_leaves_it_once() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@5"));

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactly("orders@5");
        }

        @Test
        void markHeldEvents_raises_the_version_by_one() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().version()).isEqualTo(2);
        }

        @Test
        void markHeldEvents_makes_a_compareAndSave_that_read_before_it_lose() {
            // Given
            saveActiveStartedInstance(true, Set.of());
            SagaEnvelope<String> readBefore = store.find(SAGA_ID).orElseThrow();
            store.markHeldEvents(SAGA_ID, "orders@5");

            // When
            boolean saved = store.compareAndSave(SAGA_ID, envelope(readBefore.version() + 1, SagaStatus.ACTIVE, true, true, Set.of()), readBefore.version());

            // Then
            assertThat(saved).isFalse();
        }

        @Test
        void markHeldEvents_changes_nothing_else_about_the_instance() {
            // Given
            saveActiveStartedInstance(true, Set.of());

            // When
            store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            SagaEnvelope<String> after = store.find(SAGA_ID).orElseThrow();
            assertThat(after).isEqualTo(envelope(2, SagaStatus.ACTIVE, true, false, Set.of("orders@5")));
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_an_instance_that_does_not_exist() {
            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).isEmpty()
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_an_instance_that_has_not_started() {
            // Given
            SagaEnvelope<String> notStarted = envelope(1, SagaStatus.ACTIVE, false, true, Set.of());
            store.compareAndSave(SAGA_ID, notStarted, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(notStarted)
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_a_completed_instance() {
            // Given
            SagaEnvelope<String> completed = envelope(1, SagaStatus.COMPLETED, true, true, Set.of());
            store.compareAndSave(SAGA_ID, completed, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(completed)
            );
        }

        @Test
        void markHeldEvents_is_empty_and_writes_nothing_for_a_quarantined_instance() {
            // Given
            SagaEnvelope<String> quarantined = envelope(1, SagaStatus.QUARANTINED, true, true, Set.of());
            store.compareAndSave(SAGA_ID, quarantined, 0);

            // When
            Optional<SagaEnvelope<String>> before = store.markHeldEvents(SAGA_ID, "orders@5");

            // Then
            assertAll(
                    () -> assertThat(before).isEmpty(),
                    () -> assertThat(store.find(SAGA_ID)).contains(quarantined)
            );
        }
    }

    @Nested
    @DisplayName("when a key is unmarked as self-held")
    class WhenAKeyIsUnmarkedAsSelfHeld {

        @Test
        void unmarkSelfHeld_removes_the_key_and_keeps_the_others() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@3", "orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().selfHeldKeys()).containsExactly("orders@3");
        }

        @Test
        void unmarkSelfHeld_raises_the_version_by_one() {
            // Given
            saveActiveStartedInstance(false, Set.of("orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().version()).isEqualTo(2);
        }

        @Test
        void unmarkSelfHeld_leaves_heldEventsChecked_as_it_was() {
            // Given
            saveActiveStartedInstance(true, Set.of("orders@5"));

            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID).orElseThrow().heldEventsChecked()).isTrue();
        }

        @Test
        void unmarkSelfHeld_of_an_instance_that_does_not_exist_writes_nothing() {
            // When
            store.unmarkSelfHeld(SAGA_ID, "orders@5");

            // Then
            assertThat(store.find(SAGA_ID)).isEmpty();
        }
    }

    @Nested
    @DisplayName("when the instance is deleted")
    class WhenTheInstanceIsDeleted {

        @Test
        void delete_removes_the_events_held_for_the_instance() {
            // Given
            saveActiveStartedInstance(false, Set.of());
            store.hold(SAGA_ID, heldEvent("orders@5"));

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex(SAGA_ID)).isEmpty();
        }

        @Test
        void delete_leaves_the_events_held_for_another_instance() {
            // Given
            saveActiveStartedInstance(false, Set.of());
            HeldEvent other = heldEvent("orders@5");
            store.hold("another-instance", other);

            // When
            store.delete(SAGA_ID);

            // Then
            assertThat(store.heldIndex("another-instance")).containsExactly(other.ref());
        }
    }
}
