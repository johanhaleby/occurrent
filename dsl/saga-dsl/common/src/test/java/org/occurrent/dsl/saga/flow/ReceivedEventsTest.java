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

package org.occurrent.dsl.saga.flow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the {@link ReceivedEvents#any(Class)} and {@link ReceivedEvents#none(Class)} default methods, plus the windowed
 * view a window-condition reaction reads. The other members ({@code initiating}, {@code first}, {@code all},
 * {@code count}) are covered elsewhere over the whole retained list.
 */
@DisplayName("ReceivedEvents")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReceivedEventsTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, PaymentFailed {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    record PaymentFailed(String orderId, int attempt) implements OrderEvent {
    }

    private static ReceivedEvents<OrderEvent> received(OrderEvent... events) {
        return ReceivedEvents.of(List.of(events));
    }

    @Nested
    class Any {

        @Test
        void is_true_when_an_event_of_the_type_was_received() {
            ReceivedEvents<OrderEvent> received = received(new OrderPlaced("o1"), new PaymentFailed("o1", 1));

            assertThat(received.any(PaymentFailed.class)).isTrue();
        }

        @Test
        void is_false_when_no_event_of_the_type_was_received() {
            ReceivedEvents<OrderEvent> received = received(new OrderPlaced("o1"), new PaymentFailed("o1", 1));

            assertThat(received.any(PaymentReserved.class)).isFalse();
        }
    }

    @Nested
    class None {

        @Test
        void is_true_when_no_event_of_the_type_was_received() {
            ReceivedEvents<OrderEvent> received = received(new OrderPlaced("o1"), new PaymentFailed("o1", 1));

            assertThat(received.none(PaymentReserved.class)).isTrue();
        }

        @Test
        void is_false_when_an_event_of_the_type_was_received() {
            ReceivedEvents<OrderEvent> received = received(new OrderPlaced("o1"), new PaymentFailed("o1", 1));

            assertThat(received.none(PaymentFailed.class)).isFalse();
        }

        @Test
        void is_the_logical_negation_of_any() {
            ReceivedEvents<OrderEvent> received = received(new OrderPlaced("o1"), new PaymentFailed("o1", 1));

            assertThat(received.none(PaymentFailed.class)).isEqualTo(!received.any(PaymentFailed.class));
            assertThat(received.none(PaymentReserved.class)).isEqualTo(!received.any(PaymentReserved.class));
        }
    }

    @Nested
    class WindowedView {

        @Test
        void answers_every_query_over_the_window_alone_while_initiating_still_reaches_element_zero() {
            List<OrderEvent> events = List.of(new OrderPlaced("o1"), new PaymentFailed("o1", 1), new PaymentFailed("o1", 2),
                    new PaymentReserved("o1"));

            ReceivedEvents<OrderEvent> window = new ReceivedEventsList<>(events, 2);

            assertThat(window.count(PaymentFailed.class)).as("only the second attempt is in the window").isEqualTo(1);
            assertThat(window.first(PaymentFailed.class)).contains(new PaymentFailed("o1", 2));
            assertThat(window.all(PaymentFailed.class)).containsExactly(new PaymentFailed("o1", 2));
            assertThat(window.any(OrderPlaced.class)).as("the initiating event is outside the window").isFalse();
            assertThat(window.asList()).containsExactly(new PaymentFailed("o1", 2), new PaymentReserved("o1"));
            assertThat(window.initiating(OrderPlaced.class)).as("initiating reaches past the window").isEqualTo(new OrderPlaced("o1"));
        }

        @Test
        void a_window_starting_at_the_end_is_empty_rather_than_invalid() {
            List<OrderEvent> events = List.of(new OrderPlaced("o1"), new PaymentReserved("o1"));

            ReceivedEvents<OrderEvent> window = new ReceivedEventsList<>(events, events.size());

            assertThat(window.asList()).isEmpty();
            assertThat(window.initiating()).isEqualTo(new OrderPlaced("o1"));
        }

        @Test
        void a_window_start_outside_the_received_events_is_refused() {
            List<OrderEvent> events = List.of(new OrderPlaced("o1"), new PaymentReserved("o1"));

            assertThatThrownBy(() -> new ReceivedEventsList<>(events, 3))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("windowStart");
            assertThatThrownBy(() -> new ReceivedEventsList<>(events, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("windowStart");
        }
    }
}
