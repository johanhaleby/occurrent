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

/**
 * Covers the {@link ReceivedEvents#any(Class)} and {@link ReceivedEvents#none(Class)} default methods. The other
 * members ({@code initiating}, {@code first}, {@code all}, {@code count}) are covered elsewhere, this file is only
 * about the two boolean queries built on top of {@code first}.
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
}
