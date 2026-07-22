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

package org.occurrent.dsl.saga.flow

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Proves that the reified Kotlin extensions over [ReceivedEvents] delegate to their Class-taking members correctly and
 * expose Kotlin-idiomatic return types (`T?` rather than `Optional<T>`).
 */
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class ReceivedEventsExtensionsTest {

    sealed interface OrderEvent
    data class OrderPlaced(val orderId: String) : OrderEvent
    data class PaymentReserved(val orderId: String) : OrderEvent
    data class PaymentFailed(val orderId: String, val attempt: Int) : OrderEvent

    private fun received(vararg events: OrderEvent): ReceivedEvents<OrderEvent> =
        ReceivedEvents.of(events.toList())

    @Nested
    inner class Initiating {

        @Test
        fun `returns the start event cast to the requested type`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1))

            assertThat(received.initiating<OrderPlaced>()).isEqualTo(OrderPlaced("o1"))
        }

        @Test
        fun `throws when the start event is not of the requested type`() {
            val received = received(OrderPlaced("o1"))

            assertThatThrownBy { received.initiating<PaymentReserved>() }
                .isInstanceOf(ClassCastException::class.java)
        }
    }

    @Nested
    inner class First {

        @Test
        fun `returns the first received event of the type`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1), PaymentFailed("o1", 2))

            assertThat(received.first<PaymentFailed>()).isEqualTo(PaymentFailed("o1", 1))
        }

        @Test
        fun `returns null when no event of the type was received`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1))

            assertThat(received.first<PaymentReserved>()).isNull()
        }
    }

    @Nested
    inner class All {

        @Test
        fun `returns every event of the type in arrival order`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1), PaymentReserved("o1"), PaymentFailed("o1", 2))

            assertThat(received.all<PaymentFailed>())
                .containsExactly(PaymentFailed("o1", 1), PaymentFailed("o1", 2))
        }

        @Test
        fun `returns an empty list when no event of the type was received`() {
            val received = received(OrderPlaced("o1"))

            assertThat(received.all<PaymentFailed>()).isEmpty()
        }
    }

    @Nested
    inner class Count {

        @Test
        fun `counts the events of the type`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1), PaymentFailed("o1", 2))

            assertThat(received.count<PaymentFailed>()).isEqualTo(2)
        }

        @Test
        fun `is zero when no event of the type was received`() {
            val received = received(OrderPlaced("o1"))

            assertThat(received.count<PaymentReserved>()).isZero()
        }
    }

    @Nested
    inner class Any {

        @Test
        fun `is true when an event of the type was received`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1))

            assertThat(received.any<PaymentFailed>()).isTrue()
        }

        @Test
        fun `is false when no event of the type was received`() {
            val received = received(OrderPlaced("o1"), PaymentFailed("o1", 1))

            assertThat(received.any<PaymentReserved>()).isFalse()
        }
    }
}
