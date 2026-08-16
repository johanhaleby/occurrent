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

package org.occurrent.dsl.subscription

import io.cloudevents.CloudEvent
import io.cloudevents.core.builder.CloudEventBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.condition.Condition
import org.occurrent.filter.Filter
import java.net.URI

sealed interface OrderEvent

data class OrderPlaced(val orderId: String) : OrderEvent

data class PaymentReserved(val orderId: String) : OrderEvent

sealed interface ReopenedEvent

// Sealed above, plain abstract here, so nothing below this class can be found.
abstract class ReopenedBase : ReopenedEvent

/**
 * [filterFromEventTypes] derives its filter through `EventTypeExpansion.deriveFilter`, so a declared sealed type asks
 * for every concrete type it permits (ADR 126).
 */
@DisplayName("SubscriptionFilters")
@DisplayNameGeneration(ReplaceUnderscores::class)
class SubscriptionFiltersTest {

    @Test
    fun `no declared event types matches everything`() {
        val filter = filterFromEventTypes(converter<OrderEvent>(), emptyArray())

        assertThat(filter).isEqualTo(Filter.all())
    }

    @Test
    fun `one declared concrete type derives a single type condition`() {
        val filter = filterFromEventTypes(converter<OrderEvent>(), arrayOf(OrderPlaced::class))

        assertThat(filter).isEqualTo(Filter.type(Condition.eq("OrderPlaced")))
    }

    @Test
    fun `a declared sealed type asks for every concrete type it permits`() {
        val filter = filterFromEventTypes(converter<OrderEvent>(), arrayOf(OrderEvent::class))

        assertThat(filter).isEqualTo(
            Filter.type(Condition.or(listOf(Condition.eq("OrderEvent"), Condition.eq("OrderPlaced"), Condition.eq("PaymentReserved"))))
        )
    }

    @Test
    fun `a declared sealed type reopened below it is refused`() {
        assertThatThrownBy { filterFromEventTypes(converter<ReopenedEvent>(), arrayOf(ReopenedEvent::class)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(ReopenedEvent::class.java.name)
    }

    /** One CloudEvent type per concrete class, so a derived type filter can tell the types apart. */
    private fun <E : Any> converter(): CloudEventConverter<E> = object : CloudEventConverter<E> {
        override fun toCloudEvent(domainEvent: E): CloudEvent =
            CloudEventBuilder.v1().withId("id").withSource(URI.create("urn:test")).withType(domainEvent.javaClass.simpleName).build()

        override fun toDomainEvent(cloudEvent: CloudEvent): E =
            throw UnsupportedOperationException("not needed for these tests")

        override fun getCloudEventType(type: Class<out E>): String = type.simpleName
    }
}
