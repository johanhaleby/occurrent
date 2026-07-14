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

package org.occurrent.example.domain.dcbpatterns.idempotency

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.math.BigDecimal
import java.net.URI
import java.time.Instant
import java.util.UUID

class IdempotencyTest {

    private val eventStore = InMemoryEventStore()
    private val converter: CloudEventConverter<OrderEvent> = JacksonCloudEventConverter.Builder<OrderEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:dcb-patterns"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(OrderEvent::class.java))
        .idMapper { it.eventId.toString() }
        .build()
    private val applicationService: DcbApplicationService<OrderEvent> = GenericDcbApplicationService(eventStore, converter)

    @Test
    fun `first placement stores an event`() {
        applicationService.execute(PlaceOrder(UUID.randomUUID(), "token-1", BigDecimal("19.99"), Instant.now()), orderDcbDecider)

        assertThat(eventStore.read(DcbCriteria.type("OrderPlaced")).events()).hasSize(1)
    }

    @Test
    fun `retrying with the same idempotency token is a no-op`() {
        val orderId = UUID.randomUUID()
        val placedAt = Instant.now()
        applicationService.execute(PlaceOrder(orderId, "token-1", BigDecimal("19.99"), placedAt), orderDcbDecider)

        val result = applicationService.execute(PlaceOrder(orderId, "token-1", BigDecimal("19.99"), placedAt.plusSeconds(5)), orderDcbDecider)

        assertThat(result).isNull()
        assertThat(eventStore.read(DcbCriteria.type("OrderPlaced")).events()).hasSize(1)
    }

    @Test
    fun `a different idempotency token stores a new order`() {
        applicationService.execute(PlaceOrder(UUID.randomUUID(), "token-1", BigDecimal("19.99"), Instant.now()), orderDcbDecider)
        applicationService.execute(PlaceOrder(UUID.randomUUID(), "token-2", BigDecimal("29.99"), Instant.now()), orderDcbDecider)

        assertThat(eventStore.read(DcbCriteria.type("OrderPlaced")).events()).hasSize(2)
    }
}
