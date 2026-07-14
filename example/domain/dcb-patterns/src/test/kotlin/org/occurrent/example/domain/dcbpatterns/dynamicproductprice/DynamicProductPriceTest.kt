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

package org.occurrent.example.domain.dcbpatterns.dynamicproductprice

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.application.service.blocking.dcb.DcbApplicationService
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService
import org.occurrent.dsl.dcb.blocking.execute
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.math.BigDecimal
import java.net.URI
import java.time.Instant
import java.util.UUID

class DynamicProductPriceTest {

    private val eventStore = InMemoryEventStore()
    private val converter: CloudEventConverter<ProductPriceEvent> = JacksonCloudEventConverter.Builder<ProductPriceEvent>(jacksonObjectMapper(), URI.create("urn:occurrent:example:dcb-patterns"))
        .typeMapper(ReflectionCloudEventTypeMapper.simple(ProductPriceEvent::class.java))
        .idMapper { it.eventId.toString() }
        .build()
    private val applicationService: DcbApplicationService<ProductPriceEvent> = GenericDcbApplicationService(eventStore, converter)

    private val productId: UUID = UUID.randomUUID()
    private val definedAt = Instant.parse("2026-01-01T00:00:00Z")

    @Test
    fun `ordering at the current price is accepted`() {
        applicationService.execute(ProductPriceCommand.DefineProduct(productId, BigDecimal("100.00"), definedAt), productPriceDcbDecider)

        applicationService.execute(
            ProductPriceCommand.PlacePriceOrder(productId, UUID.randomUUID(), BigDecimal("100.00"), definedAt.plusSeconds(60)),
            productPriceDcbDecider
        )
        // No exception means the order was accepted.
    }

    @Test
    fun `ordering at a just-superseded price within the grace period is accepted`() {
        applicationService.execute(ProductPriceCommand.DefineProduct(productId, BigDecimal("100.00"), definedAt), productPriceDcbDecider)
        val changedAt = definedAt.plusSeconds(3600)
        applicationService.execute(ProductPriceCommand.ChangeProductPrice(productId, BigDecimal("120.00"), changedAt), productPriceDcbDecider)

        val orderedAt = changedAt.plus(ProductPricePolicy.GRACE_PERIOD).minusSeconds(1)
        applicationService.execute(ProductPriceCommand.PlacePriceOrder(productId, UUID.randomUUID(), BigDecimal("100.00"), orderedAt), productPriceDcbDecider)
        // No exception means the superseded price was still honored.
    }

    @Test
    fun `ordering at a superseded price outside the grace period is rejected`() {
        applicationService.execute(ProductPriceCommand.DefineProduct(productId, BigDecimal("100.00"), definedAt), productPriceDcbDecider)
        val changedAt = definedAt.plusSeconds(3600)
        applicationService.execute(ProductPriceCommand.ChangeProductPrice(productId, BigDecimal("120.00"), changedAt), productPriceDcbDecider)

        val orderedAt = changedAt.plus(ProductPricePolicy.GRACE_PERIOD).plusSeconds(1)
        assertThatThrownBy {
            applicationService.execute(ProductPriceCommand.PlacePriceOrder(productId, UUID.randomUUID(), BigDecimal("100.00"), orderedAt), productPriceDcbDecider)
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("no longer valid")
    }
}
