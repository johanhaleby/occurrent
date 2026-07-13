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

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Pattern: honor a displayed price for a short grace period after it changes. A shopper who loaded the product page
 * a moment before the price changed should not be rejected at checkout just because the price ticked over in the
 * meantime; [PlacePriceOrder.displayedPrice] is accepted if it was the live price at any point within
 * [ProductPricePolicy.GRACE_PERIOD] of [PlacePriceOrder.orderedAt].
 * <p>
 * Time is entirely in the payload: [ProductPriceChanged.changedAt] and [PlacePriceOrder.orderedAt] are plain
 * [Instant] fields, so the decision folds only over domain data, never CloudEvent metadata.
 */
val productPriceDcbDecider: DcbDecider<ProductPriceCommand, ProductPriceState, ProductPriceEvent> = dcbDecider(
    initialState = ProductPriceState(),
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

object ProductPricePolicy {
    /** How long a superseded price is still honored after a price change. */
    val GRACE_PERIOD: Duration = Duration.ofMinutes(10)
}

private fun productTag(productId: UUID): Tag = Tag.of("product", productId.toString())

private fun criteria(command: ProductPriceCommand): DcbCriteria = DcbCriteria.tags(productTag(command.productId))

private fun tags(event: ProductPriceEvent): Set<Tag> = setOf(productTag(event.productId))

sealed interface ProductPriceCommand {
    val productId: UUID

    data class DefineProduct(override val productId: UUID, val price: BigDecimal, val definedAt: Instant) : ProductPriceCommand
    data class ChangeProductPrice(override val productId: UUID, val newPrice: BigDecimal, val changedAt: Instant) : ProductPriceCommand
    data class PlacePriceOrder(override val productId: UUID, val orderId: UUID, val displayedPrice: BigDecimal, val orderedAt: Instant) : ProductPriceCommand
}

sealed interface ProductPriceEvent {
    val eventId: UUID
    val productId: UUID
    val effectiveAt: Instant
}

// Top-level (not nested) event classes: see the equivalent comment in the idempotency vignette's OrderEvent.kt for
// why the reflection-based cloud event type mapper requires this.
data class ProductDefined(override val eventId: UUID, override val productId: UUID, val price: BigDecimal, override val effectiveAt: Instant) : ProductPriceEvent

data class ProductPriceChanged(override val eventId: UUID, override val productId: UUID, val newPrice: BigDecimal, val changedAt: Instant) : ProductPriceEvent {
    override val effectiveAt: Instant get() = changedAt
}

data class ProductOrdered(override val eventId: UUID, override val productId: UUID, val orderId: UUID, val price: BigDecimal, val orderedAt: Instant) : ProductPriceEvent {
    override val effectiveAt: Instant get() = orderedAt
}

/** A price point that became effective at a point in time, folded from [ProductPriceEvent.ProductDefined]/[ProductPriceEvent.ProductPriceChanged]. */
data class PricePoint(val price: BigDecimal, val effectiveFrom: Instant)

data class ProductPriceState(val priceHistory: List<PricePoint> = emptyList())

private fun decide(command: ProductPriceCommand, state: ProductPriceState): List<ProductPriceEvent> = when (command) {
    is ProductPriceCommand.DefineProduct -> {
        require(state.priceHistory.isEmpty()) { "Product ${command.productId} is already defined" }
        listOf(ProductDefined(UUID.randomUUID(), command.productId, command.price, command.definedAt))
    }

    is ProductPriceCommand.ChangeProductPrice -> {
        require(state.priceHistory.isNotEmpty()) { "Product ${command.productId} is not defined" }
        listOf(ProductPriceChanged(UUID.randomUUID(), command.productId, command.newPrice, command.changedAt))
    }

    is ProductPriceCommand.PlacePriceOrder -> {
        require(state.priceHistory.isNotEmpty()) { "Product ${command.productId} is not defined" }
        val validPrices = validPricesAt(state.priceHistory, command.orderedAt)
        require(command.displayedPrice in validPrices) {
            "Displayed price ${command.displayedPrice} is no longer valid for product ${command.productId} at ${command.orderedAt} (valid: $validPrices)"
        }
        listOf(ProductOrdered(UUID.randomUUID(), command.productId, command.orderId, command.displayedPrice, command.orderedAt))
    }
}

/**
 * The current price at [at] is always valid. The immediately preceding price is also valid if the change to the
 * current price happened within [ProductPricePolicy.GRACE_PERIOD] before [at] - the grace period a shopper who saw
 * the old price gets to complete their order.
 */
private fun validPricesAt(priceHistory: List<PricePoint>, at: Instant): Set<BigDecimal> {
    val effective = priceHistory.filter { !it.effectiveFrom.isAfter(at) }.sortedBy { it.effectiveFrom }
    if (effective.isEmpty()) return emptySet()
    val current = effective.last()
    val previous = effective.dropLast(1).lastOrNull()
    return buildSet {
        add(current.price)
        if (previous != null && Duration.between(current.effectiveFrom, at) <= ProductPricePolicy.GRACE_PERIOD) {
            add(previous.price)
        }
    }
}

private fun evolve(state: ProductPriceState, event: ProductPriceEvent): ProductPriceState = when (event) {
    is ProductDefined -> state.copy(priceHistory = state.priceHistory + PricePoint(event.price, event.effectiveAt))
    is ProductPriceChanged -> state.copy(priceHistory = state.priceHistory + PricePoint(event.newPrice, event.effectiveAt))
    is ProductOrdered -> state
}
