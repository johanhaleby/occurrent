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

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import java.math.BigDecimal
import java.time.Instant
import java.util.*

/**
 * Pattern: prevent record duplication with an idempotency token. A client that retries a request (say, after a
 * timeout on the first response) supplies the same [idempotencyToken][OrderEvent.OrderPlaced.idempotencyToken] on
 * every attempt. The boundary is scoped to that token alone, so the decider can see with a single DCB read whether an
 * order has already been placed for it and simply not place a second one: [decide] returns an empty list rather than
 * throwing, so a retried command is a silent no-op instead of an error.
 */
val orderDcbDecider: DcbDecider<PlaceOrder, OrderState, OrderEvent> = dcbDecider(
    initialState = OrderState(),
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

private fun criteria(command: PlaceOrder): DcbCriteria = DcbCriteria.tags(Tag.of("idempotency", command.idempotencyToken))

private fun tags(event: OrderEvent): Set<Tag> = when (event) {
    is OrderPlaced -> setOf(Tag.of("order", event.orderId.toString()), Tag.of("idempotency", event.idempotencyToken))
}

data class PlaceOrder(val orderId: UUID, val idempotencyToken: String, val amount: BigDecimal, val placedAt: Instant)

sealed interface OrderEvent {
    val eventId: UUID
}

// A top-level (not nested) event class: the reflection-based cloud event type mapper resolves a cloud event type back
// to a domain event class by prepending the package name to the simple class name, which only works for top-level
// classes (a nested class's binary name includes its enclosing class, e.g. "OrderEvent$OrderPlaced").
data class OrderPlaced(override val eventId: UUID, val occurredAt: Instant, val orderId: UUID, val idempotencyToken: String, val amount: BigDecimal) : OrderEvent

/** Scoped to a single idempotency token, so this is either empty (never seen) or holds exactly one order. */
data class OrderState(val alreadyPlaced: Boolean = false)

private fun decide(command: PlaceOrder, state: OrderState): List<OrderEvent> =
    if (state.alreadyPlaced) {
        emptyList()
    } else {
        listOf(OrderPlaced(UUID.randomUUID(), command.placedAt, command.orderId, command.idempotencyToken, command.amount))
    }

private fun evolve(state: OrderState, event: OrderEvent): OrderState = when (event) {
    is OrderPlaced -> state.copy(alreadyPlaced = true)
}
