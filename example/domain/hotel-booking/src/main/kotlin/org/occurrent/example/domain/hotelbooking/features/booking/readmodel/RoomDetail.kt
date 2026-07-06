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

package org.occurrent.example.domain.hotelbooking.features.booking.readmodel

import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries
import org.occurrent.dsl.dcb.reactor.queryForList
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.GuestId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCancelled
import org.occurrent.example.domain.hotelbooking.features.booking.model.RoomBooked
import org.occurrent.example.domain.hotelbooking.features.booking.model.Stay
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestRegistered
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomClosed
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomDefined
import org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

data class BookedStay(val guestId: GuestId, val guestName: String, val stay: Stay)

private data class RoomAccumulator(val roomNumber: String? = null, val closed: Boolean = false, val activeStays: List<Pair<GuestId, Stay>> = emptyList())

data class RoomDetailView(val roomId: RoomId, val roomNumber: String, val bookings: List<BookedStay>) {
    @Suppress("unused")
    val bookingCount: Int get() = bookings.size // Used by thymeleaf
}

/**
 * Builds the room-detail read model on demand by querying the event store, so it is strongly consistent with the last
 * write rather than eventually consistent like the dashboard. This is the [DcbDomainEventQueries] selling point.
 *
 * On the reactive stack the whole view is assembled in a reactive pipeline: the room's events are collected into a list,
 * folded into an accumulator, and each active booking's guest name is resolved with its own consistent read. The method
 * returns a [Mono] so the WebFlux controller can compose it without blocking.
 */
@Component
class RoomDetail(private val queries: DcbDomainEventQueries<DomainEvent>) {

    fun of(roomId: RoomId): Mono<RoomDetailView> {
        // The room tag scopes the read to this room's own events (definition plus bookings), not the guests'.
        return queries.queryForList(HotelBookingCriteria.roomCriteria(roomId))
            .map { events ->
                events.fold(RoomAccumulator()) { acc, event ->
                    when (event) {
                        is RoomDefined -> acc.copy(roomNumber = event.roomNumber)
                        is RoomClosed -> acc.copy(closed = true)
                        is RoomBooked -> acc.copy(activeStays = acc.activeStays + (event.guestId to event.stay))
                        is BookingCancelled -> acc.copy(activeStays = acc.activeStays.removeFirst(event.guestId to event.stay))
                        else -> acc
                    }
                }
            }
            // A closed room, or one that was never defined, is not shown. Mono.empty() maps to "no view".
            .flatMap { acc ->
                val roomNumber = acc.roomNumber
                if (acc.closed || roomNumber == null) {
                    Mono.empty()
                } else {
                    // Resolve names with a consistent read per booking, since names live on the guest boundary.
                    Mono.just(acc.activeStays).flatMap { stays ->
                        if (stays.isEmpty()) Mono.just(RoomDetailView(roomId, roomNumber, emptyList()))
                        else Mono.zip(stays.map { (guestId, stay) -> nameOf(guestId).map { BookedStay(guestId, it, stay) } }) { resolved ->
                            RoomDetailView(roomId, roomNumber, resolved.map { it as BookedStay })
                        }
                    }
                }
            }
    }

    private fun nameOf(guestId: GuestId): Mono<String> =
        queries.queryForList(HotelBookingCriteria.guestCriteria(guestId))
            .map { events -> events.filterIsInstance<GuestRegistered>().map { it.name }.firstOrNull() ?: guestId.toString() }
}

private fun <T> List<T>.removeFirst(element: T): List<T> {
    val index = indexOf(element)
    return if (index < 0) this else toMutableList().apply { removeAt(index) }
}
