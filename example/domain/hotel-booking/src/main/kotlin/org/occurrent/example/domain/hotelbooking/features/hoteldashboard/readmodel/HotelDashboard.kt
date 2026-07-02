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

package org.occurrent.example.domain.hotelbooking.features.hoteldashboard.readmodel

import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.GuestId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCancelled
import org.occurrent.example.domain.hotelbooking.features.booking.model.RoomBooked
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestDeregistered
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestRegistered
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomClosed
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomDefined
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicReference

/** A room as shown on the dashboard. The active-booking count folds book/cancel events, so replay stays idempotent. */
data class RoomRow(val roomId: RoomId, val roomNumber: String, val activeBookings: Int) {
    @Suppress("unused")
    val activeBookingCount: Int get() = activeBookings // Used by thymeleaf
}

data class DashboardState(val rooms: Map<RoomId, RoomRow>, val guests: Map<GuestId, String>) {
    companion object {
        val EMPTY = DashboardState(emptyMap(), emptyMap())
    }
}

/**
 * An in-memory read model of all rooms and guests, kept current by a DCB subscription (see [HotelDashboardSubscriber]).
 * It is eventually consistent with the event store. For a strongly consistent read see the room-detail read model in the
 * booking feature.
 */
@Component
class HotelDashboard {

    private val slot = AtomicReference(DashboardState.EMPTY)

    fun update(event: DomainEvent) {
        slot.updateAndGet { state -> evolve(state, event) }
    }

    fun rooms(): List<RoomRow> = slot.get().rooms.values.sortedBy { it.roomNumber }

    fun guests(): List<RegisteredGuest> =
        slot.get().guests.entries.map { RegisteredGuest(it.key, it.value) }.sortedBy { it.name }

    fun guestName(guestId: GuestId): String? = slot.get().guests[guestId]

    private fun evolve(state: DashboardState, event: DomainEvent): DashboardState = when (event) {
        is RoomDefined -> {
            val existing = state.rooms[event.roomId]
            val row = RoomRow(event.roomId, event.roomNumber, existing?.activeBookings ?: 0)
            state.copy(rooms = state.rooms + (event.roomId to row))
        }

        is GuestRegistered -> state.copy(guests = state.guests + (event.guestId to event.name))

        is RoomClosed -> state.copy(rooms = state.rooms - event.roomId)

        is GuestDeregistered -> state.copy(guests = state.guests - event.guestId)

        is RoomBooked -> {
            val existing = state.rooms[event.roomId] ?: return state
            state.copy(rooms = state.rooms + (event.roomId to existing.copy(activeBookings = existing.activeBookings + 1)))
        }

        is BookingCancelled -> {
            val existing = state.rooms[event.roomId] ?: return state
            state.copy(rooms = state.rooms + (event.roomId to existing.copy(activeBookings = maxOf(0, existing.activeBookings - 1))))
        }

        else -> state
    }
}

data class RegisteredGuest(val guestId: GuestId, val name: String)
