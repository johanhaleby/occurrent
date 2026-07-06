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

package org.occurrent.example.domain.hotelbooking.features.booking.model

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.hotelbooking.common.*
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestDeregistered
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestRegistered
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomClosed
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomDefined
import java.time.Instant
import java.util.*

/**
 * The cross-boundary decider, and the point of the example. Its boundary spans a room AND a guest at once (see
 * [org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria.bookingCriteria]), so one
 * conditional append holds both the no-double-booking invariant (on the room) and the per-guest booking-limit invariant
 * (on the guest).
 *
 * Note how this differs from [org.occurrent.example.domain.hotelbooking.features.roommanagement.model.roomDecider] and
 * [org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.guestDecider]: it does not emit
 * [RoomDefined] or [GuestRegistered], it only reads them (to learn the entities exist and are still open). Most deciders
 * are single-boundary like those two, this is the one that genuinely needs DCB.
 */
val bookingDecider: Decider<BookingCommand, BookingState, DomainEvent> =
    decider(
        initialState = BookingState(),
        decide = ::decide,
        evolve = ::evolve
    )

/** Domain policy constants. */
object BookingPolicy {
    /** The maximum number of active (booked and not cancelled) bookings a single guest may hold at once. */
    const val MAX_ACTIVE_BOOKINGS_PER_GUEST: Int = 5
}

sealed interface BookingCommand : DomainCommand {
    val roomId: RoomId
    val guestId: GuestId

    data class BookRoom(val eventId: UUID, val occurredAt: Instant, val hotelId: HotelId, override val roomId: RoomId, override val guestId: GuestId, val stay: Stay) : BookingCommand
    data class CancelBooking(val eventId: UUID, val occurredAt: Instant, override val roomId: RoomId, override val guestId: GuestId, val stay: Stay) : BookingCommand
}

/**
 * State folded from the booking boundary.
 *
 * The boundary returns events for the target room (RoomDefined plus bookings of it) AND the target guest
 * (GuestRegistered plus that guest's bookings of any room). [evolve] does not receive the command, so it cannot know
 * which room or guest is being decided on. The simplest sound approach is therefore to key the state by id and let
 * [decide] look up the entry for the command's room and guest. Because the query is scoped to one room and one guest,
 * these maps only ever hold those two.
 */
data class BookingState(
    val definedRooms: Set<RoomId> = emptySet(),
    val closedRooms: Set<RoomId> = emptySet(),
    val registeredGuests: Set<GuestId> = emptySet(),
    val deregisteredGuests: Set<GuestId> = emptySet(),
    // Active (non-cancelled) stays currently held on each room, used for the no-double-booking check.
    val activeStaysByRoom: Map<RoomId, List<Stay>> = emptyMap(),
    // How many active bookings each guest holds, used for the per-guest limit.
    val activeBookingsByGuest: Map<GuestId, Int> = emptyMap()
)

private fun decide(command: BookingCommand, state: BookingState): List<DomainEvent> {
    val roomId = command.roomId
    val guestId = command.guestId

    require(state.isRoomDefined(roomId)) { "Room $roomId is not defined" }
    require(state.isGuestRegistered(guestId)) { "Guest $guestId is not registered" }

    return when (command) {
        is BookingCommand.BookRoom -> {
            require(!state.isRoomClosed(roomId)) { "Room $roomId is closed" }
            require(!state.isGuestDeregistered(guestId)) { "Guest $guestId is deregistered" }

            require(!state.roomIsDoubleBooked(roomId, command.stay)) {
                "Room $roomId is already booked for an overlapping stay"
            }

            require(!state.isGuestAtBookingLimit(guestId)) {
                "Guest $guestId already holds ${BookingPolicy.MAX_ACTIVE_BOOKINGS_PER_GUEST} active bookings"
            }

            listOf(RoomBooked(UUID.randomUUID(), command.occurredAt, command.hotelId, roomId, guestId, command.stay))
        }

        is BookingCommand.CancelBooking -> {
            require(state.hasActiveBooking(roomId, guestId, command.stay)) {
                "Guest $guestId has no active booking of room $roomId for that stay"
            }

            listOf(BookingCancelled(UUID.randomUUID(), command.occurredAt, roomId, guestId, command.stay))
        }
    }
}

private fun evolve(state: BookingState, event: DomainEvent): BookingState = when (event) {
    is RoomDefined -> state.copy(definedRooms = state.definedRooms + event.roomId)
    is GuestRegistered -> state.copy(registeredGuests = state.registeredGuests + event.guestId)

    is RoomBooked -> state.copy(
        activeStaysByRoom = state.activeStaysByRoom + (event.roomId to ((state.activeStaysByRoom[event.roomId] ?: emptyList()) + event.stay)),
        activeBookingsByGuest = state.activeBookingsByGuest + (event.guestId to ((state.activeBookingsByGuest[event.guestId] ?: 0) + 1))
    )

    is BookingCancelled -> state.copy(
        activeStaysByRoom = state.activeStaysByRoom + (event.roomId to ((state.activeStaysByRoom[event.roomId] ?: emptyList()).minusFirst(event.stay))),
        activeBookingsByGuest = state.activeBookingsByGuest + (event.guestId to maxOf(0, (state.activeBookingsByGuest[event.guestId] ?: 0) - 1))
    )

    // The booking boundary also sees the room and guest lifecycle events, so a closed room or a deregistered guest can
    // no longer be booked.
    is RoomClosed -> state.copy(closedRooms = state.closedRooms + event.roomId)
    is GuestDeregistered -> state.copy(deregisteredGuests = state.deregisteredGuests + event.guestId)

    else -> throw IllegalArgumentException("Unexpected event type ${event::class.simpleName} in booking boundary")
}


// Helpers
private fun <T> List<T>.minusFirst(element: T): List<T> {
    val index = indexOf(element)
    return if (index < 0) this else toMutableList().apply { removeAt(index) }
}

private fun BookingState.isRoomDefined(roomId: RoomId): Boolean = definedRooms.contains(roomId)

private fun BookingState.isRoomClosed(roomId: RoomId): Boolean = closedRooms.contains(roomId)

private fun BookingState.isGuestRegistered(guestId: GuestId): Boolean = registeredGuests.contains(guestId)

private fun BookingState.isGuestDeregistered(guestId: GuestId): Boolean = deregisteredGuests.contains(guestId)

private fun BookingState.roomIsDoubleBooked(roomId: RoomId, stay: Stay): Boolean =
    (activeStaysByRoom[roomId] ?: emptyList()).any { it.overlaps(stay) }

private fun BookingState.isGuestAtBookingLimit(guestId: GuestId): Boolean =
    (activeBookingsByGuest[guestId] ?: 0) >= BookingPolicy.MAX_ACTIVE_BOOKINGS_PER_GUEST

private fun BookingState.hasActiveBooking(roomId: RoomId, guestId: GuestId, stay: Stay): Boolean {
    // A matching active booking exists if the room still holds this exact stay. The guest scope is enforced by the
    // boundary query, so if the stay is active on the room and the guest is in scope it belongs to that guest.
    return (activeStaysByRoom[roomId] ?: emptyList()).contains(stay) && (activeBookingsByGuest[guestId] ?: 0) > 0
}
