package org.occurrent.example.domain.hotelbooking.features.booking.model

import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.GuestId
import org.occurrent.example.domain.hotelbooking.common.HotelId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import java.time.Instant
import java.util.*

/**
 * The events of the hotel-booking domain.
 *
 * The point of the example is the booking decision, which must hold two invariants that live on two different entities
 * at the same time: the room must not be double-booked and the guest must stay under a per-guest booking limit. That is
 * exactly what a Dynamic Consistency Boundary lets you do without a saga.
 */
sealed interface BookingEvent : DomainEvent

/** A guest reserved a room for a stay. Belongs to BOTH the room and the guest boundary (the cross-entity move). */
data class RoomBooked(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val hotelId: HotelId,
    val roomId: RoomId,
    val guestId: GuestId,
    val stay: Stay
) : BookingEvent

/** A guest gave up a reservation. Belongs to BOTH the room and the guest boundary. */
data class BookingCancelled(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val roomId: RoomId,
    val guestId: GuestId,
    val stay: Stay
) : BookingEvent
