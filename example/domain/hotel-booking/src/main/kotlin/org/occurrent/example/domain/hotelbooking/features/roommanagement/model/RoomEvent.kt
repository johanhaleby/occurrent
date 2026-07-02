package org.occurrent.example.domain.hotelbooking.features.roommanagement.model

import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.HotelId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import java.time.Instant
import java.util.*

sealed interface RoomEvent : DomainEvent

/** A room exists and may be booked. Belongs to the room boundary. */
data class RoomDefined(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val hotelId: HotelId,
    val roomId: RoomId,
    val roomNumber: String
) : RoomEvent

/** A room is taken out of service and can no longer be booked. Belongs to the room boundary. */
data class RoomClosed(
    override val eventId: UUID,
    override val occurredAt: Instant,
    val hotelId: HotelId,
    val roomId: RoomId
) : RoomEvent
