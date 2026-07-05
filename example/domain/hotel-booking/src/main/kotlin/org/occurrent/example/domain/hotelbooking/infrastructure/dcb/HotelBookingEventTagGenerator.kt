package org.occurrent.example.domain.hotelbooking.infrastructure.dcb

import org.occurrent.application.service.dcb.TagGenerator
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCancelled
import org.occurrent.example.domain.hotelbooking.features.booking.model.RoomBooked
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestDeregistered
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestRegistered
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestTags
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomClosed
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomDefined
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomTags

/**
 * Assigns DCB tags to each event when it is appended. The tags decide which boundaries an event belongs to, and
 * therefore which events a later [HotelBookingDcbQueries] query will see.
 *
 * Tagging the book/cancel events with BOTH the room and the guest is the heart of the example: it is what lets a single
 * conditional append protect the no-double-booking invariant AND the per-guest booking limit in one atomic decision.
 */
internal class HotelBookingEventTagGenerator : TagGenerator<DomainEvent> {
    override fun tags(event: DomainEvent): Set<Tag> = when (event) {
        is RoomDefined -> setOf(RoomTags.room(event.roomId))
        is RoomClosed -> setOf(RoomTags.room(event.roomId))
        is GuestRegistered -> setOf(GuestTags.guest(event.guestId))
        is GuestDeregistered -> setOf(GuestTags.guest(event.guestId))
        is RoomBooked -> setOf(RoomTags.room(event.roomId), GuestTags.guest(event.guestId))
        is BookingCancelled -> setOf(RoomTags.room(event.roomId), GuestTags.guest(event.guestId))
        else -> error("No DCB tags defined for event ${event::class.simpleName}. Every event must be tagged so the right decision boundary can find it.")
    }
}
