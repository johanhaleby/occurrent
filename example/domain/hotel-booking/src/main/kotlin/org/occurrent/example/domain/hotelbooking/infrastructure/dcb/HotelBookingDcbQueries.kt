package org.occurrent.example.domain.hotelbooking.infrastructure.dcb

import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.example.domain.hotelbooking.common.GuestId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestTags
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomTags

/**
 * The DCB queries that define the decision boundary for each command. A query is both the read filter (what the decider
 * folds its state from) and the consistency boundary (what a conditional append is checked against).
 */
internal object HotelBookingDcbQueries {

    /**
     * The boundary for booking or cancelling a room for a guest. It must span TWO entities at once:
     *  - the room's events, to know it is defined and open and which stays are already booked, and
     *  - the guest's events, to know the guest exists and is under the per-guest booking limit.
     */
    fun bookingBoundary(roomId: RoomId, guestId: GuestId): DcbCriteria =
        DcbCriteria.tagsAnyOf(RoomTags.room(roomId), GuestTags.guest(guestId))

    /** The boundary for defining a room (the room's own events). */
    fun roomBoundary(roomId: RoomId): DcbCriteria =
        DcbCriteria.tags(RoomTags.room(roomId))

    /** The boundary for registering a guest (the guest's own events). */
    fun guestBoundary(guestId: GuestId): DcbCriteria =
        DcbCriteria.tags(GuestTags.guest(guestId))
}
