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

import org.occurrent.annotation.DcbSubscription
import org.occurrent.annotation.ResumeBehavior
import org.occurrent.annotation.StartPosition
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCancelled
import org.occurrent.example.domain.hotelbooking.features.booking.model.RoomBooked
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestDeregistered
import org.occurrent.example.domain.hotelbooking.features.guestmanagement.model.GuestRegistered
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomClosed
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomDefined
import org.springframework.stereotype.Component

/**
 * Feeds the [HotelDashboard] read model from a DCB subscription declared with [DcbSubscription].
 *
 * The read model is in-memory only, so it must be rebuilt from the whole DCB history on every boot. That is why this
 * combines [StartPosition.BEGINNING] with [ResumeBehavior.SAME_AS_START_AT]: BEGINNING alone would replay only the
 * first time and then resume from the stored position on later restarts, which would leave the in-memory model missing
 * all history before that position. SAME_AS_START_AT replays from the beginning on every boot (and keeps no checkpoint).
 * The event types are narrowed on the annotation, so the subscription receives only the dashboard's events server-side.
 *
 * The reactive annotation post-processor accepts a handler that returns `Mono<Void>` OR a plain `void`/Unit method (a
 * non-Mono return is treated as an already-completed action), so the simple synchronous in-memory update below is fine.
 */
@Component
class HotelDashboardSubscriber(private val hotelDashboard: HotelDashboard) {

    @DcbSubscription(
        id = "hotel-dashboard",
        eventTypes = [RoomDefined::class, RoomClosed::class, GuestRegistered::class, GuestDeregistered::class, RoomBooked::class, BookingCancelled::class],
        startAt = StartPosition.BEGINNING,
        resumeBehavior = ResumeBehavior.SAME_AS_START_AT
    )
    fun update(event: DomainEvent) {
        hotelDashboard.update(event)
    }
}
