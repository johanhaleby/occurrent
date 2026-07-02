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

package org.occurrent.example.domain.hotelbooking.features.booking.usecases

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.reactor.execute
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.GuestId
import org.occurrent.example.domain.hotelbooking.common.HotelId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCommand.BookRoom
import org.occurrent.example.domain.hotelbooking.features.booking.model.BookingCommand.CancelBooking
import org.occurrent.example.domain.hotelbooking.features.booking.model.Stay
import org.occurrent.example.domain.hotelbooking.features.booking.model.bookingDecider
import org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingDcbQueries.bookingBoundary
import java.time.Instant
import java.util.*

fun DcbApplicationService<DomainEvent>.bookRoom(hotelId: HotelId, roomId: RoomId, guestId: GuestId, stay: Stay, occurredAt: Instant = Instant.now()) = execute(
    bookingBoundary(roomId, guestId),
    BookRoom(UUID.randomUUID(), occurredAt, hotelId, roomId, guestId, stay),
    bookingDecider
)

fun DcbApplicationService<DomainEvent>.cancelBooking(roomId: RoomId, guestId: GuestId, stay: Stay, occurredAt: Instant = Instant.now()) = execute(
    bookingBoundary(roomId, guestId),
    CancelBooking(UUID.randomUUID(), occurredAt, roomId, guestId, stay),
    bookingDecider
)
