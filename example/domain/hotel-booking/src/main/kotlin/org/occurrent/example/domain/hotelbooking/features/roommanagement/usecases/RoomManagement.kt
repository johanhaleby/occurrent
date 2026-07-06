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

package org.occurrent.example.domain.hotelbooking.features.roommanagement.usecases

import org.occurrent.application.service.reactor.dcb.DcbApplicationService
import org.occurrent.dsl.dcb.reactor.execute
import org.occurrent.example.domain.hotelbooking.common.DomainEvent
import org.occurrent.example.domain.hotelbooking.common.HotelId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomCommand.CloseRoom
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.RoomCommand.DefineRoom
import org.occurrent.example.domain.hotelbooking.features.roommanagement.model.roomDecider
import org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria.roomCriteria
import java.time.Instant
import java.util.*

fun DcbApplicationService<DomainEvent>.defineRoom(hotelId: HotelId, roomId: RoomId, roomNumber: String, occurredAt: Instant = Instant.now()) = execute(
    roomCriteria(roomId),
    DefineRoom(UUID.randomUUID(), occurredAt, hotelId, roomId, roomNumber),
    roomDecider
)

fun DcbApplicationService<DomainEvent>.closeRoom(hotelId: HotelId, roomId: RoomId, occurredAt: Instant = Instant.now()) = execute(
    roomCriteria(roomId),
    CloseRoom(UUID.randomUUID(), occurredAt, hotelId, roomId),
    roomDecider
)
