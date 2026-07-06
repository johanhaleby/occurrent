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

package org.occurrent.example.domain.hotelbooking.features.roommanagement.model

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.hotelbooking.common.DomainCommand
import org.occurrent.example.domain.hotelbooking.common.HotelId
import org.occurrent.example.domain.hotelbooking.common.RoomId
import java.time.Instant
import java.util.*

/**
 * Decider for the room's own lifecycle. Single boundary: the room (see
 * [org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria.roomCriteria]).
 */
val roomDecider: Decider<RoomCommand, RoomState, RoomEvent> = decider(
    initialState = RoomState.NotDefined, decide = ::decide, evolve = ::evolve
)

sealed interface RoomCommand : DomainCommand {
    data class DefineRoom(val eventId: UUID, val occurredAt: Instant, val hotelId: HotelId, val roomId: RoomId, val roomNumber: String) : RoomCommand
    data class CloseRoom(val eventId: UUID, val occurredAt: Instant, val hotelId: HotelId, val roomId: RoomId) : RoomCommand
}

sealed interface RoomState {
    data object NotDefined : RoomState
    data class Defined(val hotelId: HotelId, val roomId: RoomId, val roomNumber: String, val definedAt: Instant) : RoomState
    data object Closed : RoomState
}

private fun decide(command: RoomCommand, state: RoomState): List<RoomEvent> = when (command) {
    is RoomCommand.DefineRoom -> when (state) {
        RoomState.NotDefined -> listOf(RoomDefined(UUID.randomUUID(), command.occurredAt, command.hotelId, command.roomId, command.roomNumber))
        is RoomState.Defined -> throw IllegalArgumentException("Room ${command.roomNumber} is already defined")
        RoomState.Closed -> throw IllegalArgumentException("Room ${command.roomId} was closed and cannot be redefined")
    }

    is RoomCommand.CloseRoom -> when (state) {
        is RoomState.Defined -> listOf(RoomClosed(UUID.randomUUID(), command.occurredAt, command.hotelId, command.roomId))
        RoomState.NotDefined -> throw IllegalArgumentException("Room ${command.roomId} is not defined")
        RoomState.Closed -> throw IllegalArgumentException("Room ${command.roomId} is already closed")
    }
}

private fun evolve(state: RoomState, event: RoomEvent): RoomState = when (event) {
    is RoomDefined -> RoomState.Defined(event.hotelId, event.roomId, event.roomNumber, event.occurredAt)
    is RoomClosed -> RoomState.Closed
}
