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

package org.occurrent.example.domain.hotelbooking.features.guestmanagement.model

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.hotelbooking.common.DomainCommand
import org.occurrent.example.domain.hotelbooking.common.GuestId
import java.time.Instant
import java.util.*

/**
 * Decider for the guest's own lifecycle. Single boundary: the guest (see
 * [org.occurrent.example.domain.hotelbooking.infrastructure.dcb.HotelBookingCriteria.guestCriteria]).
 */
val guestDecider: Decider<GuestCommand, GuestRegistry, GuestEvent> =
    decider(
        initialState = GuestRegistry(),
        decide = ::decide,
        evolve = ::evolve
    )

sealed interface GuestCommand : DomainCommand {
    data class RegisterGuest(val eventId: UUID, val occurredAt: Instant, val guestId: GuestId, val name: String) : GuestCommand
    data class DeregisterGuest(val eventId: UUID, val occurredAt: Instant, val guestId: GuestId) : GuestCommand
}

data class GuestRegistry(val guests: Map<GuestId, Guest> = emptyMap())

private fun decide(command: GuestCommand, state: GuestRegistry): List<GuestEvent> =
    when (command) {
        is GuestCommand.RegisterGuest -> {
            val guestId = command.guestId
            require(!state.isGuestRegistered(guestId)) { "Guest $guestId is already registered" }

            listOf(GuestRegistered(command.eventId, command.occurredAt, guestId, command.name))
        }

        is GuestCommand.DeregisterGuest -> {
            val guestId = command.guestId
            require(state.isGuestRegistered(guestId)) { "Guest $guestId is not registered" }

            listOf(GuestDeregistered(command.eventId, command.occurredAt, guestId))
        }
    }

private fun evolve(state: GuestRegistry, event: GuestEvent): GuestRegistry = when (event) {
    is GuestRegistered -> state.copy(guests = state.guests + (event.guestId to Guest(event.guestId, event.name, event.occurredAt)))
    is GuestDeregistered -> state.copy(guests = state.guests - event.guestId)
}

// Helpers
data class Guest(val guestId: GuestId, val name: String, val registeredAt: Instant)

private fun GuestRegistry.isGuestRegistered(guestId: GuestId): Boolean = guests.containsKey(guestId)
