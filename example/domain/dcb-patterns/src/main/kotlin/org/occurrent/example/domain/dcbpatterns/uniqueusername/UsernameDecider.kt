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

package org.occurrent.example.domain.dcbpatterns.uniqueusername

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * Pattern: global uniqueness with a retention period. A username can only be held by one account at a time, and once
 * released it stays reserved for [UsernamePolicy.RETENTION] after the account closes, so nobody can immediately grab a
 * name someone else just gave up.
 * <p>
 * The DCB boundary is a single tag value, the username itself: [criteria] reads only the events that ever mentioned
 * that exact username, which is enough to know whether it is free. [UsernameChanged] carries both the old and the new
 * name and is tagged with both (see [tags]), so a rename shows up whichever of the two names you query.
 * <p>
 * Time-in-payload, now-in-command: [AccountClosed.closedAt] and [RegisterAccount.now] are both plain [Instant]
 * fields on the domain event/command, never read from CloudEvent metadata. The decider's [evolve]/[decide] only ever
 * see domain payloads, so the same decision is reproducible from the events alone, independent of when they happen to
 * be replayed.
 */
val usernameDcbDecider: DcbDecider<UsernameCommand, UsernameState, UsernameEvent> = dcbDecider(
    initialState = UsernameState(),
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

object UsernamePolicy {
    /** How long a username stays reserved after the account holding it closes. */
    val RETENTION: Duration = Duration.ofDays(30)
}

private fun usernameTag(username: String): Tag = Tag.of("username", username)

private fun criteria(command: UsernameCommand): DcbCriteria = when (command) {
    is UsernameCommand.RegisterAccount -> DcbCriteria.tags(usernameTag(command.username))
    is UsernameCommand.CloseAccount -> DcbCriteria.tags(usernameTag(command.username))
    is UsernameCommand.ChangeUsername -> DcbCriteria.tagsAnyOf(usernameTag(command.oldUsername), usernameTag(command.newUsername))
}

private fun tags(event: UsernameEvent): Set<Tag> = when (event) {
    is AccountRegistered -> setOf(usernameTag(event.username))
    is AccountClosed -> setOf(usernameTag(event.username))
    is UsernameChanged -> setOf(usernameTag(event.oldUsername), usernameTag(event.newUsername))
}

sealed interface UsernameCommand {
    data class RegisterAccount(val accountId: UUID, val username: String, val now: Instant) : UsernameCommand
    data class CloseAccount(val accountId: UUID, val username: String, val closedAt: Instant) : UsernameCommand
    data class ChangeUsername(val accountId: UUID, val oldUsername: String, val newUsername: String, val now: Instant) : UsernameCommand
}

sealed interface UsernameEvent {
    val eventId: UUID
    val occurredAt: Instant
}

data class AccountRegistered(override val eventId: UUID, override val occurredAt: Instant, val accountId: UUID, val username: String) : UsernameEvent
data class AccountClosed(override val eventId: UUID, override val occurredAt: Instant, val accountId: UUID, val username: String, val closedAt: Instant) : UsernameEvent
data class UsernameChanged(override val eventId: UUID, override val occurredAt: Instant, val accountId: UUID, val oldUsername: String, val newUsername: String) : UsernameEvent

/**
 * Because [criteria] scopes the read to one username's tag, the sets below only ever contain that single value. The
 * shape is a map/set anyway (like [org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentState])
 * because [evolve] doesn't know which username [decide] is asking about.
 */
data class UsernameState(
    val activeUsernames: Set<String> = emptySet(),
    val closedAt: Map<String, Instant> = emptyMap()
)

private fun decide(command: UsernameCommand, state: UsernameState): List<UsernameEvent> = when (command) {
    is UsernameCommand.RegisterAccount -> {
        requireAvailable(state, command.username, command.now)
        listOf(AccountRegistered(UUID.randomUUID(), command.now, command.accountId, command.username))
    }

    is UsernameCommand.CloseAccount -> {
        require(command.username in state.activeUsernames) { "Username ${command.username} is not registered" }
        listOf(AccountClosed(UUID.randomUUID(), command.closedAt, command.accountId, command.username, command.closedAt))
    }

    is UsernameCommand.ChangeUsername -> {
        require(command.oldUsername in state.activeUsernames) { "Username ${command.oldUsername} is not registered" }
        requireAvailable(state, command.newUsername, command.now)
        listOf(UsernameChanged(UUID.randomUUID(), command.now, command.accountId, command.oldUsername, command.newUsername))
    }
}

private fun requireAvailable(state: UsernameState, username: String, now: Instant) {
    require(username !in state.activeUsernames) { "Username $username is already taken" }
    val closedAt = state.closedAt[username] ?: return
    val availableFrom = closedAt.plus(UsernamePolicy.RETENTION)
    require(!now.isBefore(availableFrom)) { "Username $username is reserved until $availableFrom (closed at $closedAt)" }
}

private fun evolve(state: UsernameState, event: UsernameEvent): UsernameState = when (event) {
    is AccountRegistered -> state.copy(
        activeUsernames = state.activeUsernames + event.username,
        closedAt = state.closedAt - event.username
    )

    is AccountClosed -> state.copy(
        activeUsernames = state.activeUsernames - event.username,
        closedAt = state.closedAt + (event.username to event.closedAt)
    )

    is UsernameChanged -> state.copy(
        activeUsernames = state.activeUsernames - event.oldUsername + event.newUsername,
        closedAt = state.closedAt - event.oldUsername
    )
}
