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
 * Every event is tagged with the username it mentions and with the account it belongs to (see [tags]).
 * [UsernameChanged] carries both the old and the new name and is tagged with both, so a rename shows up whichever of
 * the two names you query.
 * <p>
 * Closing an account and changing its username read only the username tags (see [criteria]). Every event that changes
 * which account holds a username is tagged with that username, so those events alone tell who holds it, and only that
 * account may close it or rename it. Adding the account tag to these reads would change no decision.
 * <p>
 * Registering reads the username tag and the account tag as two alternatives. The username tag tells whether the name
 * is free, and the account tag tells whether this account has registered before. An account registers once, because
 * an account that registered twice would hold two usernames and closing it would release only one of them. The append
 * condition covers both tags, so registering one account under two usernames at the same time conflicts the same way
 * two accounts registering one username does.
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
private fun accountTag(accountId: UUID): Tag = Tag.of("account", accountId.toString())

private fun criteria(command: UsernameCommand): DcbCriteria = when (command) {
    is UsernameCommand.RegisterAccount -> DcbCriteria.tagsAnyOf(usernameTag(command.username), accountTag(command.accountId))
    is UsernameCommand.CloseAccount -> DcbCriteria.tags(usernameTag(command.username))
    is UsernameCommand.ChangeUsername -> DcbCriteria.tagsAnyOf(usernameTag(command.oldUsername), usernameTag(command.newUsername))
}

private fun tags(event: UsernameEvent): Set<Tag> = when (event) {
    is AccountRegistered -> setOf(usernameTag(event.username), accountTag(event.accountId))
    is AccountClosed -> setOf(usernameTag(event.username), accountTag(event.accountId))
    is UsernameChanged -> setOf(usernameTag(event.oldUsername), usernameTag(event.newUsername), accountTag(event.accountId))
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
 * The shape is maps and a set (like [org.occurrent.example.domain.courseenrollment.features.enrollment.model.EnrollmentState])
 * because [evolve] doesn't know which username or account [decide] is asking about. Only the entries for the command's
 * own usernames and account are complete. A registration also reads the account's events about usernames it held
 * before, without other accounts' events about those names, and [decide] never looks at those entries.
 */
data class UsernameState(
    val holders: Map<String, UUID> = emptyMap(),
    val closedAt: Map<String, Instant> = emptyMap(),
    val registeredAccounts: Set<UUID> = emptySet()
)

private fun decide(command: UsernameCommand, state: UsernameState): List<UsernameEvent> = when (command) {
    is UsernameCommand.RegisterAccount -> {
        require(command.accountId !in state.registeredAccounts) { "Account ${command.accountId} is already registered" }
        requireAvailable(state, command.username, command.now)
        listOf(AccountRegistered(UUID.randomUUID(), command.now, command.accountId, command.username))
    }

    is UsernameCommand.CloseAccount -> {
        requireHeldBy(state, command.username, command.accountId)
        listOf(AccountClosed(UUID.randomUUID(), command.closedAt, command.accountId, command.username, command.closedAt))
    }

    is UsernameCommand.ChangeUsername -> {
        requireHeldBy(state, command.oldUsername, command.accountId)
        requireAvailable(state, command.newUsername, command.now)
        listOf(UsernameChanged(UUID.randomUUID(), command.now, command.accountId, command.oldUsername, command.newUsername))
    }
}

private fun requireHeldBy(state: UsernameState, username: String, accountId: UUID) {
    val holder = requireNotNull(state.holders[username]) { "Username $username is not registered" }
    require(holder == accountId) { "Username $username is held by another account" }
}

private fun requireAvailable(state: UsernameState, username: String, now: Instant) {
    require(username !in state.holders) { "Username $username is already taken" }
    val closedAt = state.closedAt[username] ?: return
    val availableFrom = closedAt.plus(UsernamePolicy.RETENTION)
    require(!now.isBefore(availableFrom)) { "Username $username is reserved until $availableFrom (closed at $closedAt)" }
}

private fun evolve(state: UsernameState, event: UsernameEvent): UsernameState = when (event) {
    is AccountRegistered -> state.copy(
        holders = state.holders + (event.username to event.accountId),
        closedAt = state.closedAt - event.username,
        registeredAccounts = state.registeredAccounts + event.accountId
    )

    is AccountClosed -> state.copy(
        holders = state.holders - event.username,
        closedAt = state.closedAt + (event.username to event.closedAt)
    )

    is UsernameChanged -> state.copy(
        holders = state.holders - event.oldUsername + (event.newUsername to event.accountId),
        closedAt = state.closedAt - event.oldUsername
    )
}
