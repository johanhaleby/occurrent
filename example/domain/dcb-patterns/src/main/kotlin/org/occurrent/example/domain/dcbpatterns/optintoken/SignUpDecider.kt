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

package org.occurrent.example.domain.dcbpatterns.optintoken

import org.occurrent.dsl.dcb.DcbDecider
import org.occurrent.dsl.dcb.dcbDecider
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.Tag
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Pattern: double opt-in with a consume-once, expiring token. [SignUpInitiated] carries the one-time password and
 * when it was issued; [ConfirmSignUp] must arrive with the matching OTP before [SignUpTokenPolicy.TTL] elapses, and
 * only once - a second confirmation with the same OTP is rejected because the boundary already shows the token
 * consumed.
 * <p>
 * The boundary is the pair of tags (email, otp) together (see [criteria]): both tags on [SignUpInitiated]/
 * [SignUpConfirmed] (see [tags]) so a single scoped read finds the whole lifecycle of one sign-up attempt.
 */
val signUpDcbDecider: DcbDecider<SignUpCommand, SignUpState, SignUpEvent> = dcbDecider(
    initialState = SignUpState(),
    decide = ::decide,
    evolve = ::evolve,
    criteria = ::criteria,
    tags = ::tags
)

object SignUpTokenPolicy {
    /** How long a one-time password remains confirmable after it was issued. */
    val TTL: Duration = Duration.ofMinutes(15)
}

private fun criteria(command: SignUpCommand): DcbCriteria = DcbCriteria.tags(Tag.of("email", command.email), Tag.of("otp", command.otp))

private fun tags(event: SignUpEvent): Set<Tag> = setOf(Tag.of("email", event.email), Tag.of("otp", event.otp))

sealed interface SignUpCommand {
    val email: String
    val otp: String

    data class InitiateSignUp(override val email: String, override val otp: String, val initiatedAt: Instant) : SignUpCommand
    data class ConfirmSignUp(override val email: String, override val otp: String, val confirmedAt: Instant) : SignUpCommand
}

sealed interface SignUpEvent {
    val eventId: UUID
    val email: String
    val otp: String
}

data class SignUpInitiated(override val eventId: UUID, override val email: String, override val otp: String, val initiatedAt: Instant) : SignUpEvent
data class SignUpConfirmed(override val eventId: UUID, override val email: String, override val otp: String, val confirmedAt: Instant) : SignUpEvent

data class SignUpState(val initiatedAt: Instant? = null, val consumed: Boolean = false)

private fun decide(command: SignUpCommand, state: SignUpState): List<SignUpEvent> = when (command) {
    is SignUpCommand.InitiateSignUp -> {
        require(state.initiatedAt == null) { "A sign-up for ${command.email}/${command.otp} was already initiated" }
        listOf(SignUpInitiated(UUID.randomUUID(), command.email, command.otp, command.initiatedAt))
    }

    is SignUpCommand.ConfirmSignUp -> {
        val initiatedAt = state.initiatedAt ?: throw IllegalArgumentException("No pending sign-up for ${command.email}/${command.otp}")
        require(!state.consumed) { "Sign-up for ${command.email}/${command.otp} was already confirmed" }
        val expiresAt = initiatedAt.plus(SignUpTokenPolicy.TTL)
        require(!command.confirmedAt.isAfter(expiresAt)) { "One-time password for ${command.email} expired at $expiresAt" }
        listOf(SignUpConfirmed(UUID.randomUUID(), command.email, command.otp, command.confirmedAt))
    }
}

private fun evolve(state: SignUpState, event: SignUpEvent): SignUpState = when (event) {
    is SignUpInitiated -> state.copy(initiatedAt = event.initiatedAt)
    is SignUpConfirmed -> state.copy(consumed = true)
}
