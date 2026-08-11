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

package org.occurrent.dsl.saga.flow

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.SagaEffect
import org.occurrent.dsl.saga.SagaInput
import org.occurrent.dsl.saga.TimerName
import java.time.Instant

@DisplayName("flow saga with an absolute (data-derived) timeout")
class AbsoluteTimeoutFlowSagaTest {

    private sealed interface AuctionEvent {
        val auctionId: String
    }

    private data class AuctionStarted(override val auctionId: String, val endsAt: Instant) : AuctionEvent
    private data class BidPlaced(override val auctionId: String) : AuctionEvent

    private sealed interface AuctionCommand
    private data class CloseAuction(val auctionId: String) : AuctionCommand

    private val endsAt: Instant = Instant.parse("2026-07-19T18:00:00Z")

    private val auctionSaga: Saga<AuctionEvent, FlowState<AuctionEvent>, AuctionCommand> = saga {
        startsOn<AuctionStarted>()
        correlate<AuctionStarted> { it.auctionId }
        correlate<BidPlaced> { it.auctionId }
        step("bidding") {
            on<BidPlaced>(then = transitionTo("bidding"))
            timeout(at = { received -> received.initiating<AuctionStarted>().endsAt }, then = end) { received ->
                issue(CloseAuction(received.initiating<AuctionStarted>().auctionId))
            }
        }
    }

    @Test
    @DisplayName("arms an absolute timeout computed from the initiating event when the instance starts")
    fun armsAbsoluteTimeoutOnStart() {
        val started = AuctionStarted("a1", endsAt)
        val afterStart = auctionSaga.evolve(auctionSaga.initialState(), SagaInput.event(started))

        val startEffects = auctionSaga.onStart(afterStart, started) + auctionSaga.react(afterStart, SagaInput.event(started))

        assertThat(startEffects).containsExactly(SagaEffect.startTimeoutAt<AuctionCommand>("step:bidding", endsAt))
    }

    @Test
    @DisplayName("issues the expiry command and completes when the absolute timeout fires")
    fun firesAbsoluteTimeout() {
        val started = AuctionStarted("a1", endsAt)
        val afterStart = auctionSaga.evolve(auctionSaga.initialState(), SagaInput.event(started))

        val step = auctionSaga.step(afterStart, SagaInput.timeout("a1", TimerName.parse("step:bidding")))

        assertThat(auctionSaga.isTerminal(step.state())).isTrue()
        assertThat(step.effects()).containsExactly(SagaEffect.issue<AuctionCommand>(CloseAuction("a1")))
    }
}
