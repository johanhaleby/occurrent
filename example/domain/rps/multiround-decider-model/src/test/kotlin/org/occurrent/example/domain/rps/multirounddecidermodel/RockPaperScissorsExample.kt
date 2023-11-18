/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.multirounddecidermodel

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.dsl.decider.component1
import org.occurrent.dsl.decider.component2
import org.occurrent.example.domain.rps.multirounddecidermodel.InitiateNewGame.NumberOfRounds.THREE

@DisplayName("Example of Rock Paper Scissors Game")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class RockPaperScissorsExample {

    @Test
    fun `using decideOnEvents with multiple commands`() {
        // Given
        val gameId = GameId.randomUUID()
        val firstPlayerId = PlayerId.randomUUID()
        val secondPlayerId = PlayerId.randomUUID()

        // When
        val (state, events) = rps.decideOnEvents(emptyList(),
            InitiateNewGame(gameId, Timestamp.now(), firstPlayerId, THREE),

            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.PAPER),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.PAPER),

            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.ROCK),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.SCISSORS),

            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.SCISSORS),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.PAPER),
        )

        // Then
        println("state:\n$state")
        println()
        println("events:\n${events.joinToString("\n")}")
    }

    @Test
    fun `using decideOnState with multiple commands`() {
        // Given
        val gameId = GameId.randomUUID()
        val firstPlayerId = PlayerId.randomUUID()
        val secondPlayerId = PlayerId.randomUUID()

        // When
        val (state1, events1) = rps.decideOnEvents(emptyList(), InitiateNewGame(gameId, Timestamp.now(), firstPlayerId, THREE))

        val (state2, events2) = rps.decideOnState(state1,
            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.PAPER),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.PAPER),

            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.ROCK),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.SCISSORS),

            ShowHandGesture(gameId, Timestamp.now(), firstPlayerId, HandGesture.SCISSORS),
            ShowHandGesture(gameId, Timestamp.now(), secondPlayerId, HandGesture.PAPER),
        )

        // Then
        println("state:\n$state2")
        println()
        println("events:\n${(events1 + events2).joinToString("\n")}")
    }
}