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

package org.occurrent.example.domain.mastermind

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.occurrent.example.domain.mastermind.CodePeg.*
import org.occurrent.example.domain.mastermind.DetermineFeedback.determineFeedback
import org.occurrent.example.domain.mastermind.KeyPeg.Black
import org.occurrent.example.domain.mastermind.KeyPeg.White
import org.occurrent.example.domain.mastermind.KeyPegHoleStatus.Empty
import org.occurrent.example.domain.mastermind.KeyPegHoleStatus.Occupied
import java.util.stream.Stream


@DisplayName("MasterMind")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class MasterMindTest {

    companion object {
        @JvmStatic
        fun input(): Stream<Arguments> = Stream.of(
            Arguments.of(SecretCode(Red, Green, Blue, Yellow), Guess(Green, Purple, Red, Red), ExpectedFeedback(Occupied(White), Empty, Occupied(White), Empty)),
            Arguments.of(SecretCode(Red, Green, Blue, Yellow), Guess(Red, Purple, Red, Red), ExpectedFeedback(Occupied(Black), Empty, Empty, Empty)),
            Arguments.of(SecretCode(Red, Green, Blue, Red), Guess(Red, Red, Red, Red), ExpectedFeedback(Occupied(Black), Empty, Empty, Occupied(Black))),
            Arguments.of(SecretCode(Red, Green, Blue, Red), Guess(Red, Yellow, Red, Purple), ExpectedFeedback(Occupied(Black), Empty, Occupied(White), Empty)),
            Arguments.of(SecretCode(Red, Red, Blue, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Occupied(Black), Occupied(Black), Occupied(White), Empty)),
            Arguments.of(SecretCode(Red, Blue, Blue, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Occupied(Black), Occupied(White), Empty, Empty)),
            Arguments.of(SecretCode(Red, Blue, Red, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Occupied(Black), Occupied(White), Occupied(Black), Empty)),
            Arguments.of(SecretCode(Orange, Red, Red, Purple), Guess(Orange, Purple, Purple, Red), ExpectedFeedback(Occupied(Black), Occupied(White), Empty, Occupied(White))),
        )
    }

    @ParameterizedTest
    @MethodSource("input")
    fun `determine feedback`(secretCode: SecretCode, guess: Guess, expectedFeedback: ExpectedFeedback) {
        // When
        val (hole1Status, hole2Status, hole3Status, hole4Status) = determineFeedback(secretCode, guess)

        // Then
        val (expectedHole1Status, expectedHole2Status, expectedHole3Status, expectedHole4Status) = expectedFeedback

        assertAll(
            { assertThat(hole1Status).describedAs("status of key peg hole 1").isEqualTo(expectedHole1Status) },
            { assertThat(hole2Status).describedAs("status of key peg hole 2").isEqualTo(expectedHole2Status) },
            { assertThat(hole3Status).describedAs("status of key peg hole 3").isEqualTo(expectedHole3Status) },
            { assertThat(hole4Status).describedAs("status of key peg hole 4").isEqualTo(expectedHole4Status) },
        )
    }

    data class ExpectedFeedback(val hole1: KeyPegHoleStatus, val hole2: KeyPegHoleStatus, val hole3: KeyPegHoleStatus, val hole4: KeyPegHoleStatus)
}