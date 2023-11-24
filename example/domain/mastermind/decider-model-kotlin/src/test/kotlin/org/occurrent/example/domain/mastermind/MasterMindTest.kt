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
import java.util.stream.Stream


@DisplayName("MasterMind")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class MasterMindTest {

    companion object {
        @JvmStatic
        fun input(): Stream<Arguments> = Stream.of(
            Arguments.of(SecretCode(Red, Green, Blue, Yellow), Guess(Green, Purple, Red, Red), ExpectedFeedback(White, null, White, null)),
            Arguments.of(SecretCode(Red, Green, Blue, Yellow), Guess(Red, Purple, Red, Red), ExpectedFeedback(Black, null, null, null)),
            Arguments.of(SecretCode(Red, Green, Blue, Red), Guess(Red, Red, Red, Red), ExpectedFeedback(Black, null, null, Black)),
            Arguments.of(SecretCode(Red, Green, Blue, Red), Guess(Red, Yellow, Red, Purple), ExpectedFeedback(Black, null, White, null)),
            Arguments.of(SecretCode(Red, Red, Blue, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Black, Black, White, null)),
            Arguments.of(SecretCode(Red, Blue, Blue, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Black, White, null, null)),
            Arguments.of(SecretCode(Red, Blue, Red, Red), Guess(Red, Red, Red, Purple), ExpectedFeedback(Black, White, Black, null)),
        )
    }

    @ParameterizedTest
    @MethodSource("input")
    fun `determine feedback`(secretCode: SecretCode, guess: Guess, expectedFeedback: ExpectedFeedback) {
        // When
        val (peg1, peg2, peg3, peg4) = determineFeedback(secretCode, guess)

        // Then
        val (expectedKeyPeg1, expectedKeyPeg2, expectedKeyPeg3, expectedKeyPeg4) = expectedFeedback

        assertAll(
            { assertThat(peg1).describedAs("peg1").isEqualTo(expectedKeyPeg1) },
            { assertThat(peg2).describedAs("peg2").isEqualTo(expectedKeyPeg2) },
            { assertThat(peg3).describedAs("peg3").isEqualTo(expectedKeyPeg3) },
            { assertThat(peg4).describedAs("peg4").isEqualTo(expectedKeyPeg4) },
        )
    }

    data class ExpectedFeedback(val peg1: KeyPeg?, val peg2: KeyPeg?, val peg3: KeyPeg?, val peg4: KeyPeg?)
}