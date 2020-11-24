/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.example.domain.wordguessinggame.writemodel

import net.jqwik.api.Arbitrary
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.Provide
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assumptions.assumeThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertAll
import org.occurrent.example.domain.wordguessinggame.RandomValidWordProvider
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation.dash
import java.util.*

@DisplayName("word hint character revelation")
internal class WordHintCharacterRevelationPropertyBasedTest {


    // Initial characters
    @Property
    fun `reveal initial characters in word hint when game was started reveals two characters when word is longer than 3 and doesn't contain dash`(@ForAll("wordsLongerThan3CharactersAndNoDash") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        val (e1, e2) = events
        // Then
        assertAll(
                { assertThat(events).hasSize(2) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(dash) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },

                { assertThat(e2.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e2.character).isNotEqualTo(dash) },
                { assertThat(e2.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },

                { assertThat(e1.characterPositionInWord).isNotEqualTo(e2.characterPositionInWord) },
        )
    }

    @Property
    fun `reveal initial characters in word hint when game was started reveals one character that is not dash when word has 3 characters without dash`(@ForAll("wordsHas3CharsAndNoDash") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        val e1 = events[0]
        // Then
        assertAll(
                { assertThat(events).hasSize(1) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(dash) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },
        )
    }

    @Property
    fun `reveal initial characters in word hint when game was started reveals one character when word has 4 characters where one is dash`(@ForAll("wordsHas4CharactersAndDash") wordToGuess: String) {
        // Given
        assumeThat(wordToGuess.count { char -> char == dash }).isEqualTo(1)
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        val e1 = events[0]
        assertAll(
                { assertThat(events).hasSize(1) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(dash) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },
        )

    }

    @Property
    fun `reveal initial characters in word hint when game was started doesn't reveal any character when word has 3 characters where one is dash`(@ForAll("wordsHas3CharsAndOneDash") wordToGuess: String) {
        // Given
        assumeThat(wordToGuess.count { char -> char == dash }).isEqualTo(1)
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        assertThat(events).isEmpty()
    }

    @Provide
    fun wordsLongerThan3CharactersAndNoDash(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 4..15, allowDash = false)

    @Provide
    fun wordsHas4CharactersAndDash(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 4..4, allowDash = true)

    @Provide
    fun wordsHas3CharsAndOneDash(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 3..3)

    @Provide
    fun wordsHas3CharsAndNoDash(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 3..3, allowDash = false)
}