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

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.util.*


class WordHintCharacterRevelationEdgeCasesTest {


    @Test
    fun `reveal initial characters in word hint when game was started reveals one character when word has 4 characters where one is dash`() {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), "ab-c")

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        val e1 = events[0]
        assertAll(
                { Assertions.assertThat(events).hasSize(1) },
                { Assertions.assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { Assertions.assertThat(e1.character).isNotEqualTo(WordHintCharacterRevelation.dash) },
                { Assertions.assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },
        )

    }

    @Test
    fun `reveal initial characters in word hint when game was started doesn't reveal any character when word has 3 characters where one is dash`() {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), "a-b")

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        Assertions.assertThat(events).isEmpty()
    }
}