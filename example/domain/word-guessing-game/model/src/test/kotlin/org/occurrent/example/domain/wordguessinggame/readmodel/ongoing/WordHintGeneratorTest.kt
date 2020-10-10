package org.occurrent.example.domain.wordguessinggame.readmodel.ongoing

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.generateNewHint
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.obfuscationCharacter
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.whitespace

@DisplayName("word hint generator")
internal class WordHintGeneratorTest {

    @Nested
    @DisplayName("generate new hint")
    inner class GenerateNewHint {

        @Nested
        @DisplayName("when word contains space")
        inner class WhenWordContainsSpace {

            @Test
            fun `then two characters are revealed and the rest are obfuscated`() {
                // Given
                val wordToGuess = "hello world"

                // When
                val hint = wordToGuess.generateNewHint()

                // Then
                assertAll(
                        { assertThat(hint).hasSameSizeAs(wordToGuess) },
                        { assertThat(hint.count { it != obfuscationCharacter && it != whitespace }).isEqualTo(2) },
                )
            }

            @Test
            fun `only one char is revealed when revealing more characters would exceed minimum of hidden characters`() {
                // Given
                val wordToGuess = "he j"

                // When
                val hint = wordToGuess.generateNewHint()

                // Then
                assertAll(
                        { assertThat(hint).hasSameSizeAs(wordToGuess) },
                        { assertThat(hint.count { it != obfuscationCharacter && it != whitespace }).isEqualTo(1) },
                )
            }
        }
    }
}