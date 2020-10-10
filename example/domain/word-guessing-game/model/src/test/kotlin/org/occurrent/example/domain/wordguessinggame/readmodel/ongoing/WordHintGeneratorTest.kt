package org.occurrent.example.domain.wordguessinggame.readmodel.ongoing

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.generateNewHint

@DisplayName("word hint generator")
internal class WordHintGeneratorTest {

    @Nested
    @DisplayName("generate new hint")
    inner class GenerateNewHint {

        @Nested
        @DisplayName("when word contains space")
        inner class WhenWordContainsSpace {

            @Test
            fun `then ikk `() {
                // Given
                val wordToGuess = "Carrot soup"

                // When
                val generateNewHint = wordToGuess.generateNewHint()

                // Then
                println(generateNewHint)
            }
        }

    }
}