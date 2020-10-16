package org.occurrent.example.domain.wordguessinggame.writemodel.game

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

@DisplayName("point calculation logic")
internal class PointCalculationLogicTest {

    @Nested
    @DisplayName("calculate points to award player after successfully guessed the right word")
    inner class CalculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord {

        @Test
        fun `awards 5 points when player guessed the right word on the first attempt`() {
            // Given
            val totalNumberOfGuessesForPlayer = 1

            // When
            val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessesForPlayer)

            // Then
            assertThat(points).isEqualTo(5)
        }

        @Test
        fun `awards 3 points when player guessed the right word on the second attempt`() {
            // Given
            val totalNumberOfGuessesForPlayer = 2

            // When
            val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessesForPlayer)

            // Then
            assertThat(points).isEqualTo(3)
        }

        @Test
        fun `awards 1 points when player guessed the right word on the third attempt`() {
            // Given
            val totalNumberOfGuessesForPlayer = 3

            // When
            val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessesForPlayer)

            // Then
            assertThat(points).isEqualTo(1)
        }

        @Test
        fun `doesn't award any points when player guessed the right word on additional attempt`() {
            // Given
            val totalNumberOfGuessesForPlayer = 4

            // When
            val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessesForPlayer)

            // Then
            assertThat(points).isZero
        }
    }
}