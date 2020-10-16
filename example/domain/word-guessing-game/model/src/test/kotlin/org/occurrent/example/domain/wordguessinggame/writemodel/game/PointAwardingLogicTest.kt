package org.occurrent.example.domain.wordguessinggame.writemodel.game

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

@DisplayName("point awarding logic")
internal class PointAwardingLogicTest {

    @Nested
    @DisplayName("calculate points to award player after successfully guessed the right word")
    inner class CalculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord {

        @Test
        fun `awards 5 points when player guessed the right word on the first attempt`() {
            // Given
            val totalNumberOfGuessedForPlayer = 1

            // When
            val points = PointAwardingLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessedForPlayer)

            // Then
            assertThat(points).isEqualTo(5)
        }

        @Test
        fun `awards 3 points when player guessed the right word on the second attempt`() {
            // Given
            val totalNumberOfGuessedForPlayer = 2

            // When
            val points = PointAwardingLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessedForPlayer)

            // Then
            assertThat(points).isEqualTo(3)
        }

        @Test
        fun `awards 1 points when player guessed the right word on the third attempt`() {
            // Given
            val totalNumberOfGuessedForPlayer = 3

            // When
            val points = PointAwardingLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessedForPlayer)

            // Then
            assertThat(points).isEqualTo(1)
        }

        @Test
        fun `doesn't award any points when player guessed the right word on additional attempt`() {
            // Given
            val totalNumberOfGuessedForPlayer = 4

            // When
            val points = PointAwardingLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessedForPlayer)

            // Then
            assertThat(points).isZero
        }
    }
}