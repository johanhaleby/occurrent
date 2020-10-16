package org.occurrent.example.domain.wordguessinggame.writemodel

typealias Points = Int
typealias NumberOfGuessesRequiredToGuessToRightWord = Int

internal object PointCalculationLogic {

    private const val NUMBER_OF_POINTS_FOR_FIRST_GUESS = 5
    private const val NUMBER_OF_POINTS_FOR_SECOND_GUESS = 3
    private const val NUMBER_OF_POINTS_FOR_LAST_GUESS = 1

    internal fun calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(numberOfGuesses: NumberOfGuessesRequiredToGuessToRightWord): Points = when (numberOfGuesses) {
        1 -> NUMBER_OF_POINTS_FOR_FIRST_GUESS
        2 -> NUMBER_OF_POINTS_FOR_SECOND_GUESS
        MaxNumberOfGuessesPerPlayer.value -> NUMBER_OF_POINTS_FOR_LAST_GUESS
        else -> 0
    }
}