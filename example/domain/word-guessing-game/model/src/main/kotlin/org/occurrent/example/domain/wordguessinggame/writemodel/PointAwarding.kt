package org.occurrent.example.domain.wordguessinggame.writemodel

import org.occurrent.example.domain.wordguessinggame.event.DomainEvent
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasNotAwardedAnyPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.ReasonForNotBeingAwardedPoints.PlayerCreatedListOfWords
import java.util.*

typealias Points = Int
typealias TotalNumberOfGuessesRequiredForPlayerToGuessToRightWord = Int

data class BasisForPointAwarding(val gameId: GameId, val playerThatCreatedTheWordList: PlayerId, val playerThatGuessedTheRightWord: PlayerId, val totalNumberGuessesForPlayerInGame: TotalNumberOfGuessesRequiredForPlayerToGuessToRightWord)

object PointAwarding {

    fun awardPointsToPlayerThatGuessedTheRightWord(basisForPointAwarding: BasisForPointAwarding): Sequence<DomainEvent> {
        val (gameId, playerThatCreatedTheWordList, playerThatGuessedTheRightWord, totalGuessesForPlayerInGameTotal) = basisForPointAwarding
        return if (playerThatCreatedTheWordList == playerThatGuessedTheRightWord) {
            sequenceOf(PlayerWasNotAwardedAnyPointsForGuessingTheRightWord(UUID.randomUUID(), Timestamp(), gameId, playerThatGuessedTheRightWord, reason = PlayerCreatedListOfWords))
        } else {
            val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalGuessesForPlayerInGameTotal)
            sequenceOf(PlayerWasAwardedPointsForGuessingTheRightWord(UUID.randomUUID(), Timestamp(), gameId, playerThatGuessedTheRightWord, points))
        }
    }
}


internal object PointCalculationLogic {

    private const val NUMBER_OF_POINTS_FOR_FIRST_GUESS = 5
    private const val NUMBER_OF_POINTS_FOR_SECOND_GUESS = 3
    private const val NUMBER_OF_POINTS_FOR_LAST_GUESS = 1

    internal fun calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalNumberOfGuessesForPlayer: TotalNumberOfGuessesRequiredForPlayerToGuessToRightWord): Points = when (totalNumberOfGuessesForPlayer) {
        1 -> NUMBER_OF_POINTS_FOR_FIRST_GUESS
        2 -> NUMBER_OF_POINTS_FOR_SECOND_GUESS
        MaxNumberOfGuessesPerPlayer.value -> NUMBER_OF_POINTS_FOR_LAST_GUESS
        else -> throw IllegalStateException("Internal error: Number of guesses required for player exceeded expected value. Was $totalNumberOfGuessesForPlayer, max expected ${MaxNumberOfGuessesPerPlayer.value}.")
    }
}