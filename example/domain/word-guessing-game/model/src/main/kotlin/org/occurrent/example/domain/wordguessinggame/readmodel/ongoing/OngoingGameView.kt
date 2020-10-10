package org.occurrent.example.domain.wordguessinggame.readmodel.ongoing

import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.generateNewHint
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.revealAdditionalCharacterFrom
import org.occurrent.example.domain.wordguessinggame.support.add
import java.util.*

typealias WordToGuess = String
typealias WordHint = String
typealias GuessedWord = String
typealias Category = String

internal object WordHintGenerator {
    private const val obfuscationCharacter = '_'
    private const val whitespace = ' '
    private const val minimumNumberOfRevealedCharactersForWordHint = 2
    private const val maximumNumberOfRevealedCharactersForWordHint = 2

    private fun WordHint.findObfuscationPositions(): List<Int> = mapIndexedNotNull { index, char -> if (char == obfuscationCharacter && char != whitespace) index else null }

    private fun WordHint.replaceCharAt(index: Int, char: Char): String {
        val b = StringBuilder(this)
        b.setCharAt(index, char)
        return b.toString()
    }

    private fun String.obfuscateCharacters() = map { char -> if (char == whitespace) whitespace else obfuscationCharacter }.joinToString(separator = "")


    internal fun WordToGuess.generateNewHint(): WordHint {
        // Minimum number of characters that are not obfuscated are two
        fun WordHint.needsToRevealMoreCharacters() = count { character -> character != obfuscationCharacter && character != whitespace } < minimumNumberOfRevealedCharactersForWordHint

        fun WordHint.revealMinimumNumberOfCharacters(wordToGuess: WordToGuess): WordHint {
            val newHint = revealAdditionalCharacterFrom(wordToGuess)
            return if (newHint.needsToRevealMoreCharacters()) newHint.revealMinimumNumberOfCharacters(wordToGuess) else newHint
        }

        val hintWithAllCharactersObfuscated: WordHint = obfuscateCharacters()
        return hintWithAllCharactersObfuscated.revealMinimumNumberOfCharacters(this)
    }

    internal fun WordHint.revealAdditionalCharacterFrom(wordToGuess: WordToGuess): WordHint {
        val findObfuscationPositions = findObfuscationPositions()
        return if (findObfuscationPositions.size <= maximumNumberOfRevealedCharactersForWordHint) {
            // The hint should never obfuscate the last two characters
            this
        } else {
            val randomPositionThatIsCurrentlyObfuscated = findObfuscationPositions.random()
            val charAtRandomPosition = wordToGuess[randomPositionThatIsCurrentlyObfuscated]
            replaceCharAt(randomPositionThatIsCurrentlyObfuscated, charAtRandomPosition)
        }
    }
}

data class Guess internal constructor(val playerId: UUID, val word: GuessedWord, val guessMadeAt: Date)
data class OngoingGameView internal constructor(val gameId: UUID, val startedAt: Date, val category: Category,
                                                val hint: WordHint, val guesses: List<Guess>,
                                                internal val wordToGuess: WordToGuess)

fun initializeOngoingGameViewWhenGameWasStarted(e: GameWasStarted): OngoingGameView = e.run {
    OngoingGameView(gameId, timestamp, category, wordToGuess.generateNewHint(), emptyList(), wordToGuess)
}

fun addGuessToOngoingGameViewWhenPlayerGuessedTheWrongWord(view: OngoingGameView, e: PlayerGuessedTheWrongWord): OngoingGameView = e.run {
    view.copy(guesses = view.guesses.add(Guess(playerId, guessedWord, timestamp)), hint = view.hint.revealAdditionalCharacterFrom(view.wordToGuess))
}