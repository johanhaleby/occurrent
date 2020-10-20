package org.occurrent.example.domain.wordguessinggame.writemodel

import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import java.util.*
import kotlin.math.min

data class WordHintData(val gameId: UUID, val wordToGuess: String, val currentlyRevealedPositions: Set<Int> = emptySet())

private data class RevealedCharacter(val character: Char, val characterPositionInWord: Int)

object WordHintCharacterRevelation {
    internal const val whitespace = ' '
    private const val minimumNumberOfRevealedCharactersInWordHint = 2
    private const val minimumNumberOfObfuscatedCharactersInWordHint = 2

    fun revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData: WordHintData): Sequence<CharacterInWordHintWasRevealed> {
        return wordHintData.wordToGuess.revealNewRandomCharacters(minimumNumberOfRevealedCharactersInWordHint).thenConvertToCharacterInWordHintWasRevealedEvents(wordHintData.gameId)
    }

    fun revealCharacterInWordHintWhenPlayerGuessedTheWrongWord(wordHintData: WordHintData): Sequence<CharacterInWordHintWasRevealed> {
        val (gameId, wordToGuess, currentlyRevealedPositions) = wordHintData
        return wordToGuess.revealNewRandomCharacters(1, currentlyRevealedPositions).thenConvertToCharacterInWordHintWasRevealedEvents(gameId)
    }

    private fun String.revealNewRandomCharacters(maxNumberOfCharactersToReveal: Int, currentlyRevealedPositions: Set<Int> = emptySet()): List<RevealedCharacter> {
        val potentialIndicesToReveal = toCharArray().filterIndexed { index, char -> char != whitespace && !currentlyRevealedPositions.contains(index.inc()) }.toMutableList()

        val actualNumberOfCharactersToReveal = min(maxNumberOfCharactersToReveal, potentialIndicesToReveal.size - minimumNumberOfObfuscatedCharactersInWordHint)
        if (actualNumberOfCharactersToReveal <= 0) {
            return emptyList()
        }

        @Suppress("UnnecessaryVariable")
        val revealCharacters = (0 until actualNumberOfCharactersToReveal).map {
            val randomIndexOfCharacterToReveal = potentialIndicesToReveal.indices.random()
            val characterToReveal = this[randomIndexOfCharacterToReveal]
            potentialIndicesToReveal.removeAt(randomIndexOfCharacterToReveal)
            RevealedCharacter(characterToReveal, randomIndexOfCharacterToReveal + 1)
        }
        return revealCharacters
    }
}

private fun List<RevealedCharacter>.thenConvertToCharacterInWordHintWasRevealedEvents(gameId: UUID): Sequence<CharacterInWordHintWasRevealed> {
    return map { (character, characterPositionInWord) -> CharacterInWordHintWasRevealed(UUID.randomUUID(), Timestamp(), gameId, character, characterPositionInWord) }.asSequence()
}