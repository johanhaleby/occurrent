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
        return wordHintData.wordToGuess.revealNewRandomCharacters(minimumNumberOfRevealedCharactersInWordHint).thenConvertToCharacterInWordHintWasRevealedEvents(Timestamp(), wordHintData.gameId)
    }

    fun revealCharacterInWordHintWhenPlayerGuessedTheWrongWord(wordHintData: WordHintData): Sequence<CharacterInWordHintWasRevealed> {
        val (gameId, wordToGuess, currentlyRevealedPositions) = wordHintData
        return wordToGuess.revealNewRandomCharacters(1, currentlyRevealedPositions).thenConvertToCharacterInWordHintWasRevealedEvents(Timestamp(), gameId)
    }

    private fun String.revealNewRandomCharacters(maxNumberOfCharactersToReveal: Int, currentlyRevealedPositions: Set<Int> = emptySet()): List<RevealedCharacter> {
        val potentialIndicesToReveal = indices.filter { index -> get(index) != whitespace && !currentlyRevealedPositions.contains(index.inc()) }.toMutableList()

        val actualNumberOfCharactersToReveal = min(maxNumberOfCharactersToReveal, potentialIndicesToReveal.size - minimumNumberOfObfuscatedCharactersInWordHint)
        if (actualNumberOfCharactersToReveal <= 0) {
            return emptyList()
        }

        @Suppress("UnnecessaryVariable")
        val revealCharacters = (0 until actualNumberOfCharactersToReveal).map {
            val index = potentialIndicesToReveal.indices.random()
            val indexOfCharacterToReveal = potentialIndicesToReveal[index]
            val characterToReveal = this[indexOfCharacterToReveal]
            potentialIndicesToReveal.removeAt(index)
            RevealedCharacter(characterToReveal, indexOfCharacterToReveal + 1)
        }
        return revealCharacters
    }
}

private fun List<RevealedCharacter>.thenConvertToCharacterInWordHintWasRevealedEvents(timestamp: Timestamp, gameId: UUID): Sequence<CharacterInWordHintWasRevealed> {
    return map { (character, characterPositionInWord) -> CharacterInWordHintWasRevealed(UUID.randomUUID(), timestamp, gameId, character, characterPositionInWord) }.asSequence()
}