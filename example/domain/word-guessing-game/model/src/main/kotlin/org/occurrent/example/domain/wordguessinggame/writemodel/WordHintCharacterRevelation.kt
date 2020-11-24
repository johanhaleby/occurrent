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

import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import java.util.*
import kotlin.math.min

data class WordHintData(val gameId: UUID, val wordToGuess: String, val currentlyRevealedPositions: Set<Int> = emptySet())

private data class RevealedCharacter(val character: Char, val characterPositionInWord: Int)

object WordHintCharacterRevelation {
    internal const val dash = '-'
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
        val potentialIndicesToReveal = indices.filter { index -> get(index) != dash && !currentlyRevealedPositions.contains(index.inc()) }.toMutableList()

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