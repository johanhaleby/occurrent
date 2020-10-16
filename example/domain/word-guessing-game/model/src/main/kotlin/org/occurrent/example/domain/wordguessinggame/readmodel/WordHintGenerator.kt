package org.occurrent.example.domain.wordguessinggame.readmodel

internal object WordHintGenerator {
    internal const val obfuscationCharacter = '_'
    internal const val whitespace = ' '
    private const val minimumNumberOfRevealedCharactersInWordHint = 2
    private const val minimumNumberOfObfuscatedCharactersInWordHint = 2

    private fun WordHint.findObfuscationPositions(): List<Int> = mapIndexedNotNull { index, char -> if (char == obfuscationCharacter && char != whitespace) index else null }

    private fun WordHint.replaceCharAt(index: Int, char: Char): String {
        val b = StringBuilder(this)
        b.setCharAt(index, char)
        return b.toString()
    }

    private fun String.obfuscateCharacters() = map { char -> if (char == whitespace) whitespace else obfuscationCharacter }.joinToString(separator = "")


    internal fun WordToGuess.generateNewHint(): WordHint {
        fun WordHint.needToRevealMoreCharacters(): Boolean {
            val characterFrequenciesInWord = groupBy { it }.mapValues { (_, v) -> v.size }
            val numberOfWhitespace = characterFrequenciesInWord.getOrElse(whitespace) { 0 }
            val numberOfObfuscatedCharacters = characterFrequenciesInWord.getOrElse(obfuscationCharacter) { 0 }
            val numberOfRevealedCharactersIncludingWhitespace = length - numberOfObfuscatedCharacters
            val numberOfRevealedCharactersExcludingWhitespace = numberOfRevealedCharactersIncludingWhitespace - numberOfWhitespace

            return numberOfRevealedCharactersExcludingWhitespace < minimumNumberOfRevealedCharactersInWordHint
                    && numberOfRevealedCharactersIncludingWhitespace < (length - minimumNumberOfObfuscatedCharactersInWordHint)
        }

        fun WordHint.revealMinimumNumberOfCharacters(wordToGuess: WordToGuess): WordHint {
            val newHint = revealAdditionalCharacterFrom(wordToGuess)
            return if (newHint.needToRevealMoreCharacters()) newHint.revealMinimumNumberOfCharacters(wordToGuess) else newHint
        }

        val hintWithAllCharactersObfuscated: WordHint = obfuscateCharacters()
        return hintWithAllCharactersObfuscated.revealMinimumNumberOfCharacters(this)
    }

    internal fun WordHint.revealAdditionalCharacterFrom(wordToGuess: WordToGuess): WordHint {
        val findObfuscationPositions = findObfuscationPositions()
        return if (findObfuscationPositions.size == minimumNumberOfObfuscatedCharactersInWordHint) {
            // The hint should never obfuscate the last two characters
            this
        } else {
            val randomPositionThatIsCurrentlyObfuscated = findObfuscationPositions.random()
            val charAtRandomPosition = wordToGuess[randomPositionThatIsCurrentlyObfuscated]
            replaceCharAt(randomPositionThatIsCurrentlyObfuscated, charAtRandomPosition)
        }
    }
}