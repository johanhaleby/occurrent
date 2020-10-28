package org.occurrent.example.domain.wordguessinggame

import net.jqwik.api.Arbitraries
import net.jqwik.api.Arbitrary


object RandomValidWordProvider {

    fun provideValidRandomWords(limitWordLength: IntRange = 3..15, allowWhitespace: Boolean = true): Arbitrary<String> =
            Arbitraries.strings()
                    // Allow alpha characters
                    .alpha()
                    .ofMinLength(limitWordLength.first)
                    .ofMaxLength(limitWordLength.last)
                    .let { arbitrary ->
                        if (allowWhitespace) {
                            arbitrary
                                    // Allow whitespace characters
                                    .whitespace()
                                    // No consecutive whitespaces
                                    .filter { wordToGuess -> !wordToGuess.contains("  ") }
                                    // Word cannot start with whitespace
                                    .map { wordToGuess -> if (wordToGuess.startsWith(" ")) wordToGuess.replaceFirst(' ', ('A'..'Z').random()) else wordToGuess }
                                    // Word cannot end with whitespace
                                    .map { wordToGuess -> if (wordToGuess.endsWith(" ")) wordToGuess.replaceRange(wordToGuess.length - 2, wordToGuess.length - 1, ('A'..'Z').random().toString()) else wordToGuess }
                        } else {
                            arbitrary
                        }
                    }
}