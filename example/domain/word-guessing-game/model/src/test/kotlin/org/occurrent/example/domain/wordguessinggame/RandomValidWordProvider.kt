package org.occurrent.example.domain.wordguessinggame

import net.jqwik.api.Arbitraries
import net.jqwik.api.Arbitrary


object RandomValidWordProvider {

    fun provideValidRandomWords(): Arbitrary<String> =
            Arbitraries.strings()
                    // Allow alpha characters
                    .alpha()
                    // Allow whitespace characters
                    .whitespace()
                    // No consecutive whitespaces
                    .filter { wordToGuess -> !wordToGuess.contains("  ") }
                    // Word start or end with whitespace
                    .map(String::trim)
                    // Must be between 3 and 15 characters
                    .filter { wordToGuess -> wordToGuess.length in 3..15 }
}