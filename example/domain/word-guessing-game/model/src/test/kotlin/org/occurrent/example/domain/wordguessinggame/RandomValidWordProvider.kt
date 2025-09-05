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

package org.occurrent.example.domain.wordguessinggame

import net.jqwik.api.Arbitraries
import net.jqwik.api.Arbitrary


object RandomValidWordProvider {
    
    private const val DASH = '-'

    fun provideValidRandomWords(limitWordLength: IntRange = 3..15, allowDash: Boolean = true): Arbitrary<String> =
            Arbitraries.strings()
                    // Allow alpha characters
                    .alpha()
                    .ofMinLength(limitWordLength.first)
                    .ofMaxLength(limitWordLength.last)
                    .let { arbitrary ->
                        if (allowDash) {
                            arbitrary
                                    // Allow dash character
                                    .withChars(DASH)
                                    // No consecutive dashes
                                    .filter { wordToGuess -> !wordToGuess.contains(DASH) }
                                    // Word cannot start with dash
                                    .map { wordToGuess -> if (wordToGuess.startsWith(DASH)) wordToGuess.replaceFirst(DASH, ('A'..'Z').random()) else wordToGuess }
                                    // Word cannot end with dash
                                    .map { wordToGuess -> if (wordToGuess.endsWith(DASH)) wordToGuess.replaceRange(wordToGuess.length - 2, wordToGuess.length - 1, ('A'..'Z').random().toString()) else wordToGuess }
                        } else {
                            arbitrary
                        }
                    }
}