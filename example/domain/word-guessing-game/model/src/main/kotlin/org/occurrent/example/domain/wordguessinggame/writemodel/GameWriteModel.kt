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

import java.util.*

typealias GameId = UUID
typealias Timestamp = Date
typealias PlayerId = UUID

class Word(value: String) {

    companion object {
        private const val DASH = '-'
        const val VALID_WORD_REGEX = "^[A-Za-z]([A-Za-z]|$DASH)+[A-Za-z]$"
        const val MINIMUM_NUMBER_OF_CHARACTERS = 3
        const val MAXIMUM_NUMBER_OF_CHARACTERS = 15
    }

    val value: String

    init {
        require(value.length in MINIMUM_NUMBER_OF_CHARACTERS..MAXIMUM_NUMBER_OF_CHARACTERS) {
            "A word must be between $MINIMUM_NUMBER_OF_CHARACTERS and $MAXIMUM_NUMBER_OF_CHARACTERS characters long (\"$value\" doesn't fulfill this criteria)"
        }

        require(!value.contains("$DASH$DASH")) {
            "Word cannot contain two consecutive dashes (\"$value\")"
        }

        require(value.matches(Regex(VALID_WORD_REGEX))) {
            "Word can only contain alphabetic characters and dash (and it cannot start or end with dash), was \"$value\"."
        }
        this.value = value
    }

    fun hasValue(value: String): Boolean = this.value == value

    override fun toString(): String {
        return "Word(value='$value')"
    }
}

data class WordCategory(val value: String) {
    init {
        require(value.isNotEmpty()) {
            "Word category cannot be empty"
        }
        require(value.trim() == value) {
            "Word category cannot start or end with whitespace"
        }
    }
}

object MaxNumberOfGuessesPerPlayer {
    const val value = 3
}

object MaxNumberOfGuessesTotal {
    const val value = 10
}

data class WordList(val category: WordCategory, val words: List<Word>) : Sequence<Word> {
    init {
        val distinctWords = words.distinctBy { it.value.uppercase(Locale.getDefault()) }
        if (distinctWords.size != words.size) {
            val duplicateWords = words.groupBy { it.value.uppercase(Locale.getDefault()) }
                    .filterValues { it.size > 1 }
                    .values
                    .joinToString { wordList -> wordList.map(Word::value).joinToString() }
            throw IllegalArgumentException("Duplicate words in the same category is not allowed: $duplicateWords")
        }

        require(words.size in 4..20) {
            "You need to supply between 4 to 20 words in the ${category.value} category, was ${words.size}."
        }
    }

    override fun iterator(): Iterator<Word> = words.iterator()
}