package org.occurrent.example.domain.wordguessinggame.writemodel

import java.util.*

typealias GameId = UUID
typealias Timestamp = Date
typealias PlayerId = UUID

data class Word(val value: String) {
    init {
        require(value.length in 4..10) {
            "A word must be between 4 and 10 characters long"
        }
        require(value.trim() == value) {
            "Word cannot start or end with whitespace"
        }
    }

    fun hasValue(value: String): Boolean = this.value == value
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

data class WordsToChooseFrom(val category: WordCategory, val words: List<Word>) : Sequence<Word> {
    init {
        require(words.size in 5..20) {
            "You need to supply between 5 to 20 words in the $category category"
        }
    }

    override fun iterator(): Iterator<Word> = words.iterator()
}