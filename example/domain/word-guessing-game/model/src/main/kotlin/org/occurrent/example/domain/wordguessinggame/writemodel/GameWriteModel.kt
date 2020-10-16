package org.occurrent.example.domain.wordguessinggame.writemodel

import java.util.*

typealias GameId = UUID
typealias Timestamp = Date
typealias PlayerId = UUID

class Word(value: String) {

    companion object {
        const val VALID_WORD_REGEX = """^([A-Za-z]|\s)+\$"""
        const val MINIMUM_NUMBER_OF_CHARACTERS = 4
        const val MAXIMUM_NUMBER_OF_CHARACTERS = 10
    }

    val value: String

    init {
        require(value.length in MINIMUM_NUMBER_OF_CHARACTERS..MAXIMUM_NUMBER_OF_CHARACTERS) {
            "A word must be between $MINIMUM_NUMBER_OF_CHARACTERS and $MAXIMUM_NUMBER_OF_CHARACTERS characters long (\"$value\" doesn't fulfill this criteria)"
        }
        require(value.trim() == value) {
            "Word cannot start or end with whitespace (\"$value\")"
        }
        require(!value.contains("  ")) {
            "Word cannot contain two consecutive whitespaces (\"$value\")"
        }
        require(!value.matches(Regex(VALID_WORD_REGEX))) {
            "Word can only contain alphabetic characters and whitespace, was \"$value\"."
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

data class WordsToChooseFrom(val category: WordCategory, val words: List<Word>) : Sequence<Word> {
    init {
        require(words.size in 5..20) {
            "You need to supply between 5 to 20 words in the $category category"
        }
    }

    override fun iterator(): Iterator<Word> = words.iterator()
}