package org.occurrent.example.domain.wordguessinggame.writemodel

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.wordguessinggame.support.wordsOf

class WordsToChooseFromTest {

    @Test
    fun `throws iae when list contains duplicate words`() {
        // Given
        val words = wordsOf("Hello", "shark", "HELLO", "hellO", "apple")

        // When
        val throwable = catchThrowable { WordsToChooseFrom(WordCategory("category"), words) }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Duplicate words in the same category is not allowed: Hello, HELLO, hellO")
    }
}