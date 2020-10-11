package org.occurrent.example.domain.wordguessinggame.writemodel

import net.jqwik.api.Arbitrary
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.Provide
import org.assertj.core.api.Assertions.assertThat
import org.occurrent.example.domain.wordguessinggame.RandomValidWordProvider

internal class WordPropertyBasedTest {

    @Property
    fun `generates word from list of random valid words without throwing exception`(@ForAll("words") wordToGuess: String) {
        // When
        val word = Word(wordToGuess)

        // Then
        assertThat(word.hasValue(wordToGuess)).isTrue
        assertThat(word.value).isEqualTo(wordToGuess)
    }

    @Provide
    fun words(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords()
}