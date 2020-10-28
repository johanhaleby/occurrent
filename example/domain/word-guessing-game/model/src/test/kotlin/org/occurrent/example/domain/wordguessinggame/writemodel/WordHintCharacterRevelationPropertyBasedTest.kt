package org.occurrent.example.domain.wordguessinggame.writemodel

import net.jqwik.api.Arbitrary
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.Provide
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertAll
import org.occurrent.example.domain.wordguessinggame.RandomValidWordProvider
import org.occurrent.example.domain.wordguessinggame.writemodel.WordHintCharacterRevelation.whitespace
import java.util.*

@DisplayName("word hint character revelation")
internal class WordHintCharacterRevelationPropertyBasedTest {


    // Initial characters
    @Property
    fun `reveal initial characters in word hint when game was started reveals two characters when word is longer than 3 and doesn't contain space`(@ForAll("wordsLongerThan3CharactersAndNoWhitespace") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        val (e1, e2) = events
        // Then
        assertAll(
                { assertThat(events).hasSize(2) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(whitespace) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },

                { assertThat(e2.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e2.character).isNotEqualTo(whitespace) },
                { assertThat(e2.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },

                { assertThat(e1.characterPositionInWord).isNotEqualTo(e2.characterPositionInWord) },
        )
    }

    @Property
    fun `reveal initial characters in word hint when game was started reveals one character that is not space when word has 3 characters without whitespace`(@ForAll("wordsHas3CharsAndNoWhitespace") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        val e1 = events[0]
        // Then
        assertAll(
                { assertThat(events).hasSize(1) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(whitespace) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },
        )
    }

    @Property
    fun `reveal initial characters in word hint when game was started reveals one character when word has 4 characters where one is whitespace`(@ForAll("wordsHas4CharactersAndWhitespace") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        val e1 = events[0]
        assertAll(
                { assertThat(events).hasSize(1) },

                { assertThat(e1.character).isIn(wordHintData.wordToGuess.toCharArray().asIterable()) },
                { assertThat(e1.character).isNotEqualTo(whitespace) },
                { assertThat(e1.characterPositionInWord - 1).isIn(wordHintData.wordToGuess.indices) },
        )

    }

    @Property
    fun `reveal initial characters in word hint when game was started doesn't reveal any character when word has 3 characters where one is whitespace`(@ForAll("wordsHas3CharsAndOneWhitespace") wordToGuess: String) {
        // Given
        val wordHintData = WordHintData(UUID.randomUUID(), wordToGuess)

        // When
        val events = WordHintCharacterRevelation.revealInitialCharactersInWordHintWhenGameWasStarted(wordHintData).toList()

        // Then
        assertThat(events).isEmpty()
    }

    @Provide
    fun wordsLongerThan3CharactersAndNoWhitespace(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 4..15, allowWhitespace = false)

    @Provide
    fun wordsHas4CharactersAndWhitespace(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 4..4, allowWhitespace = true)
            .filter { wordToGuess -> wordToGuess.count { char -> char == whitespace } == 1 }

    @Provide
    fun wordsHas3CharsAndOneWhitespace(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 3..3)
            .filter { wordToGuess -> wordToGuess.count { char -> char == whitespace } == 1 }

    @Provide
    fun wordsHas3CharsAndNoWhitespace(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(limitWordLength = 3..3, allowWhitespace = false)
}