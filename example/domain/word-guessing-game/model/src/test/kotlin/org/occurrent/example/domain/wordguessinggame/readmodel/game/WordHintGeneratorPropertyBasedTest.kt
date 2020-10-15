package org.occurrent.example.domain.wordguessinggame.readmodel.game

import net.jqwik.api.Arbitrary
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.Provide
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.assertAll
import org.occurrent.example.domain.wordguessinggame.RandomValidWordProvider
import org.occurrent.example.domain.wordguessinggame.readmodel.game.WordHintGenerator.generateNewHint
import org.occurrent.example.domain.wordguessinggame.readmodel.game.WordHintGenerator.obfuscationCharacter
import org.occurrent.example.domain.wordguessinggame.readmodel.game.WordHintGenerator.whitespace

internal class WordHintGeneratorPropertyBasedTest {

    @Property
    fun `generate new hint conforms to the business rules`(@ForAll("words") wordToGuess: String) {
        // When
        val hint = wordToGuess.generateNewHint()

        // Then
        assertAll(
                { assertThat(hint).hasSameSizeAs(wordToGuess) },
                { assertThat(hint).containsPattern("""^([A-Za-z]|[0-9]|_|\s)+""") },
                { assertThat(hint.count { it != obfuscationCharacter } - hint.count { it == whitespace }).isIn(1, 2) },
        )
    }

    @Provide
    fun words(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords()
}