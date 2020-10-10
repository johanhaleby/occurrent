package org.occurrent.example.domain.wordguessinggame.readmodel.ongoing

import net.jqwik.api.Assume
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.constraints.CharRange
import net.jqwik.api.constraints.CharRangeList
import net.jqwik.api.constraints.Chars
import net.jqwik.api.constraints.StringLength
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertAll
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.generateNewHint
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.obfuscationCharacter
import org.occurrent.example.domain.wordguessinggame.readmodel.ongoing.WordHintGenerator.whitespace

@DisplayName("word hint generator")
internal class WordHintGeneratorPropertyBasedTest {

    @Property
    fun `generate new hint conforms to the business rules`(@CharRangeList(CharRange(from = 'a', to = 'z'), CharRange(from = 'A', to = 'Z'))
                                                           @Chars(' ') @StringLength(min = 4, max = 10) @ForAll wordToGuess: String) {
        // Given
        Assume.that(!wordToGuess.endsWith(' ')) // Word cannot end with whitespace
        Assume.that(!wordToGuess.startsWith(' ')) // Word cannot start with whitespace
        Assume.that(!wordToGuess.contains("  ")) // No consecutive whitespaces

        // When
        val hint = wordToGuess.generateNewHint()

        // Then
        assertAll(
                { assertThat(hint).hasSameSizeAs(wordToGuess) },
                { assertThat(hint).containsPattern("""^([A-Za-z]|[0-9]|_|\s)+""") },
                { assertThat(hint.count { it != obfuscationCharacter } - hint.count { it == whitespace }).isIn(1, 2) },
        )
    }
}