package se.haleby.occurrent.example.domain.numberguessinggame;

import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import static org.assertj.core.api.Assertions.assertThat;

class SecretNumberToGuessRandomTest {

    @Property
    void secret_random_number_is_between_1_and_1000(@IntRange(min = 1, max = 999) @ForAll int minNumber, @IntRange(min = 2, max = 1000) @ForAll int maxNumber) {
        // Given
        Assume.that(minNumber < maxNumber);

        // When
        SecretNumberToGuess secretNumberToGuess = SecretNumberToGuess.randomBetween(minNumber, maxNumber);

        // Then
        System.out.println("min = " + minNumber + ", max = " + maxNumber + " => " + secretNumberToGuess.value);
        assertThat(secretNumberToGuess.value).isBetween(minNumber, maxNumber);
    }
}