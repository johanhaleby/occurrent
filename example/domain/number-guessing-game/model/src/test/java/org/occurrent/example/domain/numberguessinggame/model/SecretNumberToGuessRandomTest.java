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

package org.occurrent.example.domain.numberguessinggame.model;

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
        assertThat(secretNumberToGuess.value).isBetween(minNumber, maxNumber);
    }
}