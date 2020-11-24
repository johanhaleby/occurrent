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
    fun words(): Arbitrary<String> = RandomValidWordProvider.provideValidRandomWords(allowDash = false)
}