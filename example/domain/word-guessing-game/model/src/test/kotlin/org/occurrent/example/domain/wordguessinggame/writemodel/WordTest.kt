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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class WordTest {

    @Test
    fun `doesn't need to contain dash`() {
        Word("snake")
    }

    @Test
    fun `may contain a dash`() {
        Word("sn-ake")
    }

    @Test
    fun `may contain multiple non-consecutive dashes`() {
        Word("sn-ak-e")
    }

    @Test
    fun `cannot contain digits`() {
        // When
        val throwable = catchThrowable { Word("snake123") }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Word can only contain alphabetic characters and dash (and it cannot start or end with dash), was \"snake123\".")
    }

    @Test
    fun `cannot contain consecutive dashes`() {
        // When
        val throwable = catchThrowable { Word("sn--ake") }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Word cannot contain two consecutive dashes (\"sn--ake\")")
    }

    @Test
    fun `cannot contain a whitespace`() {
        // When
        val throwable = catchThrowable { Word("Hello World") }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Word can only contain alphabetic characters and dash (and it cannot start or end with dash), was \"Hello World\".")
    }

    @Test
    fun `cannot start with with dash`() {
        // When
        val throwable = catchThrowable { Word("-HelloWorld") }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Word can only contain alphabetic characters and dash (and it cannot start or end with dash), was \"-HelloWorld\".")
    }

    @Test
    fun `cannot end with with dash`() {
        // When
        val throwable = catchThrowable { Word("HelloWorld-") }

        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Word can only contain alphabetic characters and dash (and it cannot start or end with dash), was \"HelloWorld-\".")
    }
}