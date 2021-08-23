/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER
import org.junit.jupiter.params.provider.ValueSource


@DisplayName("NumberOfRounds")
class MaxNumberOfRoundsTest {

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = [1, 3, 5])
    fun `Number of rounds can only be`(value: Int) {
        MaxNumberOfRounds(value)
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = [2, 4])
    fun `Number of rounds cannot be even`(value: Int) {
        assertInvalidValue { MaxNumberOfRounds(value) }
    }

    @Test
    fun `Number of rounds cannot be 0`() {
        assertInvalidValue { MaxNumberOfRounds(0) }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = [Int.MIN_VALUE, -1])
    fun `Number of rounds cannot be negative`(value: Int) {
        assertInvalidValue { MaxNumberOfRounds(value) }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(ints = [Int.MAX_VALUE, 6])
    fun `Number of rounds cannot be more than 5`(value: Int) {
        assertInvalidValue { MaxNumberOfRounds(value) }
    }

    private fun assertInvalidValue(fn: () -> MaxNumberOfRounds) {
        val throwable = catchThrowable { fn() }
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Number of rounds can only be 1, 3 or 5")
    }
}