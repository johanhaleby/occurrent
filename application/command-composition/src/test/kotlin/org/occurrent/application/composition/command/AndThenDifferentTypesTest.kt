/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.application.composition.command

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class AndThenDifferentTypesTest {

    @Test
    fun `andThenDifferentTypes works as expected`() {
        // Given
        val composed = ::function1 andThen ::function2
        // When
        val result = composed("hello")
        // Then
        assertThat(result).isEqualTo(5)
    }

    private fun function1(obj: Any): String = obj.toString()

    private fun function2(string: String): Int = string.length
}