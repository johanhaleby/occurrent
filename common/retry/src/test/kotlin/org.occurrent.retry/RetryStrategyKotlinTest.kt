/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.retry

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class RetryStrategyKotlinTest {

    @Timeout(2000)
    @Test
    fun `example of retry executed with retry info`() {
        // Given
        val retryInfos = CopyOnWriteArrayList<RetryInfo>()
        val retryStrategy = RetryStrategy.retry().maxAttempts(4)
        val counter = AtomicInteger(0)

        // When
        val myString: String = retryStrategy.execute { retryInfo ->
            retryInfos.add(retryInfo)
            if (counter.incrementAndGet() == 3) {
                "String"
            } else {
                throw IllegalArgumentException("expected")
            }
        }

        // Then
        assertAll(
            { assertThat(myString).isEqualTo("String") },
            { assertThat(retryInfos).hasSize(3) },
            { assertThat(counter).hasValue(3) },
        )
    }

    @Timeout(2000)
    @Test
    fun `example of retry exec`() {
        // Given
        val retryStrategy = RetryStrategy.retry().maxAttempts(4)
        val counter = AtomicInteger(0)

        // When
        val myString: String = retryStrategy.exec {
            if (counter.incrementAndGet() == 3) {
                "String"
            } else {
                throw IllegalArgumentException("expected")
            }
        }

        // Then
        assertAll(
            { assertThat(myString).isEqualTo("String") },
            { assertThat(counter).hasValue(3) },
        )
    }
}