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
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.*
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
@Timeout(2000)
class RetryStrategyKotlinTest {

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

    @Test
    fun `retryAttemptException returns throwable if current retry attempt threw, otherwise null`() {
        // Given
        val retryStrategy = RetryStrategy.retry().maxAttempts(4)
        val throwables = CopyOnWriteArrayList<Throwable>()

        // When
        val myString: String = retryStrategy.onAfterRetry { _, info ->
            throwables.add(info.retryAttemptException)
        }.exec { info ->
            if (info.isLastAttempt) {
                "String"
            } else {
                throw IllegalArgumentException("expected")
            }
        }

        // Then
        assertAll(
            { assertThat(myString).isEqualTo("String") },
            { assertThat(throwables).hasSize(3) },
            { assertThat(throwables.count { it is Throwable }).isEqualTo(2) },
            { assertThat(throwables.count { it == null }).isEqualTo(1) },
        )
    }

    @Test
    fun `nextBackoff returns duration if current retry attempt failed, otherwise null`() {
        // Given
        val retryStrategy = RetryStrategy.retry().maxAttempts(4)
        val durations = CopyOnWriteArrayList<Duration>()

        // When
        catchThrowable {
            retryStrategy.onError { _, info ->
                durations.add(info.nextBackoff)
            }.exec {
                throw IllegalArgumentException("expected")
            }
        }

        // Then
        assertAll(
            { assertThat(durations).hasSize(4) },
            { assertThat(durations.count { it is Duration }).isEqualTo(3) },
            { assertThat(durations.count { it == null }).isEqualTo(1) },
        )
    }
}