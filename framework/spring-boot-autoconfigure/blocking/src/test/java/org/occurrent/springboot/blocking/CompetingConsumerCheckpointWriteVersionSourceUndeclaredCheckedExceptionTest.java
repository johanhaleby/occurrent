/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.springboot.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.ObjectProvider;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A strategy provider written in Kotlin can throw a checked exception without declaring it. A resolution that throws
 * it counts as no version for that write, as one that throws a {@link RuntimeException} does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerCheckpointWriteVersionSourceUndeclaredCheckedExceptionTest {

    private static final String SUBSCRIPTION_ID = "sub-1";

    @Test
    @SuppressWarnings("unchecked")
    void a_resolution_that_throws_a_checked_exception_answers_empty_and_is_retried_on_the_next_write() {
        // Given
        CompetingConsumerStrategy strategy = mock(CompetingConsumerStrategy.class);
        when(strategy.fencingToken(SUBSCRIPTION_ID)).thenReturn(OptionalLong.of(9L));
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique())
                .thenAnswer(__ -> {
                    throw sneaky(new IOException("The strategy cannot be built right now"));
                })
                .thenReturn(strategy);
        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, () -> true);

        // When
        AtomicReference<OptionalLong> first = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> first.set(source.writeVersion(SUBSCRIPTION_ID)));
        OptionalLong second = source.writeVersion(SUBSCRIPTION_ID);

        // Then
        assertThat(thrown).as("what the write version threw while resolving the strategy throws").isNull();
        assertThat(first.get()).as("the write version while resolving the strategy throws").isEmpty();
        assertThat(second).as("the write version once the strategy resolves").isEqualTo(OptionalLong.of(9L));
    }

    @Test
    @SuppressWarnings("unchecked")
    void a_resolution_that_throws_an_InterruptedException_answers_empty_and_leaves_the_thread_interrupted() {
        // Given
        ObjectProvider<CompetingConsumerStrategy> provider = mock(ObjectProvider.class);
        when(provider.getIfUnique()).thenAnswer(__ -> {
            throw sneaky(new InterruptedException("The strategy was interrupted while it was built"));
        });
        var source = new CompetingConsumerCheckpointWriteVersionSource(provider, () -> true);

        // When
        AtomicReference<OptionalLong> version = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> version.set(source.writeVersion(SUBSCRIPTION_ID)));
        boolean interrupted = Thread.interrupted();

        // Then
        assertThat(thrown).as("what the write version threw while resolving the strategy").isNull();
        assertThat(version.get()).as("the write version while resolving the strategy throws").isEmpty();
        assertThat(interrupted).as("the thread is interrupted once the write version returned").isTrue();
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
        throw (E) t;
    }
}
