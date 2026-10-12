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

package org.occurrent.subscription.blocking.durable.catchup;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link AbstractCatchupSubscriptionModel#runEach} and {@link AbstractCatchupSubscriptionModel#resumeEach} run every
 * step, whatever an earlier step throws, and throw the first {@link Error} when a step threw one, else the first
 * failure, with the others added as suppressed. A failure several steps threw is thrown as itself and is not added to
 * itself.
 */
@DisplayName("AbstractCatchupSubscriptionModel runEach")
@DisplayNameGeneration(ReplaceUnderscores.class)
class RunEachTest {

    @Test
    void runs_the_later_steps_when_a_step_throws_an_error() {
        // Given
        List<String> ran = new ArrayList<>();
        Consumer<String> step = item -> {
            ran.add(item);
            if (item.equals("a")) {
                throw new AssertionError("a broke");
            }
        };

        // When
        catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c"), step));

        // Then
        assertThat(ran).containsExactly("a", "b", "c");
    }

    @Test
    void adds_the_failures_of_the_later_steps_as_suppressed_to_the_first_error() {
        // Given
        AssertionError error = new AssertionError("a broke");
        IllegalStateException laterRuntimeFailure = new IllegalStateException("b broke");
        StackOverflowError laterError = new StackOverflowError("c broke");
        Consumer<String> step = item -> {
            switch (item) {
                case "a" -> throw error;
                case "b" -> throw laterRuntimeFailure;
                case "c" -> throw laterError;
                default -> {
                }
            }
        };

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c", "d"), step));

        // Then
        assertThat(thrown).isSameAs(error);
        assertThat(thrown.getSuppressed()).containsExactly(laterRuntimeFailure, laterError);
    }

    @Test
    void throws_a_later_error_with_the_earlier_runtime_exception_added_as_suppressed() {
        // Given
        IllegalStateException earlierRuntimeFailure = new IllegalStateException("a broke");
        AssertionError laterError = new AssertionError("b broke");
        List<String> ran = new ArrayList<>();
        Consumer<String> step = item -> {
            ran.add(item);
            switch (item) {
                case "a" -> throw earlierRuntimeFailure;
                case "b" -> throw laterError;
                default -> {
                }
            }
        };

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c"), step));

        // Then
        assertThat(thrown).isSameAs(laterError);
        assertThat(thrown.getSuppressed()).containsExactly(earlierRuntimeFailure);
        assertThat(ran).containsExactly("a", "b", "c");
    }

    @Test
    void throws_the_first_runtime_exception_with_the_later_runtime_exceptions_added_as_suppressed_when_no_step_throws_an_error() {
        // Given
        IllegalStateException first = new IllegalStateException("a broke");
        IllegalArgumentException later = new IllegalArgumentException("b broke");

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c"), item -> {
            switch (item) {
                case "a" -> throw first;
                case "b" -> throw later;
                default -> {
                }
            }
        }));

        // Then
        assertThat(thrown).isSameAs(first);
        assertThat(thrown.getSuppressed()).containsExactly(later);
    }

    @Test
    void throws_an_error_that_two_steps_threw_as_itself_and_does_not_add_it_to_itself() {
        // Given
        OutOfMemoryError preallocated = new OutOfMemoryError("Java heap space");
        List<String> ran = new ArrayList<>();
        Consumer<String> step = item -> {
            ran.add(item);
            if (!item.equals("c")) {
                throw preallocated;
            }
        };

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c"), step));

        // Then
        assertThat(thrown).isSameAs(preallocated);
        assertThat(thrown.getSuppressed()).doesNotContain(preallocated);
        assertThat(ran).containsExactly("a", "b", "c");
    }

    @Test
    void throws_an_error_that_two_steps_threw_with_the_earlier_runtime_exception_as_its_only_suppressed() {
        // Given
        IllegalStateException earlierRuntimeFailure = new IllegalStateException("a broke");
        OutOfMemoryError preallocated = new OutOfMemoryError("Java heap space");

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b", "c"), item -> {
            if (item.equals("a")) {
                throw earlierRuntimeFailure;
            }
            throw preallocated;
        }));

        // Then
        assertThat(thrown).isSameAs(preallocated);
        assertThat(thrown.getSuppressed()).containsExactly(earlierRuntimeFailure);
    }

    @Test
    void throws_a_runtime_exception_that_two_steps_threw_as_itself_and_does_not_add_it_to_itself() {
        // Given
        IllegalStateException failure = new IllegalStateException("broke");

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.runEach(List.of("a", "b"), item -> {
            throw failure;
        }));

        // Then
        assertThat(thrown).isSameAs(failure);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    @Test
    void resumes_the_later_subscriptions_when_resuming_one_throws_an_error() {
        // Given
        Set<String> ids = new LinkedHashSet<>(List.of("s1", "s2", "s3"));
        List<String> resumed = new ArrayList<>();
        AssertionError error = new AssertionError("s1 broke");

        // When
        Throwable thrown = catchThrowable(() -> AbstractCatchupSubscriptionModel.resumeEach(ids, id -> {
            if (id.equals("s1")) {
                throw error;
            }
            resumed.add(id);
        }));

        // Then
        assertThat(thrown).isSameAs(error);
        assertThat(resumed).containsExactly("s2", "s3");
    }
}
