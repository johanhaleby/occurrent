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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Deterministic unit tests for {@link CatchupSubscription#waitUntilStarted(Duration)}, driving the
 * {@link Future} directly rather than through a real replay, since what matters here is exactly how each way the
 * future can resolve is translated into this class's own answer.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionTest {

    @Test
    void returns_the_delegates_own_answer_rather_than_a_hardcoded_true() {
        // The delegate itself reports false, for example a CancelledSubscription or another subscription whose own
        // start did not complete. The old behaviour hardcoded true once the future resolved at all, and this returns
        // whatever the delegate says.
        Future<Subscription> future = CompletableFuture.completedFuture(new FixedAnswerSubscription("delegated", false));

        boolean started = new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5));

        assertThat(started).isFalse();
    }

    @Test
    void returns_true_when_the_delegate_answers_true() {
        Future<Subscription> future = CompletableFuture.completedFuture(new FixedAnswerSubscription("delegated", true));

        boolean started = new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5));

        assertThat(started).isTrue();
    }

    @Test
    void a_future_that_does_not_resolve_within_the_timeout_answers_false() {
        Future<Subscription> future = new CompletableFuture<>(); // Never completes.

        boolean started = new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofMillis(50));

        assertThat(started).isFalse();
    }

    @Test
    void a_cancelled_replay_answers_false_rather_than_throwing() {
        // Same answer as CancelledSubscription. The replay was cancelled, so it never started and nothing will
        // start it, which is not a failure to report. CancellationException is a RuntimeException, so without an
        // explicit catch it would otherwise escape as a thrown exception instead of a false answer.
        CompletableFuture<Subscription> future = new CompletableFuture<>();
        future.cancel(false);

        boolean started = new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5));

        assertThat(started).isFalse();
    }

    @Test
    void a_runtime_exception_from_a_failed_replay_is_rethrown_as_is() {
        IllegalStateException replayFailure = new IllegalStateException("replay blew up");
        CompletableFuture<Subscription> future = new CompletableFuture<>();
        future.completeExceptionally(replayFailure);

        Throwable thrown = catchThrowable(() -> new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5)));

        assertThat(thrown).isSameAs(replayFailure);
    }

    @Test
    void an_error_from_a_failed_replay_is_rethrown_as_is() {
        OutOfMemoryError replayFailure = new OutOfMemoryError("replay blew up");
        CompletableFuture<Subscription> future = new CompletableFuture<>();
        future.completeExceptionally(replayFailure);

        Throwable thrown = catchThrowable(() -> new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5)));

        assertThat(thrown).isSameAs(replayFailure);
    }

    @Test
    void a_checked_exception_from_a_failed_replay_is_wrapped_so_a_caller_that_discards_the_return_value_still_learns_about_it() {
        Exception checkedFailure = new java.io.IOException("could not read the stream");
        CompletableFuture<Subscription> future = new CompletableFuture<>();
        future.completeExceptionally(checkedFailure);

        Throwable thrown = catchThrowable(() -> new CatchupSubscription("sub", future).waitUntilStarted(Duration.ofSeconds(5)));

        assertThat(thrown)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The catch-up for subscription 'sub' failed")
                .cause().isSameAs(checkedFailure);
    }

    // Stands for the Subscription a real replay hands over to once it completes (for example the wrapped model's own
    // subscription, or a CancelledSubscription): CatchupSubscription.waitUntilStarted must forward this answer
    // rather than assume it.
    private record FixedAnswerSubscription(String id, boolean answer) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return answer;
        }
    }
}
