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

package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.BOUND_SECONDS;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.X;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.daemonExecutor;

/**
 * x counts as resumed by a {@code start(true)} that owes it the resume, from its turn on. A {@code resumeSubscription(x)}
 * on the dispatcher from another thread while that resume still waits for its read therefore throws and changes nothing,
 * and x is resumed once, by the start. After a {@code stop()} the start no longer owns x, so the same resume resumes it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupSubscriptionModelResumeWhileAResumeIsOwedTest {

    private static final Duration LONG_ENOUGH_FOR_A_RESUME_TO_ACT = Duration.ofSeconds(2);

    private final OwedResumeDispatcherFixture fixture = new OwedResumeDispatcherFixture();

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        fixture.releaseTheReadAndShutDown();
    }

    @Test
    void a_resume_of_x_from_another_thread_while_the_start_still_waits_for_the_read_throws_and_changes_nothing_and_x_is_resumed_once_by_the_start() throws Exception {
        // Given x is paused, and a start owes it the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> null));
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When x is resumed from another thread while the read is held
        CompletableFuture<Void> resume = CompletableFuture.runAsync(() -> fixture.model.resumeSubscription(X), daemonExecutor("resume-of-x"));

        // Then it throws saying x is running, and x stays paused and gets nothing
        assertThat(resume).as("the resume of x while the start owes it the resume").failsWithin(BOUND_SECONDS, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(SubscriptionAlreadyRunningException.class);
        fixture.publish("live-1");
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(fixture.model.isPaused(X)).as("whether x is paused while the read is held").isTrue();
            assertThat(fixture.delivered).as("the events delivered to x while the read is held").isEmpty();
        });

        // And once the read answers, the start resumes x, which gets an event once
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(fixture.model.isRunning(X)).as("whether x runs once the start resumed it").isTrue();
        fixture.publish("live-2");
        // The live model holds back what it was given while x was paused, and gives it to x when x resumes
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.delivered).as("the events delivered to x").containsExactly("live-1", "live-2"));
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.delivered).as("the events delivered to x, which is resumed once").containsExactly("live-1", "live-2"));
    }

    @Test
    void a_resume_of_x_made_after_a_stop_that_came_after_the_turn_does_not_throw_and_resumes_x() throws Exception {
        // Given x is paused, and a start owes it the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> null));
        fixture.aStartThatOwesTheResumeOfX();

        // When the model is stopped after the turn, and x is resumed from another thread
        CompletableFuture.runAsync(fixture.model::stop, daemonExecutor("stop-of-the-model")).get(BOUND_SECONDS, TimeUnit.SECONDS);
        CompletableFuture<Void> resume = CompletableFuture.runAsync(() -> fixture.model.resumeSubscription(X), daemonExecutor("resume-of-x"));

        // Then the resume doesn't throw and x runs and gets an event
        assertThat(resume).as("the resume of x made after the stop").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(fixture.model.isRunning(X)).as("whether x runs after its resume made after the stop").isTrue();
        fixture.publish("live-1");
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.delivered).as("the events delivered to x").contains("live-1"));
    }
}
