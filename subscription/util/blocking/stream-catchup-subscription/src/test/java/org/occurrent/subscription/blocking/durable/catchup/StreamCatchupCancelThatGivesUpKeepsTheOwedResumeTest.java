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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.BOUND_SECONDS;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.X;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.daemonExecutor;

/**
 * A cancel of x made after the turn of a {@code start(true)} that owes x its resume, and that gives up after the wait for
 * the claim of x because another call holds it, changed nothing. It must keep the owed resume, so that x is resumed by
 * the start once the read answers, as it is when the start comes before a cancel that changed nothing. The call that
 * holds the claim is a pause of x the live delegate holds up and then fails, which changes nothing either.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupCancelThatGivesUpKeepsTheOwedResumeTest {

    private final OwedResumeStreamFixture fixture = new OwedResumeStreamFixture();
    private final CountDownLatch pauseHoldsTheClaim = new CountDownLatch(1);
    private final CountDownLatch pauseMayFail = new CountDownLatch(1);
    private final AtomicBoolean heldOnce = new AtomicBoolean();

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        pauseMayFail.countDown();
        fixture.releaseTheReadAndShutDown();
    }

    @Test
    void a_cancel_whose_live_delegate_cancel_throws_keeps_the_resume_the_start_owes_x_for_the_record_the_cancel_kept() throws Exception {
        // Given x is paused, a start owes it the resume, and the live delegate fails the first cancel of x
        fixture.givenAPausedX(StartAt.dynamic(() -> null));
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();
        IllegalStateException refused = new IllegalStateException("the live delegate failed the cancel of x on purpose");
        AtomicBoolean failedOnce = new AtomicBoolean();
        fixture.wrapped.beforeCancel = id -> {
            if (X.equals(id) && failedOnce.compareAndSet(false, true)) {
                throw refused;
            }
        };

        // When x is cancelled while the read is held
        assertThatThrownBy(() -> fixture.model.cancelSubscription(X)).as("what the cancel of x threw while the live delegate failed it").isSameAs(refused);
        assertThat(failedOnce).as("whether the cancel reached the live delegate").isTrue();

        // Then once the read answers, the start resumes x, which gets an event once
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(fixture.model.isRunning(X)).as("whether x runs, resumed by the start the cancel that changed nothing came after").isTrue();
        String eventId = fixture.write();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x").containsExactly(eventId));
        await().during(Duration.ofSeconds(2)).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x, which is resumed once").containsExactly(eventId));
    }

    @Test
    void a_cancel_that_gives_up_after_the_wait_for_the_claim_of_x_leaves_the_resume_the_start_owes_x_to_run_once_the_read_answers() throws Exception {
        // Given x is paused, and a start owes it the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> null));
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // And a pause of x holds the claim of x in the live delegate, and then fails with something other than not running
        fixture.wrapped.beforePause = id -> {
            if (heldOnce.compareAndSet(false, true)) {
                pauseHoldsTheClaim.countDown();
                HookedSubscriptionModel.awaitLatch(pauseMayFail);
                throw new IllegalStateException("the live delegate failed the pause of x on purpose");
            }
        };
        CompletableFuture<Void> pause = CompletableFuture.runAsync(() -> fixture.model.pauseSubscription(X), daemonExecutor("pause-of-x"));
        assertThat(pauseHoldsTheClaim.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the pause of x holds the claim of x").isTrue();

        // When the cancel of x waits for that claim and gives up
        CompletableFuture<Void> cancel = CompletableFuture.runAsync(() -> fixture.model.cancelSubscription(X), daemonExecutor("cancel-of-x"));

        // Then it throws saying it changed nothing
        assertThat(cancel).as("the cancel of x that waited for a claim that was not free").failsWithin(BOUND_SECONDS, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(IllegalStateException.class)
                .withMessageStartingWith("Cannot change subscription " + X)
                .withMessageEndingWith("Nothing was changed.");

        // And once the pause has failed and the read answers, the start resumes x, which then gets an event
        pauseMayFail.countDown();
        assertThat(pause).as("the pause of x the live delegate failed").failsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(fixture.model.isRunning(X)).as("whether x runs, resumed by the start the cancel that changed nothing came after").isTrue();
        String eventId = fixture.write();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x").containsExactly(eventId));
    }
}
