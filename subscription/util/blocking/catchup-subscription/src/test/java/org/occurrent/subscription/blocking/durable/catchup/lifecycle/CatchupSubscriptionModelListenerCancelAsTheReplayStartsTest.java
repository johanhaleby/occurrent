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
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedCheckpoint;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.BOUND_SECONDS;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.X;

/**
 * A {@link CatchupListener} of x is told that a replay of x starts on the thread that launches the replay, which holds
 * the claim of x and the handover lock, both reentrant. A cancel of x made there through the dispatcher drops the replay
 * being launched, whether the listener then returns or throws, so the replay neither runs nor parks on any child, and a
 * later {@code start(true)} has nothing of x to run again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupSubscriptionModelListenerCancelAsTheReplayStartsTest {

    private static final Duration LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER = Duration.ofSeconds(2);
    // Before the time of every event of the history
    private static final String STORED_TIME = "2020-01-01T00:00:00Z";

    private final OwedResumeDispatcherFixture fixture = new OwedResumeDispatcherFixture();
    private final AtomicBoolean resumesFromTheBeginning = new AtomicBoolean();
    private final AtomicBoolean listenerRan = new AtomicBoolean();
    private final AtomicReference<Throwable> cancelThrew = new AtomicReference<>();

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        fixture.releaseTheReadAndShutDown();
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_that_then_throws_when_the_owed_resume_tells_it_that_the_replay_starts_drops_the_replay_so_a_later_start_runs_nothing() throws Exception {
        // Given x is paused and its resume replays, its listener cancels it on the owed resume's replay and then throws, and a start owes x the resume
        givenAPausedXWhoseResumeReplays();
        listenAndOnTheFirstTellWhere(OwedResumeDispatcherFixture::onAnOwedResumeThread, true, true);
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the cancel returned, and the replay is neither parked nor run
        assertThatTheCancelReturned();
        assertThatXIsGoneAndGetsNothing("live-1");

        // And a later start that resumes subscriptions runs no replay of x
        fixture.model.start(true);
        assertThatXIsGoneAndGetsNothing("live-2");
    }

    @Test
    void a_listener_of_x_that_throws_without_a_cancel_when_the_owed_resume_tells_it_that_the_replay_starts_parks_the_replay_and_a_later_start_runs_it() throws Exception {
        // Given x is paused and its resume replays, its listener throws on the owed resume's replay, and a start owes x the resume
        givenAPausedXWhoseResumeReplays();
        listenAndOnTheFirstTellWhere(OwedResumeDispatcherFixture::onAnOwedResumeThread, false, true);
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the replay is parked and nothing reaches x
        assertThat(listenerRan).as("whether the listener of x ran during the owed resume").isTrue();
        assertThat(fixture.hasParkedReplay(X)).as("whether the replay of x is parked once its listener threw").isTrue();
        assertThat(fixture.delivered).as("the events delivered to x while its replay is parked").isEmpty();

        // And a later start that resumes subscriptions runs the parked replay
        fixture.model.start(true);
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.delivered).as("the events the parked replay delivered to x").containsExactly("1"));
        assertThat(fixture.hasParkedReplay(X)).as("whether the replay of x is still parked once the start ran it").isFalse();
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_when_a_resume_of_x_tells_it_that_the_replay_starts_drops_the_replay_instead_of_running_it() throws Exception {
        // Given x is paused and its resume replays, and its listener cancels it on the resuming thread and then returns
        givenAPausedXWhoseResumeReplays();
        Thread resuming = Thread.currentThread();
        listenAndOnTheFirstTellWhere(() -> Thread.currentThread() == resuming, true, false);

        // When x is resumed
        Subscription resumed = fixture.model.resumeSubscription(X);

        // Then the cancel returned, the replay is neither parked nor run, and the handle of the resume never starts
        assertThatTheCancelReturned();
        assertThat(resumed.waitUntilStarted(Duration.ofMillis(500))).as("whether the handle the resume of the cancelled x returned started").isFalse();
        assertThatXIsGoneAndGetsNothing("live-1");
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_that_then_throws_when_a_resume_of_x_tells_it_that_the_replay_starts_drops_the_replay_so_a_later_start_runs_nothing() throws Exception {
        // Given x is paused and its resume replays, and its listener cancels it on the resuming thread and then throws
        givenAPausedXWhoseResumeReplays();
        Thread resuming = Thread.currentThread();
        listenAndOnTheFirstTellWhere(() -> Thread.currentThread() == resuming, true, true);

        // When x is resumed
        Subscription resumed = fixture.model.resumeSubscription(X);

        // Then the cancel returned, the replay is neither parked nor run, and the handle of the resume never starts
        assertThatTheCancelReturned();
        assertThat(resumed.waitUntilStarted(Duration.ofMillis(500))).as("whether the handle the resume of the cancelled x returned started").isFalse();
        assertThatXIsGoneAndGetsNothing("live-1");

        // And a later start that resumes subscriptions runs no replay of x
        fixture.model.start(true);
        assertThatXIsGoneAndGetsNothing("live-2");
    }

    // X starts live, so it gets no history when it is subscribed. A resume of it replays the history from its stored time
    private void givenAPausedXWhoseResumeReplays() {
        fixture.givenAPausedX(StartAt.dynamic(() -> resumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null));
        fixture.storage.save(X, CatchupTimeCheckpoint.of(STORED_TIME, TimeBasedCheckpoint.beginningOfTime(), STORED_TIME));
        resumesFromTheBeginning.set(true);
    }

    // Acts on the first tell made where acts answers true, and records what the cancel threw
    private void listenAndOnTheFirstTellWhere(BooleanSupplier acts, boolean cancels, boolean thenThrows) {
        fixture.model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (acts.getAsBoolean() && listenerRan.compareAndSet(false, true)) {
                    if (cancels) {
                        try {
                            fixture.model.cancelSubscription(X);
                        } catch (Throwable e) {
                            cancelThrew.set(e);
                        }
                    }
                    if (thenThrows) {
                        throw new IllegalStateException("The listener of x failed");
                    }
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
    }

    private void assertThatTheCancelReturned() {
        assertThat(listenerRan).as("whether the listener of x ran as the replay started").isTrue();
        assertThat(cancelThrew.get()).as("what the cancel of x made by the listener threw").isNull();
    }

    private void assertThatXIsGoneAndGetsNothing(String liveEventId) {
        fixture.publish(liveEventId);
        await().during(LONG_ENOUGH_FOR_A_REPLAY_TO_DELIVER).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(fixture.hasParkedReplay(X)).as("whether a replay of the cancelled x is parked").isFalse();
            assertThat(fixture.model.isPaused(X)).as("whether the cancelled x reads as paused").isFalse();
            assertThat(fixture.model.isRunning(X)).as("whether the cancelled x reads as running").isFalse();
            assertThat(fixture.delivered).as("the events delivered to the cancelled x").isEmpty();
        });
    }
}
