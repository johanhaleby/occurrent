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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.BOUND_SECONDS;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.X;
import static org.occurrent.subscription.blocking.durable.catchup.OwedResumeStreamFixture.onAnOwedResumeThread;

/**
 * Code a user supplies for x, which the resume a {@code start(true)} owes x calls on the thread of that resume, can pause
 * or cancel x there, since claims are reentrant. The owed resume then changes nothing more, so it neither resumes x nor
 * runs the replay of x. Each test makes the callback act on its first ask made on a thread of an owed resume, so what it
 * does is done inside the owed resume and not in the reads before the turn.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupCallbackPauseDuringAnOwedResumeTest {

    private static final Duration LONG_ENOUGH_FOR_A_RESUME_TO_ACT = Duration.ofSeconds(2);

    private final OwedResumeStreamFixture fixture = new OwedResumeStreamFixture();
    private final AtomicBoolean callbackRan = new AtomicBoolean();
    private final AtomicReference<Throwable> callbackThrew = new AtomicReference<>();
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void use_the_model_of_the_fixture() {
        model = fixture.model;
    }

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        fixture.releaseTheReadAndShutDown();
    }

    @Test
    void a_pause_made_by_the_function_of_x_during_the_owed_resume_keeps_the_resume_from_resuming_x_when_the_function_asks_for_no_replay() throws Exception {
        // Given x is paused, its function pauses it on its first ask during the owed resume and asks for no replay, and a start owes x the resume
        fixture.givenAPausedX(fixture.startAtThatRunsOnTheFirstAskOfAnOwedResume(this::pauseX));
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run, which is when the start returns
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the pause returned normally, x is paused, and an event written afterwards doesn't reach it
        assertThatThePauseReturnedAndXStaysPausedWhenAnEventIsWritten();
        assertThat(fixture.wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);

        // And a resume of x makes it deliver
        model.resumeSubscription(X);
        String eventId = fixture.write();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x once its own resume ran").contains(eventId));
    }

    @Test
    void a_pause_made_by_the_function_of_x_during_the_owed_resume_keeps_the_resume_from_replaying_when_the_function_asks_for_a_replay() throws Exception {
        // Given x is paused with a stored position a replay resumes from, its function pauses it on its first ask during the owed resume and asks for a replay, and a start owes x the resume
        fixture.givenAPausedX(fixture.startAtThatRunsOnTheFirstAskOfAnOwedResume(this::pauseX));
        fixture.givenXWhoseResumeReplaysFromItsStoredPosition();
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the pause returned normally, x is paused, and nothing reaches it, not the replay and not an event written afterwards
        assertThatThePauseReturnedAndXStaysPausedWhenAnEventIsWritten();

        // And a resume of x makes it deliver
        model.resumeSubscription(X);
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x once its own resume ran").isNotEmpty());
    }

    @Test
    void a_pause_made_by_the_function_of_x_during_the_owed_resume_keeps_the_replay_from_starting_so_the_listener_of_x_is_not_told_and_nothing_is_parked() throws Exception {
        // Given x is paused with a stored position a replay resumes from, its function pauses it on its first ask during the owed resume and asks for a replay, its listener counts what it is told, and a start owes x the resume
        AtomicInteger catchupStarts = new AtomicInteger();
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                catchupStarts.incrementAndGet();
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        fixture.givenAPausedX(fixture.startAtThatRunsOnTheFirstAskOfAnOwedResume(this::pauseX));
        fixture.givenXWhoseResumeReplaysFromItsStoredPosition();
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then no replay was started, so the listener was not told and nothing is parked, and x is paused and gets nothing
        assertThat(catchupStarts).as("the times the listener of x was told that a replay starts").hasValue(0);
        assertThat(model.hasParkedReplay(X)).as("whether a replay of x is parked").isFalse();
        assertThatThePauseReturnedAndXStaysPausedWhenAnEventIsWritten();
        assertThat(catchupStarts).as("the times the listener of x was told that a replay starts, after the event was written").hasValue(0);

        // And a resume of x runs the replay of its history, once, and then x gets an event written after it
        model.resumeSubscription(X);
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x by the replay its own resume ran").hasSize(2));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.isRunning(X)).as("whether x runs once its replay ended").isTrue());
        String eventId = fixture.write();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x").containsOnlyOnce(eventId));
        assertThat(fixture.idsDeliveredToX).as("the events delivered to x").doesNotHaveDuplicates();
    }

    @Test
    void a_pause_made_by_the_listener_of_x_when_the_owed_resume_tells_it_that_the_replay_starts_parks_the_replay_instead_of_running_it() throws Exception {
        // Given x is paused with a stored position a replay resumes from, its listener pauses it on the owed resume's replay, and a start owes x the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> fixture.xResumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null));
        fixture.givenXWhoseResumeReplaysFromItsStoredPosition();
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (onAnOwedResumeThread() && callbackRan.compareAndSet(false, true)) {
                    pauseX();
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the replay is parked, x is paused, and nothing reaches it
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.hasParkedReplay(X)).as("whether the replay of x parked itself once its listener paused x").isTrue());
        assertThatThePauseReturnedAndXStaysPausedWhenAnEventIsWritten();

        // And a resume of x runs the parked replay
        model.resumeSubscription(X);
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.idsDeliveredToX).as("the events delivered to x once its own resume ran the parked replay").isNotEmpty());
    }

    @Test
    void a_cancel_made_by_the_listener_of_x_when_the_owed_resume_tells_it_that_the_replay_starts_drops_the_replay_instead_of_parking_or_running_it() throws Exception {
        // Given x is paused with a stored position a replay resumes from, its listener cancels it on the owed resume's replay, and a start owes x the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> fixture.xResumesFromTheBeginning.get() ? StartAtTime.beginningOfTime() : null));
        fixture.givenXWhoseResumeReplaysFromItsStoredPosition();
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (onAnOwedResumeThread() && callbackRan.compareAndSet(false, true)) {
                    try {
                        model.cancelSubscription(X);
                    } catch (Throwable e) {
                        callbackThrew.set(e);
                    }
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the cancel returned, the replay is neither parked nor run, and nothing reaches the action of x
        assertThat(callbackRan).as("whether the listener of x ran during the owed resume").isTrue();
        assertThat(callbackThrew.get()).as("what the cancel of x made by the listener threw").isNull();
        fixture.write();
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(model.hasParkedReplay(X)).as("whether the replay of the cancelled x is parked").isFalse();
            assertThat(fixture.idsDeliveredToX).as("the events delivered to the cancelled x").isEmpty();
        });
    }

    // Records what the pause threw
    private void pauseX() {
        callbackRan.set(true);
        try {
            model.pauseSubscription(X);
        } catch (Throwable e) {
            callbackThrew.set(e);
        }
    }

    private void assertThatThePauseReturnedAndXStaysPausedWhenAnEventIsWritten() {
        assertThat(callbackRan).as("whether the callback of x ran during the owed resume").isTrue();
        assertThat(callbackThrew.get()).as("what the pause of x made during the owed resume threw").isNull();
        fixture.write();
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(model.isPaused(X)).as("whether x is paused once the owed resume ran").isTrue();
            assertThat(model.isRunning(X)).as("whether x runs once the owed resume ran").isFalse();
            assertThat(fixture.idsDeliveredToX).as("the events delivered to x after the pause").isEmpty();
        });
    }
}
