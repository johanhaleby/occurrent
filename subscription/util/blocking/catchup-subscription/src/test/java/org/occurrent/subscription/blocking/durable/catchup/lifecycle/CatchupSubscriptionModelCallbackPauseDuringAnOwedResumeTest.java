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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.BOUND_SECONDS;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.X;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.OwedResumeDispatcherFixture.onAnOwedResumeThread;

/**
 * A {@code StartAt.dynamic(..)} function of x that the resume a {@code start(true)} owes x asks on its own thread can
 * pause x there, since claims are reentrant. The dispatcher's owed resume then neither replays nor resumes x.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class CatchupSubscriptionModelCallbackPauseDuringAnOwedResumeTest {

    private static final Duration LONG_ENOUGH_FOR_A_RESUME_TO_ACT = Duration.ofSeconds(2);

    private final OwedResumeDispatcherFixture fixture = new OwedResumeDispatcherFixture();
    private final AtomicBoolean functionPaused = new AtomicBoolean();
    private final AtomicReference<Throwable> pauseThrew = new AtomicReference<>();

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        fixture.releaseTheReadAndShutDown();
    }

    @Test
    void a_pause_made_by_the_function_of_x_during_the_owed_resume_keeps_the_resume_from_resuming_x() throws Exception {
        // Given x is paused, its function pauses it on its first ask during the owed resume and asks for no replay, and a start owes x the resume
        fixture.givenAPausedX(StartAt.dynamic(() -> {
            if (onAnOwedResumeThread() && functionPaused.compareAndSet(false, true)) {
                try {
                    fixture.model.pauseSubscription(X);
                } catch (Throwable e) {
                    pauseThrew.set(e);
                }
            }
            return null;
        }));
        CompletableFuture<Void> start = fixture.aStartThatOwesTheResumeOfX();

        // When the read answers and the owed resume has run, which is when the start returns
        fixture.storage.release.countDown();
        assertThat(start).as("the start once its owed resume of x has run").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the pause returned normally, x is paused, and an event published afterwards doesn't reach it
        assertThat(functionPaused).as("whether the function of x paused it during the owed resume").isTrue();
        assertThat(pauseThrew.get()).as("what the pause of x made during the owed resume threw").isNull();
        fixture.publish("live-1");
        await().during(LONG_ENOUGH_FOR_A_RESUME_TO_ACT).atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(fixture.model.isPaused(X)).as("whether x is paused once the owed resume ran").isTrue();
            assertThat(fixture.model.isRunning(X)).as("whether x runs once the owed resume ran").isFalse();
            assertThat(fixture.delivered).as("the events delivered to x after the pause").isEmpty();
        });

        // And a resume of x makes it deliver
        fixture.model.resumeSubscription(X);
        fixture.publish("live-2");
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(fixture.delivered).as("the events delivered to x once its own resume ran").contains("live-2"));
    }
}
