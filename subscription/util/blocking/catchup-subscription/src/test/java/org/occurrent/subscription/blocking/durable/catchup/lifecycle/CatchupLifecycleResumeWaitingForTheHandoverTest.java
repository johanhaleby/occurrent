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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.lifecycle.GatedHistory.cloudEvent;

/**
 * A resume that waits for the handover lock of a replay whose subscription was paused while the replay still read the
 * history takes effect after the handover, as the serial order subscribe, pause, resume says. The handover applies the pause
 * then, so the resume finds the subscription paused and resumes it, and it neither throws nor ends with it running and
 * the pause lost.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CatchupLifecycleResumeWaitingForTheHandoverTest {

    private static final String A = "a";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final ControllableLiveModel live = new ControllableLiveModel();
    private final GatedHistory history = new GatedHistory(cloudEvent("1"));
    private final CatchupSubscriptionModel model = new CatchupSubscriptionModel(live, history);
    private final CountDownLatch leaveTheAction = new CountDownLatch(1);
    private final CountDownLatch leaveTheHandover = new CountDownLatch(1);

    @AfterEach
    void release_everything_and_shut_down() throws Exception {
        leaveTheAction.countDown();
        leaveTheHandover.countDown();
        CompletableFuture.runAsync(model::shutdown).get(10, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_that_waits_for_the_handover_lock_of_a_replay_paused_while_it_read_the_history_resumes_the_subscription_the_handover_paused() throws Exception {
        // Given a replay of a that is held in its action, and a pause of a made while it reads the history
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch inTheHandover = new CountDownLatch(1);
        live.beforeSubscribe = id -> {
            if (A.equals(id)) {
                inTheHandover.countDown();
                awaitLatch(leaveTheHandover);
            }
        };
        model.subscribe(A, StartAtTime.beginningOfTime(), event -> {
            inTheAction.countDown();
            awaitLatch(leaveTheAction);
        });
        assertThat(inTheAction.await(10, TimeUnit.SECONDS)).as("whether the replay of a reached its action").isTrue();
        model.pauseSubscription(A);

        // And the replay is in the handover, blocked in the live model's subscribe under the handover lock of a
        leaveTheAction.countDown();
        assertThat(inTheHandover.await(10, TimeUnit.SECONDS)).as("whether the replay of a reached the subscribe of the live model").isTrue();

        // And a resume of a made from another thread waits for that lock
        AtomicReference<@Nullable Thread> resumingThread = new AtomicReference<>();
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> {
            resumingThread.set(Thread.currentThread());
            return model.resumeSubscription(A);
        });
        await().atMost(WAIT).until(() -> resumingThread.get() != null && Arrays.stream(resumingThread.get().getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("lockHandover")));

        // When the handover goes on
        leaveTheHandover.countDown();

        // Then the outcome is that of subscribe, pause and resume in that order, so the resume returns, its handle starts, and a runs and is not paused
        AtomicReference<@Nullable Subscription> handleOfTheResume = new AtomicReference<>();
        assertThat(catchThrowable(() -> handleOfTheResume.set(resume.get(10, TimeUnit.SECONDS)))).as("what resumeSubscription(a) threw").isNull();
        assertThat(handleOfTheResume.get().waitUntilStarted(WAIT)).as("whether the handle of resumeSubscription(a) answers started").isTrue();
        assertThat(model.isPaused(A)).as("whether a is paused after the subscribe, the pause and the resume ran in that order").isFalse();
        assertThat(model.isRunning(A)).as("whether a runs after the subscribe, the pause and the resume ran in that order").isTrue();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
