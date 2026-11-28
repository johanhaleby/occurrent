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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A replay a resume started runs again after a backoff and tells its listener, on the replay thread, while it holds the
 * handover lock of its subscription. A {@code stop()} a live action handed over has set the stopped flag by then and waits
 * for that lock, so a {@code resumeSubscription(..)} the listener calls for a paused subscription finds the model stopped.
 * It must throw {@link IllegalStateException} as {@code start(boolean)} does and change nothing, since waiting for the stop
 * would wait for the lock its own thread holds.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeFromAListenerOnAStoppedModelTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private record Refusal(boolean stopSetTheFlag, Subscription returned, Throwable threw, boolean zPausedAfterwards, int resumesOfTheWrappedModel, int startsOfTheWrappedModel) {
    }

    private final CountDownLatch yHandedTheStop = new CountDownLatch(1);
    private final CountDownLatch releaseY = new CountDownLatch(1);
    private final CountDownLatch releaseX = new CountDownLatch(1);
    private final CountDownLatch inTheActionOfX = new CountDownLatch(1);
    private final AtomicInteger deliveriesToX = new AtomicInteger();
    private final AtomicBoolean actionOfXFailed = new AtomicBoolean();
    private final AtomicBoolean listenerActed = new AtomicBoolean();
    private final CompletableFuture<Refusal> refusal = new CompletableFuture<>();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel live;
    private StreamCatchupSubscriptionModel model;
    private Thread liveDelivery;

    @BeforeEach
    void create_a_model_whose_listener_for_x_resumes_z_when_the_replay_of_x_runs_again() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory);
        storage = new InMemoryCheckpointStorage();
        live = new HistoryLosingLiveModel();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        model = new StreamCatchupSubscriptionModel(live, eventStore, config);
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (actionOfXFailed.get() && listenerActed.compareAndSet(false, true)) {
                    releaseY.countDown();
                    boolean stopSetTheFlag;
                    try {
                        await().atMost(10, TimeUnit.SECONDS).until(() -> model.stopped);
                        stopSetTheFlag = true;
                    } catch (ConditionTimeoutException e) {
                        stopSetTheFlag = false;
                    }
                    int resumesBefore = live.resumeCalls.size();
                    int startsBefore = live.startCalls.get();
                    Subscription returned = null;
                    Throwable threw = null;
                    try {
                        returned = model.resumeSubscription(Z);
                    } catch (RuntimeException e) {
                        threw = e;
                    }
                    refusal.complete(new Refusal(stopSetTheFlag, returned, threw, live.isPaused(Z), live.resumeCalls.size() - resumesBefore, live.startCalls.get() - startsBefore));
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
    }

    @AfterEach
    void release_everything_and_shut_down() throws InterruptedException {
        releaseY.countDown();
        releaseX.countDown();
        yHandedTheStop.countDown();
        if (liveDelivery != null) {
            liveDelivery.join(TimeUnit.SECONDS.toMillis(10));
        }
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor());
        try {
            shutdown.get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertion of the test has already reported it
        }
    }

    @Test
    void a_resume_called_from_the_listener_of_a_replay_that_runs_again_throws_when_a_stop_that_a_live_action_handed_over_has_stopped_the_model_and_z_is_paused() throws Exception {
        // Given a live subscription y whose action hands a stop over and then waits
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
        model.subscribe(Y, StartAt.now(), event -> {
            model.stop();
            yHandedTheStop.countDown();
            awaitLatch(releaseY);
        });
        // And a paused subscription z
        model.subscribe(Z, StartAt.now(), event -> {
        });
        model.pauseSubscription(Z);
        // And a replay of x that a resume started, whose action fails once while y's stop is handed over
        model.subscribe(X, StartAt.checkpoint(LIVE_START), event -> {
            if (deliveriesToX.incrementAndGet() == 1) {
                inTheActionOfX.countDown();
                awaitLatch(yHandedTheStop);
                actionOfXFailed.set(true);
                throw new IllegalStateException("the action of x fails once");
            }
            awaitLatch(releaseX);
        });
        model.pauseSubscription(X);
        storage.save(X, STORED);
        model.resumeSubscription(X);
        assertThat(inTheActionOfX.await(10, TimeUnit.SECONDS)).as("whether the resumed replay of x reached its action").isTrue();

        // When y's live action runs, and the replay of x runs again after its backoff and tells its listener, which resumes z once the stop has set the stopped flag
        liveDelivery = new Thread(() -> live.deliver(Y, cloudEvent()), "live-delivery-to-y");
        liveDelivery.setDaemon(true);
        liveDelivery.start();

        // Then
        Refusal outcome = awaitRefusal();
        assertThat(outcome).as("whether resumeSubscription(..) called from the catch-up listener returned or threw within 10 s once the stop that a live action handed over was queued").isNotNull();
        assertThat(outcome.stopSetTheFlag()).as("whether the stop that the live action handed over set the stopped flag within 10 s").isTrue();
        assertThat(outcome.returned()).as("what resumeSubscription(..) returned when called from the catch-up listener on a stopped model").isNull();
        assertThat(outcome.threw()).as("what resumeSubscription(..) threw when called from the catch-up listener on a stopped model").isInstanceOf(IllegalStateException.class);
        assertThat(outcome.zPausedAfterwards()).as("whether Z was still paused in the wrapped model right after the refused resume").isTrue();
        assertThat(outcome.resumesOfTheWrappedModel()).as("the resumes the wrapped model got from the refused resume").isZero();
        assertThat(outcome.startsOfTheWrappedModel()).as("the starts the wrapped model got from the refused resume").isZero();
    }

    private Refusal awaitRefusal() throws Exception {
        try {
            return refusal.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return null;
        }
    }

    private static Executor daemonExecutor() {
        return runnable -> {
            Thread thread = new Thread(runnable, "shutdown-of-the-model");
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
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
