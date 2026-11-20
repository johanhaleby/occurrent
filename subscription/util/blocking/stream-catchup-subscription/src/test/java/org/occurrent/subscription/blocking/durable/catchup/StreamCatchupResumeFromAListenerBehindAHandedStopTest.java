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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A replay a resume started runs again after a backoff and tells its listener, on the replay thread, while it holds the
 * handover lock of its subscription. A {@code resumeSubscription(..)} the listener calls then must not wait for a
 * {@code stop()} a live action handed over, since that stop waits for the handover lock the listener's thread holds.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeFromAListenerBehindAHandedStopTest {

    private static final String X = "x";
    private static final String Y = "y";
    private static final String Z = "z";
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final CountDownLatch yHandedTheStop = new CountDownLatch(1);
    private final CountDownLatch releaseY = new CountDownLatch(1);
    private final CountDownLatch releaseX = new CountDownLatch(1);
    private final CountDownLatch inTheActionOfX = new CountDownLatch(1);
    private final CountDownLatch listenerReturned = new CountDownLatch(1);
    private final AtomicInteger deliveriesToX = new AtomicInteger();
    private final AtomicBoolean actionOfXFailed = new AtomicBoolean();
    private final AtomicBoolean listenerActed = new AtomicBoolean();
    private final AtomicReference<Subscription> resumeOfZReturned = new AtomicReference<>();
    private final AtomicReference<Throwable> resumeOfZThrew = new AtomicReference<>();
    private final CopyOnWriteArrayList<CloudEvent> eventsOfZ = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private InMemoryCheckpointStorage storage;
    private HistoryLosingLiveModel live;
    private StreamCatchupSubscriptionModel model;
    private Thread liveDelivery;

    @BeforeEach
    void create_a_model_whose_listener_for_x_resumes_y_when_the_replay_of_x_runs_again() {
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
                    try {
                        resumeOfZReturned.set(model.resumeSubscription(Z));
                    } catch (RuntimeException e) {
                        resumeOfZThrew.set(e);
                    } finally {
                        releaseY.countDown();
                        listenerReturned.countDown();
                    }
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
        });
        try {
            shutdown.get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertion of the test has already reported it
        }
    }

    @Test
    void a_resume_called_from_the_listener_of_a_replay_that_runs_again_returns_while_a_stop_that_a_live_action_handed_over_is_queued() throws InterruptedException {
        // Given a live subscription y whose action hands a stop over and then waits
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
        model.subscribe(Y, StartAt.now(), event -> {
            model.stop();
            yHandedTheStop.countDown();
            awaitLatch(releaseY);
        });
        // And a paused subscription z that records what it gets
        model.subscribe(Z, StartAt.now(), eventsOfZ::add);
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

        // When y's live action runs, and the replay of x runs again after its backoff and tells its listener
        liveDelivery = new Thread(() -> live.deliver(Y, cloudEvent()), "live-delivery-to-y");
        liveDelivery.setDaemon(true);
        liveDelivery.start();

        // Then
        assertThat(listenerReturned.await(10, TimeUnit.SECONDS))
                .as("whether resumeSubscription(..) called from the catch-up listener returned while a stop that a live action handed over was queued")
                .isTrue();
        assertThat(resumeOfZThrew.get()).as("what resumeSubscription(..) threw when called from the catch-up listener before the stop took effect").isNull();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.isRunning()).as("whether the stop that the live action handed over took effect").isFalse());
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.hasParkedReplay(X)).as("whether the stop parked the replay of x").isTrue());
        model.start(true);
        live.deliver(Z, cloudEvent());
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(eventsOfZ)
                .as("the events Z's action got after start(true), once a resume called from the catch-up listener had returned while a stop handed over from a live action was queued")
                .hasSize(1));
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
