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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A replay whose live start the live model lost runs again from its origin, and tells its listener on the replay thread
 * while it holds the handover lock of its subscription. A listener that waits there for a {@code stop()} on another
 * thread waits for a call that waits for the lock the listener's thread holds, so that stop gives up on the
 * subscription after the wait for code a caller supplied and says what it changed, instead of holding the model lock
 * for good. The model is stopped then, a shutdown completes, and a call for another subscription is held up for no
 * longer than that wait. The give-up is a 10 s wait that cannot be made shorter, so the test takes about that long.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(90)
class StreamCatchupListenerWaitsForAStopTest {

    private static final String X = "x";
    private static final String Z = "z";
    // The wait for code a caller supplied is 10 s, so a call that gives up there returns inside this
    private static final long A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS = 25;

    private final CountDownLatch listenerWaiting = new CountDownLatch(1);
    private final CountDownLatch listenerMayReturn = new CountDownLatch(1);
    private final AtomicInteger catchupStarts = new AtomicInteger();
    private final AtomicReference<CompletableFuture<Void>> stop = new AtomicReference<>();
    private final AtomicReference<Throwable> stopThrew = new AtomicReference<>();
    private final CopyOnWriteArrayList<CloudEvent> eventsOfZ = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private HistoryLosingLiveModel live;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_whose_listener_for_x_waits_for_a_stop_on_another_thread_when_the_replay_of_x_runs_again_from_its_origin() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        live = new HistoryLosingLiveModel().withoutHistoryBack(new StringBasedCheckpoint("live-start-1"));
        model = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
        model.listenForCatchup(X, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (catchupStarts.incrementAndGet() == 2) {
                    CompletableFuture<Void> stopped = CompletableFuture.runAsync(() -> model.stop());
                    stop.set(stopped);
                    listenerWaiting.countDown();
                    try {
                        stopped.get(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        stopThrew.set(e.getCause());
                    } catch (TimeoutException e) {
                        stopThrew.set(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    awaitLatch(listenerMayReturn);
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
    }

    @AfterEach
    void shut_down() {
        listenerMayReturn.countDown();
        try {
            CompletableFuture.runAsync(() -> {
                model.shutdown();
                inMemory.shutdown();
            }).get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // A shutdown that hangs is left on its daemon thread, the assertion of the test has already reported it
        }
    }

    @Test
    void a_stop_that_a_listener_of_a_replay_that_runs_again_waits_for_gives_up_after_the_wait_for_caller_code_and_leaves_the_model_stopped_and_shutdown_unblocked() throws Exception {
        // Given a history for x to replay
        writeAHistory();

        // When x subscribes, loses its live start so that its replay runs again, and the listener waits for a stop on another thread
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay runs again and began to wait for the stop").isTrue();

        // Then the stop gives up once code the caller supplied has run under the handover lock for the wait
        assertThatTheStopGaveUp();
        assertThat(model.stopped).as("whether the model counts as stopped after the stop that gave up").isTrue();
        assertThat(live.isRunning()).as("whether the live delegate runs after the stop that gave up").isFalse();

        // And the replay parks itself once the listener returned
        listenerMayReturn.countDown();
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(model.hasParkedReplay(X)).as("whether the replay of x parked itself after the listener returned").isTrue());
        assertThat(model.isRunning()).as("whether the model runs once the replay parked").isFalse();

        // And a shutdown completes within a bound
        assertThat(CompletableFuture.runAsync(() -> model.shutdown())).as("shutdown() after the stop that gave up").succeedsWithin(10, TimeUnit.SECONDS);
    }

    @Test
    void a_resume_of_another_subscription_made_while_a_listener_waits_for_a_stop_is_held_up_for_no_longer_than_the_stop_waits() throws Exception {
        // Given a paused subscription z, and a history for x to replay
        model.subscribe(Z, StartAt.now(), eventsOfZ::add);
        model.pauseSubscription(Z);
        writeAHistory();
        model.subscribe(X, StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(listenerWaiting.await(15, TimeUnit.SECONDS)).as("whether the listener of x was told that the replay runs again and began to wait for the stop").isTrue();

        // When z is resumed on another thread while the stop waits for the handover lock of x
        CompletableFuture<Subscription> resumeOfZ = CompletableFuture.supplyAsync(() -> model.resumeSubscription(Z));

        // Then the resume returns once the stop has given up, and z is resumed
        assertThatTheStopGaveUp();
        assertThat(resumeOfZ).as("the resume of z made while the listener waited").succeedsWithin(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(model.isPaused(Z)).as("whether z is paused after its resume").isFalse();

        // And a shutdown completes within a bound
        listenerMayReturn.countDown();
        assertThat(CompletableFuture.runAsync(() -> model.shutdown())).as("shutdown() after the stop that gave up").succeedsWithin(10, TimeUnit.SECONDS);
    }

    private void assertThatTheStopGaveUp() {
        assertThat(stop.get()).as("the stop the listener waited for").failsWithin(A_BOUND_PAST_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause().isInstanceOf(IllegalStateException.class)
                .withMessageStartingWith("Cannot change subscription " + X + ", because code the caller supplied, such as a CatchupListener, ran under its handover lock")
                .withMessageEndingWith(AbstractCatchupSubscriptionModel.STOPPED_ALL_THE_SAME);
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(stopThrew.get()).as("what the listener saw the stop throw").isInstanceOf(IllegalStateException.class));
    }

    private void writeAHistory() {
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
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

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }
}
