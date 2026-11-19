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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A {@code shutdown()} of a {@link CatchupSubscriptionModel} from the action of a replay or from a thread that holds a
 * claim returns at once while a {@code start(..)} is inside the live delegate. The live delegate is shut down after the
 * start returned, and the start resumes nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelShutdownInTurnTest {

    private static final String START_RETURNED = "start-returned";
    private static final String SHUTDOWN = "shutdown";

    private RecordingLiveSubscriptionModel live;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_start_can_be_held() {
        live = new RecordingLiveSubscriptionModel();
        eventStore = new InMemoryEventStore();
        model = new CatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            gates.forEach(CountDownLatch::countDown);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
        }
    }

    @Test
    @Timeout(40)
    void a_shutdown_from_a_replay_action_while_a_start_is_inside_the_live_delegate_returns_at_once_and_shuts_the_live_delegate_down_after_the_start() throws InterruptedException {
        // Given
        write("1");
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch theStartIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theShutdownReturned = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean first = new AtomicBoolean(true);
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            if (first.compareAndSet(true, false)) {
                inTheAction.countDown();
                awaitLatch(theStartIsInTheLiveDelegate);
                thrown.set(catchThrowable(model::shutdown));
                theShutdownReturned.countDown();
            }
        });
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the replay is inside the action").isTrue();
        CountDownLatch startGate = holdStarts();
        run(() -> model.start(true));
        assertThat(live.startEntered.await(10, TimeUnit.SECONDS)).as("the start is inside the live delegate's start").isTrue();

        // When
        theStartIsInTheLiveDelegate.countDown();

        // Then
        assertThat(theShutdownReturned.await(10, TimeUnit.SECONDS)).as("the shutdown from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the shutdown from the action of a replay threw").isNull();
        assertThat(live.events()).as("what the live delegate was asked while the start was inside it").doesNotContain(SHUTDOWN);
        startGate.countDown();
        calls.forEach(CallOnAThread::awaitEnded);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked once the start returned").containsExactly(START_RETURNED, SHUTDOWN));
    }

    @Test
    @Timeout(40)
    void a_start_that_finds_the_model_shut_down_by_a_shutdown_that_did_not_wait_for_it_runs_no_parked_replay_again() throws InterruptedException {
        // Given
        write("1");
        CountDownLatch inTheReplay = new CountDownLatch(1);
        CountDownLatch theStartIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theShutdownReturned = new CountDownLatch(1);
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("parked", StartAtTime.beginningOfTime(), event -> {
            delivered.add(event.getId());
            inTheReplay.countDown();
            awaitLatch(theStartIsInTheLiveDelegate);
            model.shutdown();
            theShutdownReturned.countDown();
        });
        assertThat(inTheReplay.await(5, TimeUnit.SECONDS)).as("the replay is inside the action").isTrue();
        model.stop();
        CountDownLatch startGate = holdStarts();
        run(() -> model.start(true));
        assertThat(live.startEntered.await(10, TimeUnit.SECONDS)).as("the start is inside the live delegate's start").isTrue();
        live.events().clear();

        // When
        theStartIsInTheLiveDelegate.countDown();
        assertThat(theShutdownReturned.await(10, TimeUnit.SECONDS)).as("the shutdown from the action of the parked replay returned").isTrue();
        startGate.countDown();
        calls.forEach(CallOnAThread::awaitEnded);

        // Then
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked once the start returned").containsExactly(START_RETURNED, SHUTDOWN));
        assertThat(delivered).as("the events the parked replay delivered, which a start after the shutdown doesn't run again").containsExactly("1");
    }

    private CountDownLatch holdStarts() {
        CountDownLatch gate = new CountDownLatch(1);
        gates.add(gate);
        live.startGate = gate;
        return gate;
    }

    private CallOnAThread run(Runnable body) {
        CallOnAThread call = new CallOnAThread(body);
        calls.add(call);
        return call;
    }

    private void write(String id) {
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent(id)));
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
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

    // One call on a thread of its own, whose awaitEnded fails when the call threw
    private static final class CallOnAThread {
        private final Thread thread;
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        private CallOnAThread(Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                }
            }, "call-on-a-thread");
            thread.setDaemon(true);
            thread.start();
        }

        void awaitEnded() {
            try {
                thread.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertThat(thread.isAlive()).as("whether the call is still running 15 seconds after the test released what held it").isFalse();
            assertThat(thrown.get()).as("what the call threw").isNull();
        }
    }

    // A live delegate that records the start and shutdown calls that returned, in order, and can hold its start
    private static final class RecordingLiveSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final List<String> events = new CopyOnWriteArrayList<>();
        final CountDownLatch startEntered = new CountDownLatch(1);
        volatile @Nullable CountDownLatch startGate;
        private volatile boolean runs = true;

        List<String> events() {
            return events;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("live start read now");
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public void stop() {
            runs = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            CountDownLatch gate = startGate;
            if (gate != null) {
                startEntered.countDown();
                awaitLatch(gate);
            }
            runs = true;
            events.add(START_RETURNED);
        }

        @Override
        public boolean isRunning() {
            return runs;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }

        @Override
        public void shutdown() {
            events.add(SHUTDOWN);
        }
    }
}
