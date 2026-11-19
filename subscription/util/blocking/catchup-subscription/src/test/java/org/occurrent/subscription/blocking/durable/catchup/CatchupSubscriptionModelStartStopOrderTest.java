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
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * {@code start(..)}, {@code stop()} and {@code shutdown()} of one {@link CatchupSubscriptionModel} run one at a
 * time, each across the calls to its catch-up models and the wrapped model, as do those of a
 * {@link DcbCatchupSubscriptionModel}. A call from a replay thread of the model is refused instead of waiting for
 * itself.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelStartStopOrderTest {

    private static final String CALL_FROM_ANOTHER_THREAD = "Call it from another thread.";
    private static final String NOTHING_WAS_CHANGED = "Nothing was changed.";

    private InMemorySubscriptionModel inMemory;
    private GatedLifecycleSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;
    private final List<Call> calls = new ArrayList<>();
    private final List<Gate> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_wrapped_model_whose_start_and_stop_can_be_held() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new GatedLifecycleSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        model = new CatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            gates.forEach(Gate::release);
            calls.forEach(Call::awaitEnded);
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_that_comes_while_a_start_is_inside_the_wrapped_start_waits_for_it_and_leaves_both_models_stopped() throws InterruptedException {
        model.stop();
        Gate startGate = holdStarts();
        run(() -> model.start(true));

        try {
            assertThat(startGate.entered.await(5, TimeUnit.SECONDS)).as("the start is inside the wrapped model's start").isTrue();
            Call stop = run(model::stop);
            stop.awaitReturnedOrWaiting();

            assertThat(stop.hasReturned()).as("whether the stop returned while the start was still inside the wrapped model's start").isFalse();
        } finally {
            startGate.release();
        }
        calls.forEach(Call::awaitEnded);

        assertThat(model.isRunning()).as("whether the catch-up model runs once the start and then the stop returned").isFalse();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the start and then the stop returned").isFalse();
        model.subscribe("later", StartAtTime.beginningOfTime(), event -> {
        });
        assertThat(model.isPaused("later")).as("whether a subscription made after the stop is held paused").isTrue();
        assertThat(model.isRunning("later")).as("whether a subscription made after the stop runs").isFalse();
    }

    @Test
    @Timeout(30)
    void a_start_that_comes_while_a_stop_is_inside_the_wrapped_stop_waits_for_it_and_leaves_both_models_started() throws InterruptedException {
        write("1");
        Gate stopGate = holdStops();
        run(model::stop);

        try {
            assertThat(stopGate.entered.await(5, TimeUnit.SECONDS)).as("the stop is inside the wrapped model's stop").isTrue();
            Call start = run(() -> model.start(true));
            start.awaitReturnedOrWaiting();

            assertThat(start.hasReturned()).as("whether the start returned while the stop was still inside the wrapped model's stop").isFalse();
        } finally {
            stopGate.release();
        }
        calls.forEach(Call::awaitEnded);

        assertThat(model.isRunning()).as("whether the catch-up model runs once the stop and then the start returned").isTrue();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the stop and then the start returned").isTrue();
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        model.subscribe("later", StartAtTime.beginningOfTime(), event -> received.add(event.getId()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events a subscription made after the start replays").containsExactly("1"));
    }

    @Test
    @Timeout(30)
    void a_start_that_comes_while_a_stop_waits_inside_the_wrapped_stop_for_a_replay_in_its_action_waits_for_the_stop() throws InterruptedException {
        write("1");
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch releaseTheAction = new CountDownLatch(1);
        Gate stopGate = holdStops();
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            inTheAction.countDown();
            awaitLatch(releaseTheAction);
        });

        try {
            assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the replay is in its action").isTrue();
            run(model::stop);
            assertThat(stopGate.entered.await(10, TimeUnit.SECONDS)).as("the stop is inside the wrapped model's stop").isTrue();
            Call start = run(() -> model.start(false));
            start.awaitReturnedOrWaiting();

            assertThat(start.hasReturned()).as("whether the start returned while the stop was still inside the wrapped model's stop").isFalse();
        } finally {
            stopGate.release();
            releaseTheAction.countDown();
        }
        calls.forEach(Call::awaitEnded);

        assertThat(model.isRunning()).as("whether the catch-up model runs once the stop and then the start returned").isTrue();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the stop and then the start returned").isTrue();
    }

    static Stream<Arguments> lifecycleCallsThatAreRefused() {
        return Stream.of(
                Arguments.of(Named.of("start(true)", (Consumer<CatchupSubscriptionModel>) m -> m.start(true))),
                Arguments.of(Named.of("stop()", (Consumer<CatchupSubscriptionModel>) CatchupSubscriptionModel::stop)));
    }

    @ParameterizedTest
    @MethodSource("lifecycleCallsThatAreRefused")
    @Timeout(30)
    void a_start_or_stop_called_from_the_action_of_a_replay_is_refused_and_the_replay_goes_on(Consumer<CatchupSubscriptionModel> lifecycleCall) throws InterruptedException {
        write("1");
        write("2");
        write("3");
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        CountDownLatch called = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            received.add(event.getId());
            if (first.compareAndSet(true, false)) {
                thrown.set(catchThrowable(() -> lifecycleCall.accept(model)));
                called.countDown();
            }
        });

        assertThat(called.await(5, TimeUnit.SECONDS)).as("the call from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the call from the action of a replay of the model threw").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(CALL_FROM_ANOTHER_THREAD).hasMessageContaining(NOTHING_WAS_CHANGED);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the replay delivered after the refused call").hasSize(3));
        assertThat(model.isRunning()).as("whether the model runs after the refused call").isTrue();
        assertThat(wrapped.isRunning()).as("whether the wrapped model runs after the refused call").isTrue();
    }

    @Test
    @Timeout(30)
    void a_shutdown_from_the_action_of_a_replay_does_not_throw() throws InterruptedException {
        write("1");
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        CountDownLatch called = new CountDownLatch(1);

        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            thrown.set(catchThrowable(model::shutdown));
            called.countDown();
        });

        assertThat(called.await(10, TimeUnit.SECONDS)).as("the shutdown from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what a shutdown from the action of a replay of the model threw").isNull();
    }

    @Test
    @Timeout(30)
    void a_shutdown_that_comes_while_a_start_is_inside_the_wrapped_start_waits_for_it() throws InterruptedException {
        model.stop();
        Gate startGate = holdStarts();
        run(() -> model.start(true));

        try {
            assertThat(startGate.entered.await(5, TimeUnit.SECONDS)).as("the start is inside the wrapped model's start").isTrue();
            Call shutdown = run(model::shutdown);
            shutdown.awaitReturnedOrWaiting();

            assertThat(shutdown.hasReturned()).as("whether the shutdown returned while the start was still inside the wrapped model's start").isFalse();
        } finally {
            startGate.release();
        }
        calls.forEach(Call::awaitEnded);
    }

    @Test
    @Timeout(30)
    void a_start_that_comes_while_a_stop_is_inside_the_wrapped_stop_lets_no_replay_deliver_after_the_stop_returned_unless_the_start_returned_after_it() throws InterruptedException {
        write("1");
        write("2");
        write("3");

        startDuringAStopInsideTheWrappedStop(model, action -> model.subscribe("subscription", StartAtTime.beginningOfTime(), action));
    }

    @Test
    @Timeout(30)
    void a_start_that_comes_while_a_stop_is_inside_the_wrapped_stop_lets_no_dcb_replay_deliver_after_the_stop_returned_unless_the_start_returned_after_it() throws InterruptedException {
        InMemoryEventStore dcbStore = new InMemoryEventStore(inMemory);
        dcbStore.append(List.of("1", "2", "3").stream().map(id -> DcbCloudEvents.withTags(cloudEvent(id), List.of(Tag.parse("name:1")))).toList());
        DcbCatchupSubscriptionModel dcbModel = new DcbCatchupSubscriptionModel(wrapped, dcbStore, DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(100));

        try {
            startDuringAStopInsideTheWrappedStop(dcbModel, action -> dcbModel.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), action));
        } finally {
            dcbModel.shutdown();
        }
    }

    private void startDuringAStopInsideTheWrappedStop(SubscriptionModelLifeCycle target, Consumer<Consumer<CloudEvent>> subscribe) throws InterruptedException {
        AtomicLong sequence = new AtomicLong();
        AtomicLong stopReturned = new AtomicLong();
        AtomicLong startReturned = new AtomicLong();
        CopyOnWriteArrayList<Long> actionBegan = new CopyOnWriteArrayList<>();
        CountDownLatch inTheFirstCall = new CountDownLatch(1);
        CountDownLatch releaseTheFirstCall = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        Gate stopGate = holdStops();

        try {
            subscribe.accept(event -> {
                actionBegan.add(sequence.incrementAndGet());
                if (first.compareAndSet(true, false)) {
                    inTheFirstCall.countDown();
                    awaitLatch(releaseTheFirstCall);
                }
            });
            assertThat(inTheFirstCall.await(5, TimeUnit.SECONDS)).as("the replay is in its first call to the action").isTrue();
            Call stop = run(() -> {
                target.stop();
                stopReturned.set(sequence.incrementAndGet());
            });
            assertThat(stopGate.entered.await(10, TimeUnit.SECONDS)).as("the stop is inside the wrapped model's stop").isTrue();
            Call start = run(() -> {
                target.start(true);
                startReturned.set(sequence.incrementAndGet());
            });
            start.awaitReturnedOrWaiting();
            stopGate.release();
            stop.awaitEnded();
            releaseTheFirstCall.countDown();
            start.awaitEnded();
            await().atMost(Duration.ofSeconds(10)).until(() -> actionBegan.size() >= 2);
        } finally {
            stopGate.release();
            releaseTheFirstCall.countDown();
        }

        long stoppedAt = stopReturned.get();
        long startedAt = startReturned.get();
        boolean startedLast = startedAt > stoppedAt;
        assertThat(actionBegan.stream().anyMatch(began -> began > stoppedAt) && !startedLast)
                .as("whether a call to the action began after the stop returned, though the start returned before it. Calls began at %s, the stop returned at %s and the start at %s", actionBegan, stoppedAt, startedAt)
                .isFalse();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(target.isRunning()).as("whether the catch-up model runs once the call that returned last was the %s", startedLast ? "start" : "stop").isEqualTo(startedLast);
            assertThat(wrapped.isRunning()).as("whether the wrapped model runs once the call that returned last was the %s", startedLast ? "start" : "stop").isEqualTo(startedLast);
        });
    }

    private Gate holdStarts() {
        Gate gate = new Gate();
        gates.add(gate);
        wrapped.startGate = gate;
        return gate;
    }

    private Gate holdStops() {
        Gate gate = new Gate();
        gates.add(gate);
        wrapped.stopGate = gate;
        return gate;
    }

    private Call run(Runnable body) {
        Call call = new Call(body);
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

    // One call on a thread of its own, which has returned once the call returned or threw
    private static final class Call {
        private final Thread thread;
        private final AtomicBoolean returned = new AtomicBoolean();
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        private Call(Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                } finally {
                    returned.set(true);
                }
            }, "start-stop-order-call");
            thread.setDaemon(true);
            thread.start();
        }

        // Returns once the call has returned or its thread waits or is blocked, as a call that waits for a lock or a latch does
        void awaitReturnedOrWaiting() {
            EnumSet<Thread.State> waiting = EnumSet.of(Thread.State.WAITING, Thread.State.TIMED_WAITING, Thread.State.BLOCKED);
            await().atMost(Duration.ofSeconds(10)).until(() -> returned.get() || waiting.contains(thread.getState()));
        }

        boolean hasReturned() {
            return returned.get();
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

    // A call that passes it waits until it is released, which a test does in a finally
    private static final class Gate {
        final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void pass() {
            entered.countDown();
            awaitLatch(released);
        }

        void release() {
            released.countDown();
        }
    }

    // Wraps the in-memory model so a test can hold its start or its stop until it releases the gate
    private static final class GatedLifecycleSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        volatile @Nullable Gate startGate;
        volatile @Nullable Gate stopGate;

        private GatedLifecycleSubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, liveStart(startAt), action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribePaused(subscriptionId, filter, liveStart(startAt), action);
        }

        // The in-memory model starts at now or at its default only
        private static StartAt liveStart(StartAt startAt) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            return resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
        }

        @Override
        public void stop() {
            Gate gate = stopGate;
            if (gate != null) {
                gate.pass();
            }
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            Gate gate = startGate;
            if (gate != null) {
                gate.pass();
            }
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
