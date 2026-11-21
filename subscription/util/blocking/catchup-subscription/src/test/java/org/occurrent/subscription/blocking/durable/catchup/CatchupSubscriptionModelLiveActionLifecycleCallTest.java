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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;

/**
 * A {@code start(..)}, {@code stop()} or {@code shutdown()} of a {@link CatchupSubscriptionModel} from inside the action
 * of a live call returns at once whether or not another call holds the model's lock, and takes effect once the action
 * has returned, so it never waits for the action that made it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelLiveActionLifecycleCallTest {

    private static final String ID = "subscription";
    private static final String START_RETURNED = "start-returned";
    private static final String STOP_RETURNED = "stop-returned";
    private static final String SHUTDOWN = "shutdown";
    private static final String THE_FAILURE = "the live delegate failed";
    // How long the live delegate's stop() and shutdown() wait for an action to return before they give up
    private static final Duration WAITS_FOR_AN_ACTION_FOR = Duration.ofSeconds(8);
    // A call that isn't held up by the action returns in much less than this
    private static final Duration A_CALL_THAT_IS_NOT_HELD_UP = Duration.ofSeconds(4);
    private static final Duration WAIT = Duration.ofSeconds(10);

    enum Call {
        STOP(STOP_RETURNED, "stop()") {
            @Override
            void on(CatchupSubscriptionModel model) {
                model.stop();
            }

            @Override
            void makeFail(LiveDelegate live, RuntimeException failure) {
                live.stopThrows = failure;
            }
        },
        START(START_RETURNED, "start(..)") {
            @Override
            void on(CatchupSubscriptionModel model) {
                model.start(false);
            }

            @Override
            void makeFail(LiveDelegate live, RuntimeException failure) {
                throw new UnsupportedOperationException();
            }
        },
        SHUTDOWN_CALL(SHUTDOWN, "shutdown()") {
            @Override
            void on(CatchupSubscriptionModel model) {
                model.shutdown();
            }

            @Override
            void makeFail(LiveDelegate live, RuntimeException failure) {
                live.shutdownThrows = failure;
            }
        };

        final String returned;
        final String described;

        Call(String returned, String described) {
            this.returned = returned;
            this.described = described;
        }

        abstract void on(CatchupSubscriptionModel model);

        abstract void makeFail(LiveDelegate live, RuntimeException failure);
    }

    private LiveDelegate live;
    private CatchupSubscriptionModel model;
    private Logger modelLog;
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<CountDownLatch> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_stop_and_shutdown_wait_for_the_actions_of_every_thread() {
        live = new LiveDelegate();
        model = new CatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
        modelLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        logged.list = new CopyOnWriteArrayList<>();
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            live.stopThrows = null;
            live.shutdownThrows = null;
            gates.forEach(CountDownLatch::countDown);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            try {
                model.shutdown();
            } finally {
                modelLog.detachAppender(logged);
                logged.stop();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Call.class)
    @Timeout(30)
    void a_call_from_a_live_action_with_no_start_or_stop_under_way_returns_normally_and_runs_after_the_action(Call call) throws InterruptedException {
        // Given
        CountDownLatch called = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<List<String>> askedWhenTheActionReturned = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(() -> call.on(model)));
            askedWhenTheActionReturned.set(List.copyOf(live.events()));
            called.countDown();
        });

        // When
        run(() -> live.callTheKeptAction(ID));

        // Then
        assertThat(called.await(10, TimeUnit.SECONDS)).as("the " + call.described + " from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " from the action of a live call threw with no start or stop under way").isNull();
        assertThat(askedWhenTheActionReturned.get()).as("what the live delegate was asked when the action returned").isEmpty();
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(call.returned));
    }

    @ParameterizedTest
    @EnumSource(Call.class)
    @Timeout(40)
    void a_call_from_a_live_action_while_a_stop_waits_for_that_action_returns_normally_and_runs_after_the_action_and_the_stop(Call call) throws InterruptedException {
        // Given
        CountDownLatch stopGate = holdStops();
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch theStopIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theCallFromTheActionEnded = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<List<String>> askedWhenTheActionReturned = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            inTheAction.countDown();
            awaitLatch(theStopIsInTheLiveDelegate);
            thrown.set(catchThrowable(() -> call.on(model)));
            askedWhenTheActionReturned.set(List.copyOf(live.events()));
            theCallFromTheActionEnded.countDown();
        });
        run(() -> live.callTheKeptAction(ID));
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the live call is inside the action").isTrue();

        // When
        run(model::stop);
        assertThat(live.stopEntered.await(5, TimeUnit.SECONDS)).as("the stop is inside the live delegate's stop").isTrue();
        theStopIsInTheLiveDelegate.countDown();
        stopGate.countDown();

        // Then
        assertThat(theCallFromTheActionEnded.await(20, TimeUnit.SECONDS)).as("the " + call.described + " from the action returned or threw").isTrue();
        calls.forEach(CallOnAThread::awaitEnded);
        assertSoftly(softly -> {
            softly.assertThat(thrown.get()).as("what the " + call.described + " from the action threw while a stop waited in the live delegate for the action").isNull();
            softly.assertThat(askedWhenTheActionReturned.get()).as("what the live delegate was asked when the action returned").isEmpty();
            softly.assertThat(live.gaveUpWaitingForAnAction()).as("whether the live delegate's stop gave up waiting for the action").isFalse();
        });
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(STOP_RETURNED, call.returned));
    }

    @ParameterizedTest
    @EnumSource(value = Call.class, names = {"STOP", "SHUTDOWN_CALL"})
    @Timeout(40)
    void a_stop_or_shutdown_from_a_live_action_does_not_wait_for_that_action_in_a_live_delegate_that_waits_for_every_action(Call call) throws InterruptedException {
        // Given
        CountDownLatch actionReturned = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> thrown.set(catchThrowable(() -> call.on(model))));

        // When
        run(() -> {
            live.callTheKeptAction(ID);
            actionReturned.countDown();
        });

        // Then
        assertThat(actionReturned.await(A_CALL_THAT_IS_NOT_HELD_UP.toMillis(), TimeUnit.MILLISECONDS)).as("whether the action returned, which a " + call.described + " that waits for the action it was made from never lets it").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " from the action threw").isNull();
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(call.returned));
        assertThat(live.gaveUpWaitingForAnAction()).as("whether the live delegate gave up waiting for the action").isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = Call.class, names = {"STOP", "START"})
    @Timeout(30)
    void a_start_or_stop_from_a_live_action_that_a_shutdown_from_the_same_action_overtakes_is_not_run(Call call) throws InterruptedException {
        // Given
        CountDownLatch called = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(() -> {
                call.on(model);
                model.shutdown();
            }));
            called.countDown();
        });

        // When
        run(() -> live.callTheKeptAction(ID));

        // Then
        assertThat(called.await(10, TimeUnit.SECONDS)).as("the " + call.described + " and the shutdown from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " and the shutdown from the action threw").isNull();
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked, where the " + call.described + " the shutdown overtook was left to run before it").contains(SHUTDOWN));
        assertThat(live.events()).as("what the live delegate was asked").containsExactly(SHUTDOWN);
    }

    @ParameterizedTest
    @EnumSource(value = Call.class, names = {"STOP", "SHUTDOWN_CALL"})
    @Timeout(30)
    void a_stop_or_shutdown_from_a_live_action_that_throws_once_the_action_has_returned_is_logged_as_an_error_with_the_model_and_the_exception(Call call) throws InterruptedException {
        // Given
        call.makeFail(live, new IllegalStateException(THE_FAILURE));
        CountDownLatch called = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(() -> call.on(model)));
            called.countDown();
        });

        // When
        run(() -> live.callTheKeptAction(ID));

        // Then
        assertThat(called.await(10, TimeUnit.SECONDS)).as("the " + call.described + " from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " from the action threw, which is left to run once the action has returned").isNull();
        await().atMost(WAIT).untilAsserted(() -> assertThat(loggedForTheLeftCall()).as("lines logged for the " + call.described + " left to run").hasSize(1));
        ILoggingEvent error = loggedForTheLeftCall().getFirst();
        assertSoftly(softly -> {
            softly.assertThat(error.getLevel()).as("level of the line logged for the " + call.described + " that threw").isEqualTo(Level.ERROR);
            softly.assertThat(error.getFormattedMessage()).as("the line logged for the " + call.described + " that threw").startsWith(call.described + " of ").contains(model.toString());
            softly.assertThat(error.getThrowableProxy()).as("the exception logged for the " + call.described + " that threw").isNotNull();
            softly.assertThat(error.getThrowableProxy() == null ? null : error.getThrowableProxy().getMessage()).as("message of the exception logged for the " + call.described + " that threw").isEqualTo(THE_FAILURE);
        });
    }

    @Test
    @Timeout(30)
    void a_start_from_another_thread_made_while_a_stop_from_a_live_action_has_not_run_yet_runs_after_that_stop() {
        // Given
        AtomicReference<@Nullable CallOnAThread> start = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            model.stop();
            CallOnAThread startFromAnotherThread = new CallOnAThread(() -> model.start(false));
            start.set(startFromAnotherThread);
            startFromAnotherThread.awaitReturnedOrWaiting();
        });

        // When
        run(() -> live.callTheKeptAction(ID)).awaitEnded();
        start.get().awaitEnded();

        // Then
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked, where the start came after the stop from the action").containsExactly(STOP_RETURNED, START_RETURNED));
        assertThat(model.isRunning()).as("whether the model runs, which a start that ran before the stop from the action leaves it not doing").isTrue();
    }

    private List<ILoggingEvent> loggedForTheLeftCall() {
        return logged.list.stream()
                .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                .filter(event -> event.getFormattedMessage().contains("run on another thread"))
                .toList();
    }

    private CountDownLatch holdStops() {
        CountDownLatch gate = new CountDownLatch(1);
        gates.add(gate);
        live.stopGate = gate;
        return gate;
    }

    private CallOnAThread run(Runnable body) {
        CallOnAThread call = new CallOnAThread(body);
        calls.add(call);
        return call;
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
        private final AtomicBoolean returned = new AtomicBoolean();
        private final AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

        private CallOnAThread(Runnable body) {
            thread = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable e) {
                    thrown.set(e);
                } finally {
                    returned.set(true);
                }
            }, "call-on-a-thread");
            thread.setDaemon(true);
            thread.start();
        }

        // Returns once the call has returned or its thread waits or is blocked, as a call that waits for a lock or a future does
        void awaitReturnedOrWaiting() {
            EnumSet<Thread.State> waiting = EnumSet.of(Thread.State.WAITING, Thread.State.TIMED_WAITING, Thread.State.BLOCKED);
            await().atMost(WAIT).until(() -> returned.get() || waiting.contains(thread.getState()));
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

    // A live delegate that keeps the action it was given and calls it when told to, on the calling thread, as a delegate
    // does that delivers on a thread of its own. Its stop() and shutdown() wait, for a bounded time, for every thread that
    // is inside a call of a kept action, the calling thread included, as a live delegate does that waits for its
    // deliveries to end with no deadline. It records each call that returned, in order, and can hold its stop.
    private static final class LiveDelegate implements CheckpointAwareSubscriptionModel {
        private final Map<String, Consumer<CloudEvent>> kept = new ConcurrentHashMap<>();
        private final Set<Thread> inAKeptAction = ConcurrentHashMap.newKeySet();
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final AtomicBoolean gaveUp = new AtomicBoolean();
        final CountDownLatch stopEntered = new CountDownLatch(1);
        volatile @Nullable CountDownLatch stopGate;
        volatile @Nullable RuntimeException stopThrows;
        volatile @Nullable RuntimeException shutdownThrows;
        private volatile boolean runs = true;

        List<String> events() {
            return events;
        }

        boolean gaveUpWaitingForAnAction() {
            return gaveUp.get();
        }

        void callTheKeptAction(String subscriptionId) {
            CloudEvent event = CloudEventBuilder.v1().withId("live1").withSource(URI.create("urn:test")).withType("Tested").build();
            Thread caller = Thread.currentThread();
            inAKeptAction.add(caller);
            try {
                kept.get(subscriptionId).accept(event);
            } finally {
                inAKeptAction.remove(caller);
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            kept.put(subscriptionId, action);
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
            CountDownLatch gate = stopGate;
            if (gate != null) {
                stopEntered.countDown();
                awaitLatch(gate);
            }
            throwIfTold(stopThrows);
            waitForTheKeptActions();
            runs = false;
            events.add(STOP_RETURNED);
        }

        private void waitForTheKeptActions() {
            long deadline = System.nanoTime() + WAITS_FOR_AN_ACTION_FOR.toNanos();
            while (!inAKeptAction.isEmpty()) {
                if (System.nanoTime() >= deadline) {
                    gaveUp.set(true);
                    return;
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        private static void throwIfTold(@Nullable RuntimeException failure) {
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            runs = true;
            events.add(START_RETURNED);
        }

        @Override
        public boolean isRunning() {
            return runs;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return kept.containsKey(subscriptionId);
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
            kept.remove(subscriptionId);
        }

        @Override
        public void shutdown() {
            throwIfTold(shutdownThrows);
            waitForTheKeptActions();
            events.add(SHUTDOWN);
        }
    }
}
