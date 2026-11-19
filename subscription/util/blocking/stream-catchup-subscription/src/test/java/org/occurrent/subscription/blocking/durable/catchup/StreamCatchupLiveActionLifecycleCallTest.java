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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.Gate;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.SHUTDOWN;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.START_RETURNED;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.STOP_RETURNED;

/**
 * A live delegate's {@code stop()} can wait for the action of a live call to return. A {@code start(..)}, {@code stop()}
 * or {@code shutdown()} of the catch-up model from inside that action returns at once whether or not another call holds
 * the model's lock, and takes effect once the action has returned, so it never waits for the action that made it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLiveActionLifecycleCallTest {

    private static final String ID = "subscription";
    // How long the live delegate's stop() waits for an action to return before it gives up
    private static final Duration STOP_WAITS_FOR_AN_ACTION_FOR = Duration.ofSeconds(8);
    // A stop that isn't held up by the action returns in much less than this
    private static final Duration A_STOP_THAT_IS_NOT_HELD_UP = Duration.ofSeconds(4);
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String THE_FAILURE = "the live delegate failed";

    enum Call {
        STOP(STOP_RETURNED, "stop()") {
            @Override
            void on(StreamCatchupSubscriptionModel model) {
                model.stop();
            }

            @Override
            void makeFail(GatedLiveSubscriptionModel live, RuntimeException failure) {
                live.stopThrows = failure;
            }
        },
        START(START_RETURNED, "start(..)") {
            @Override
            void on(StreamCatchupSubscriptionModel model) {
                model.start(false);
            }

            @Override
            void makeFail(GatedLiveSubscriptionModel live, RuntimeException failure) {
                throw new UnsupportedOperationException();
            }
        },
        SHUTDOWN_CALL(SHUTDOWN, "shutdown()") {
            @Override
            void on(StreamCatchupSubscriptionModel model) {
                model.shutdown();
            }

            @Override
            void makeFail(GatedLiveSubscriptionModel live, RuntimeException failure) {
                live.shutdownThrows = failure;
            }
        };

        final String returned;
        final String described;

        Call(String returned, String described) {
            this.returned = returned;
            this.described = described;
        }

        abstract void on(StreamCatchupSubscriptionModel model);

        abstract void makeFail(GatedLiveSubscriptionModel live, RuntimeException failure);
    }

    private GatedLiveSubscriptionModel live;
    private StreamCatchupSubscriptionModel model;
    private Logger modelLog;
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<Gate> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_stop_waits_for_the_actions_of_other_threads() {
        live = new GatedLiveSubscriptionModel(STOP_WAITS_FOR_AN_ACTION_FOR);
        model = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
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
            gates.forEach(Gate::release);
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
    @Timeout(40)
    void a_call_from_a_live_action_while_a_stop_waits_for_that_action_returns_normally_and_runs_after_the_action_and_the_stop(Call call) throws InterruptedException {
        // Given
        Gate stopGate = holdStops();
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch theStopIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theCallFromTheActionEnded = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicReference<List<String>> askedWhenTheActionReturned = new AtomicReference<>();
        AtomicLong stopTookNanos = new AtomicLong();
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
        run(() -> {
            long began = System.nanoTime();
            model.stop();
            stopTookNanos.set(System.nanoTime() - began);
        });
        assertThat(stopGate.entered.await(5, TimeUnit.SECONDS)).as("the stop is inside the live delegate's stop").isTrue();
        theStopIsInTheLiveDelegate.countDown();
        stopGate.release();

        // Then
        assertThat(theCallFromTheActionEnded.await(20, TimeUnit.SECONDS)).as("the " + call.described + " from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " from the action threw while a stop waited in the live delegate for the action").isNull();
        assertThat(askedWhenTheActionReturned.get()).as("what the live delegate was asked when the action returned").isEmpty();
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(live.stopGaveUpWaitingForAnAction()).as("whether the live delegate's stop gave up waiting for the action").isFalse();
        assertThat(Duration.ofNanos(stopTookNanos.get())).as("how long the stop took").isLessThan(A_STOP_THAT_IS_NOT_HELD_UP);
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(STOP_RETURNED, call.returned));
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
    @EnumSource(value = Call.class, names = {"STOP", "SHUTDOWN_CALL"})
    @Timeout(40)
    void a_stop_or_shutdown_from_a_live_action_does_not_wait_for_that_action_in_a_live_delegate_that_waits_for_every_action(Call call) throws InterruptedException {
        // Given
        live.stopAndShutdownWaitForTheCallersOwnAction = true;
        CountDownLatch actionReturned = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> thrown.set(catchThrowable(() -> call.on(model))));

        // When
        run(() -> {
            live.callTheKeptAction(ID);
            actionReturned.countDown();
        });

        // Then
        assertThat(actionReturned.await(A_STOP_THAT_IS_NOT_HELD_UP.toMillis(), TimeUnit.MILLISECONDS)).as("whether the action returned, which a " + call.described + " that waits for the action it was made from never lets it").isTrue();
        assertThat(thrown.get()).as("what the " + call.described + " from the action threw").isNull();
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(call.returned));
        assertThat(live.stopGaveUpWaitingForAnAction()).as("whether the live delegate gave up waiting for the action").isFalse();
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
        RuntimeException failure = new IllegalStateException(THE_FAILURE);
        call.makeFail(live, failure);
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
        await().atMost(WAIT).untilAsserted(() -> assertThat(errorsLoggedForTheLeftCall()).as("errors logged for the " + call.described + " left to run").hasSize(1));
        ILoggingEvent error = errorsLoggedForTheLeftCall().getFirst();
        assertThat(error.getLevel()).as("level of the line logged for the " + call.described + " that threw").isEqualTo(Level.ERROR);
        assertThat(error.getFormattedMessage()).as("the line logged for the " + call.described + " that threw").startsWith(call.described + " of ").contains(model.toString());
        assertThat(error.getThrowableProxy()).as("the exception logged for the " + call.described + " that threw").isNotNull();
        assertThat(error.getThrowableProxy().getMessage()).as("message of the exception logged for the " + call.described + " that threw").isEqualTo(THE_FAILURE);
    }

    private List<ILoggingEvent> errorsLoggedForTheLeftCall() {
        return logged.list.stream()
                .filter(event -> event.getLevel().isGreaterOrEqual(Level.WARN))
                .filter(event -> event.getFormattedMessage().contains("run on the lifecycle thread"))
                .toList();
    }

    private Gate holdStops() {
        Gate gate = new Gate();
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
}
