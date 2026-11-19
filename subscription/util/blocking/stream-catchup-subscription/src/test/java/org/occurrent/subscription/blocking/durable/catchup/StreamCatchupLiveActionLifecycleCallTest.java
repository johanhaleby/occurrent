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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.Gate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A live delegate's {@code stop()} can wait for the action of a live call to return. A {@code start(..)} or
 * {@code stop()} of the catch-up model from inside that action, while another such call holds the model's lock and is
 * waiting in the live delegate, would wait for that call and never return, so it is refused instead.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLiveActionLifecycleCallTest {

    private static final String ID = "subscription";
    private static final String NOTHING_WAS_CHANGED = "Nothing was changed.";
    // How long the live delegate's stop() waits for an action to return before it gives up
    private static final Duration STOP_WAITS_FOR_AN_ACTION_FOR = Duration.ofSeconds(8);
    // A stop that isn't held up by the action returns in much less than this
    private static final Duration A_STOP_THAT_IS_NOT_HELD_UP = Duration.ofSeconds(4);

    private GatedLiveSubscriptionModel live;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<Gate> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_stop_waits_for_the_actions_of_other_threads() {
        live = new GatedLiveSubscriptionModel(STOP_WAITS_FOR_AN_ACTION_FOR);
        model = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            gates.forEach(Gate::release);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            model.shutdown();
        }
    }

    static Stream<Arguments> lifecycleCallsFromTheAction() {
        return Stream.of(
                Arguments.of(Named.of("stop()", (Consumer<StreamCatchupSubscriptionModel>) StreamCatchupSubscriptionModel::stop)),
                Arguments.of(Named.of("start(false)", (Consumer<StreamCatchupSubscriptionModel>) m -> m.start(false))));
    }

    @ParameterizedTest
    @MethodSource("lifecycleCallsFromTheAction")
    @Timeout(40)
    void a_start_or_stop_from_a_live_action_while_a_stop_waits_for_that_action_is_refused_and_the_stop_returns(Consumer<StreamCatchupSubscriptionModel> lifecycleCall) throws InterruptedException {
        // Given
        Gate stopGate = holdStops();
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch theStopIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theCallFromTheActionEnded = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicLong stopTookNanos = new AtomicLong();
        model.subscribe(ID, null, StartAt.now(), event -> {
            inTheAction.countDown();
            awaitLatch(theStopIsInTheLiveDelegate);
            thrown.set(catchThrowable(() -> lifecycleCall.accept(model)));
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
        assertThat(theCallFromTheActionEnded.await(20, TimeUnit.SECONDS)).as("the call from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the call from the action of a live call threw while a stop waited in the live delegate for the action")
                .isInstanceOf(IllegalStateException.class).hasMessageContaining(NOTHING_WAS_CHANGED);
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(live.stopGaveUpWaitingForAnAction()).as("whether the live delegate's stop gave up waiting for the action").isFalse();
        assertThat(Duration.ofNanos(stopTookNanos.get())).as("how long the stop took").isLessThan(A_STOP_THAT_IS_NOT_HELD_UP);
    }

    @Test
    @Timeout(30)
    void a_stop_from_a_live_action_with_no_start_or_stop_under_way_runs() throws InterruptedException {
        // Given
        CountDownLatch called = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(model::stop));
            called.countDown();
        });

        // When
        run(() -> live.callTheKeptAction(ID));

        // Then
        assertThat(called.await(10, TimeUnit.SECONDS)).as("the stop from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what a stop from the action of a live call threw with no start or stop under way").isNull();
        assertThat(live.events()).as("what the live delegate was asked").containsExactly(GatedLiveSubscriptionModel.STOP_RETURNED);
        assertThat(model.isRunning()).as("whether the model runs after the stop from the action").isFalse();
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
