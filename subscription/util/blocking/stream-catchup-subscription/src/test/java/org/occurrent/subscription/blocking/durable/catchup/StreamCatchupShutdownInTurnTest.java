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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.Gate;

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
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.SHUTDOWN;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.START_RETURNED;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.STOP_RETURNED;

/**
 * A {@code shutdown()} from a thread that must not wait for a {@code start(..)} or {@code stop()} under way, such as the
 * action of a replay or of a live call, returns at once and leaves shutting the live delegate down to the call that holds
 * the model's lock, after that call's own call to the live delegate. A start that finds the model shut down resumes
 * nothing. A {@code shutdown()} from any other thread, and a start or stop behind one, run one at a time in the order
 * they came.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownInTurnTest {

    private static final String ID = "subscription";

    enum Lifecycle {
        START(START_RETURNED) {
            @Override
            void call(StreamCatchupSubscriptionModel model) {
                model.start(true);
            }

            @Override
            Gate hold(GatedLiveSubscriptionModel live, Gate gate) {
                live.startGate = gate;
                return gate;
            }
        },
        STOP(STOP_RETURNED) {
            @Override
            void call(StreamCatchupSubscriptionModel model) {
                model.stop();
            }

            @Override
            Gate hold(GatedLiveSubscriptionModel live, Gate gate) {
                live.stopGate = gate;
                return gate;
            }
        };

        final String returned;

        Lifecycle(String returned) {
            this.returned = returned;
        }

        abstract void call(StreamCatchupSubscriptionModel model);

        abstract Gate hold(GatedLiveSubscriptionModel live, Gate gate);
    }

    enum ActionKind {REPLAY, LIVE}

    private GatedLiveSubscriptionModel live;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<Gate> gates = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_whose_start_stop_and_shutdown_can_be_held() {
        live = new GatedLiveSubscriptionModel(Duration.ofSeconds(8));
        eventStore = new InMemoryEventStore();
        model = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100));
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

    @ParameterizedTest
    @EnumSource(Lifecycle.class)
    @Timeout(40)
    void a_shutdown_from_a_replay_action_while_a_call_is_inside_the_live_delegate_returns_at_once_and_shuts_the_live_delegate_down_after_that_call(Lifecycle lifecycle) throws InterruptedException {
        shutdownFromAnActionWhileACallIsInsideTheLiveDelegate(lifecycle, ActionKind.REPLAY);
    }

    @ParameterizedTest
    @EnumSource(Lifecycle.class)
    @Timeout(40)
    void a_shutdown_from_a_live_action_while_a_call_is_inside_the_live_delegate_returns_at_once_and_shuts_the_live_delegate_down_after_that_call(Lifecycle lifecycle) throws InterruptedException {
        shutdownFromAnActionWhileACallIsInsideTheLiveDelegate(lifecycle, ActionKind.LIVE);
    }

    private void shutdownFromAnActionWhileACallIsInsideTheLiveDelegate(Lifecycle lifecycle, ActionKind actionKind) throws InterruptedException {
        // Given
        Gate gate = lifecycle.hold(live, newGate());
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch theCallIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theShutdownReturned = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean first = new AtomicBoolean(true);
        Consumer<CloudEvent> action = event -> {
            if (first.compareAndSet(true, false)) {
                inTheAction.countDown();
                awaitLatch(theCallIsInTheLiveDelegate);
                thrown.set(catchThrowable(model::shutdown));
                theShutdownReturned.countDown();
            }
        };
        if (actionKind == ActionKind.REPLAY) {
            write("1");
            model.subscribe(ID, StartAtTime.beginningOfTime(), action);
        } else {
            model.subscribe(ID, StartAt.now(), action);
            run(() -> live.callTheKeptAction(ID));
        }
        assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the " + actionKind + " is inside the action").isTrue();
        run(() -> lifecycle.call(model));
        assertThat(gate.entered.await(10, TimeUnit.SECONDS)).as("the " + lifecycle + " is inside the live delegate's call").isTrue();

        // When
        theCallIsInTheLiveDelegate.countDown();

        // Then
        assertThat(theShutdownReturned.await(10, TimeUnit.SECONDS)).as("the shutdown from the action returned or threw").isTrue();
        assertThat(thrown.get()).as("what the shutdown from the action threw").isNull();
        assertThat(live.events()).as("what the live delegate was asked while the " + lifecycle + " was inside it").doesNotContain(SHUTDOWN);

        gate.release();
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(live.events()).as("what the live delegate was asked once the " + lifecycle + " returned").containsExactly(lifecycle.returned, SHUTDOWN);
    }

    @ParameterizedTest
    @EnumSource(Lifecycle.class)
    @Timeout(40)
    void a_shutdown_from_another_thread_that_comes_while_a_call_is_inside_the_live_delegate_shuts_the_live_delegate_down_after_that_call(Lifecycle lifecycle) throws InterruptedException {
        // Given
        Gate gate = lifecycle.hold(live, newGate());
        run(() -> lifecycle.call(model));
        assertThat(gate.entered.await(10, TimeUnit.SECONDS)).as("the " + lifecycle + " is inside the live delegate's call").isTrue();

        // When
        CallOnAThread shutdown = run(model::shutdown);
        shutdown.awaitReturnedOrWaiting();

        // Then
        assertThat(shutdown.hasReturned()).as("whether the shutdown returned while the " + lifecycle + " was inside the live delegate").isFalse();
        assertThat(live.events()).as("what the live delegate was asked while the " + lifecycle + " was inside it").doesNotContain(SHUTDOWN);
        gate.release();
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(live.events()).as("what the live delegate was asked once the " + lifecycle + " returned").containsExactly(lifecycle.returned, SHUTDOWN);
    }

    @ParameterizedTest
    @EnumSource(Lifecycle.class)
    @Timeout(40)
    void a_start_or_stop_that_comes_while_a_shutdown_is_inside_the_live_delegate_runs_after_it(Lifecycle lifecycle) throws InterruptedException {
        // Given
        Gate shutdownGate = newGate();
        live.shutdownGate = shutdownGate;
        run(model::shutdown);
        assertThat(shutdownGate.entered.await(10, TimeUnit.SECONDS)).as("the shutdown is inside the live delegate's shutdown").isTrue();

        // When
        CallOnAThread call = run(() -> lifecycle.call(model));
        call.awaitReturnedOrWaiting();

        // Then
        assertThat(call.hasReturned()).as("whether the " + lifecycle + " returned while the shutdown was inside the live delegate").isFalse();
        assertThat(live.events()).as("what the live delegate was asked while the shutdown was inside it").isEmpty();
        shutdownGate.release();
        calls.forEach(CallOnAThread::awaitEnded);
        assertThat(live.events()).as("what the live delegate was asked once the shutdown returned").containsExactly(SHUTDOWN, lifecycle.returned);
    }

    @Test
    @Timeout(40)
    void a_start_that_finds_the_model_shut_down_by_a_shutdown_that_did_not_wait_for_it_runs_no_parked_replay_again() throws InterruptedException {
        // Given
        write("1");
        CountDownLatch inTheReplay = new CountDownLatch(1);
        CountDownLatch releaseTheReplay = new CountDownLatch(1);
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("parked", StartAtTime.beginningOfTime(), event -> {
            delivered.add(event.getId());
            inTheReplay.countDown();
            awaitLatch(releaseTheReplay);
        });
        assertThat(inTheReplay.await(5, TimeUnit.SECONDS)).as("the replay is inside the action").isTrue();
        model.stop();
        releaseTheReplay.countDown();
        Gate startGate = Lifecycle.START.hold(live, newGate());
        CountDownLatch theStartIsInTheLiveDelegate = new CountDownLatch(1);
        CountDownLatch theShutdownReturned = new CountDownLatch(1);
        run(() -> {
            try (AbstractCatchupSubscriptionModel.Claim ignored = model.claim("holdsAClaim")) {
                awaitLatch(theStartIsInTheLiveDelegate);
                model.shutdown();
                theShutdownReturned.countDown();
            }
        });
        run(() -> model.start(true));
        assertThat(startGate.entered.await(10, TimeUnit.SECONDS)).as("the start is inside the live delegate's start").isTrue();
        live.events().clear();

        // When
        theStartIsInTheLiveDelegate.countDown();
        assertThat(theShutdownReturned.await(10, TimeUnit.SECONDS)).as("the shutdown from the thread that holds a claim returned").isTrue();
        startGate.release();
        calls.forEach(CallOnAThread::awaitEnded);

        // Then
        assertThat(live.events()).as("what the live delegate was asked once the start returned").containsExactly(START_RETURNED, SHUTDOWN);
        assertThat(delivered).as("the events the parked replay delivered, which a start after the shutdown doesn't run again").containsExactly("1");
    }

    private Gate newGate() {
        Gate gate = new Gate();
        gates.add(gate);
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
}
