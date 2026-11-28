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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.Gate;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

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
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code stop()} or a {@code stopReplay()} called from inside the action of a replay returns at once and runs on the
 * lifecycle thread once the action has returned. No further event of that replay reaches the action from the moment the
 * action made the call, also while the lifecycle thread waits for the model lock, and a {@code start(true)} made once the
 * call has run delivers the rest.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupStopFromAReplayActionDeliversNoFurtherEventTest {

    private static final Duration NO_FURTHER_EVENT_WITHIN = Duration.ofMillis(500);
    private static final OffsetDateTime HISTORY_BEGINS = OffsetDateTime.parse("2026-01-01T00:00:00Z");

    private GatedLiveSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private StreamCatchupSubscriptionModel model;
    private final List<Gate> gates = new ArrayList<>();
    private final List<CallOnAThread> calls = new ArrayList<>();

    @BeforeEach
    void create_a_model_that_stores_the_position_after_each_replayed_event_over_a_live_model_whose_start_can_be_held() {
        wrapped = new GatedLiveSubscriptionModel(Duration.ofSeconds(1));
        eventStore = new InMemoryEventStore();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
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

    enum StopCall {
        STOP("stop()", StreamCatchupSubscriptionModel::stop, model -> !model.isRunning()),
        STOP_REPLAY("stopReplay()", StreamCatchupSubscriptionModel::stopReplay, model -> model.stopped);

        private final String description;
        private final Consumer<StreamCatchupSubscriptionModel> call;
        private final Predicate<StreamCatchupSubscriptionModel> hasRun;

        StopCall(String description, Consumer<StreamCatchupSubscriptionModel> call, Predicate<StreamCatchupSubscriptionModel> hasRun) {
            this.description = description;
            this.call = call;
            this.hasRun = hasRun;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(StopCall.class)
    @Timeout(30)
    void no_further_event_of_a_replay_reaches_the_action_once_it_called_a_stop_that_waits_for_the_model_lock_and_a_start_that_runs_the_replay_again_delivers_the_rest_once(StopCall stopCall) throws InterruptedException {
        // Given
        write("1");
        write("2");
        write("3");
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicBoolean first = new AtomicBoolean(true);
        CountDownLatch inTheFirstAction = new CountDownLatch(1);
        CountDownLatch makeTheCall = new CountDownLatch(1);
        CountDownLatch called = new CountDownLatch(1);
        CountDownLatch aFurtherDelivery = new CountDownLatch(1);
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            received.add(event.getId());
            if (!first.compareAndSet(true, false)) {
                aFurtherDelivery.countDown();
                return;
            }
            inTheFirstAction.countDown();
            awaitLatch(makeTheCall);
            stopCall.call.accept(model);
            called.countDown();
        });
        assertThat(inTheFirstAction.await(5, TimeUnit.SECONDS)).as("the replay is in the action of its first event").isTrue();
        Gate startGate = new Gate();
        gates.add(startGate);
        wrapped.startGate = startGate;
        calls.add(new CallOnAThread(() -> model.start(false)));
        assertThat(startGate.entered.await(5, TimeUnit.SECONDS)).as("a start from another thread holds the model lock inside the wrapped model's start").isTrue();

        // When
        makeTheCall.countDown();

        // Then
        assertThat(called.await(5, TimeUnit.SECONDS)).as("the %s from the action returned while the model lock was held", stopCall).isTrue();
        assertThat(aFurtherDelivery.await(NO_FURTHER_EVENT_WITHIN.toMillis(), TimeUnit.MILLISECONDS))
                .as("whether a further event of the replay reached the action within %d ms of the %s it called, while that call waited for the model lock", NO_FURTHER_EVENT_WITHIN.toMillis(), stopCall)
                .isFalse();
        startGate.release();
        calls.forEach(CallOnAThread::awaitEnded);
        await().atMost(Duration.ofSeconds(10)).until(() -> stopCall.hasRun.test(model));
        assertThat(received).as("the events the action saw once the %s it called had run", stopCall).containsExactly("1");

        wrapped.startGate = null;
        model.start(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).as("the events the action saw once a start(true) ran the replay again").containsExactly("1", "2", "3"));
    }

    private void write(String id) {
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent(id)));
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(HISTORY_BEGINS.plusSeconds(Long.parseLong(id))).build();
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
