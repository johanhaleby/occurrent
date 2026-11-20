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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.Gate;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.GatedLiveSubscriptionModel.STOP_RETURNED;

/**
 * The thread that runs the action of a live call is marked as running an action of the model while the action runs.
 * A {@code stop()} of the model from that thread is handed to the lifecycle thread and returns at once, and a
 * {@code stop()} of another model, from an action of this one, still runs on the calling thread.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupActionMarkTest {

    private static final String ID = "subscription";
    private static final String OTHER_ID = "other-subscription";
    private static final Duration WAIT = Duration.ofSeconds(10);

    private GatedLiveSubscriptionModel live;
    private StreamCatchupSubscriptionModel model;
    private final List<CallOnAThread> calls = new ArrayList<>();
    private final List<Gate> gates = new ArrayList<>();
    private final List<StreamCatchupSubscriptionModel> otherModels = new ArrayList<>();

    @BeforeEach
    void create_a_model_over_a_live_delegate_that_keeps_the_action() {
        live = new GatedLiveSubscriptionModel(Duration.ofSeconds(8));
        model = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            gates.forEach(Gate::release);
            calls.forEach(CallOnAThread::awaitEnded);
        } finally {
            try {
                otherModels.forEach(StreamCatchupSubscriptionModel::shutdown);
            } finally {
                model.shutdown();
            }
        }
    }

    @Test
    @Timeout(40)
    void a_stop_from_the_live_action_of_a_subscription_a_shutdown_has_forgotten_returns_while_that_shutdown_is_still_inside_the_live_delegate() throws InterruptedException {
        // Given
        Gate shutdownGate = new Gate();
        gates.add(shutdownGate);
        live.shutdownGate = shutdownGate;
        CountDownLatch stopReturned = new CountDownLatch(1);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(model::stop));
            stopReturned.countDown();
        });
        run(model::shutdown);
        assertThat(shutdownGate.entered.await(5, TimeUnit.SECONDS)).as("the shutdown is inside the live delegate's shutdown").isTrue();

        // When
        run(() -> live.callTheKeptAction(ID));

        // Then
        assertThat(stopReturned.await(5, TimeUnit.SECONDS)).as("whether the stop from the live action of a forgotten subscription returned while the shutdown still held the model's lock").isTrue();
        assertThat(thrown.get()).as("what the stop from the live action threw").isNull();
    }

    @Test
    @Timeout(40)
    void a_stop_from_a_live_action_after_another_subscriptions_action_ran_inside_it_on_the_same_thread_still_returns_while_the_model_runs_and_stops_it_once_the_action_has_returned() {
        // Given
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean runningRightAfterTheStop = new AtomicBoolean();
        AtomicBoolean theActionRan = new AtomicBoolean();
        model.subscribe(OTHER_ID, null, StartAt.now(), event -> {
        });
        model.subscribe(ID, null, StartAt.now(), event -> {
            live.callTheKeptAction(OTHER_ID);
            thrown.set(catchThrowable(model::stop));
            runningRightAfterTheStop.set(model.isRunning());
            theActionRan.set(true);
        });

        // When
        run(() -> live.callTheKeptAction(ID)).awaitEnded();

        // Then
        assertThat(theActionRan.get()).as("whether the action of the live call ran").isTrue();
        assertThat(thrown.get()).as("what the stop from the live action threw").isNull();
        assertThat(runningRightAfterTheStop.get()).as("whether the model still runs right after a stop from an action that ran another action of the same model before it, which only takes effect once the action has returned").isTrue();
        await().atMost(WAIT).untilAsserted(() -> assertThat(live.events()).as("what the live delegate was asked").containsExactly(STOP_RETURNED));
        assertThat(model.isRunning()).as("whether the model runs once the action has returned").isFalse();
    }

    @Test
    @Timeout(40)
    void a_stop_of_another_catch_up_model_from_a_live_action_runs_on_the_calling_thread_and_has_stopped_that_model_when_it_returns() {
        // Given
        GatedLiveSubscriptionModel otherLive = new GatedLiveSubscriptionModel(Duration.ofSeconds(8));
        StreamCatchupSubscriptionModel other = new StreamCatchupSubscriptionModel(otherLive, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
        otherModels.add(other);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean otherRunningRightAfterTheStop = new AtomicBoolean(true);
        AtomicBoolean theActionRan = new AtomicBoolean();
        model.subscribe(ID, null, StartAt.now(), event -> {
            thrown.set(catchThrowable(other::stop));
            otherRunningRightAfterTheStop.set(other.isRunning());
            theActionRan.set(true);
        });

        // When
        run(() -> live.callTheKeptAction(ID)).awaitEnded();

        // Then
        assertThat(theActionRan.get()).as("whether the action of the live call ran").isTrue();
        assertThat(thrown.get()).as("what the stop of the other model threw").isNull();
        assertThat(otherRunningRightAfterTheStop.get()).as("whether the other model still runs right after a stop of it from an action of this model, which has its own lock and so isn't handed to a lifecycle thread").isFalse();
        assertThat(otherLive.events()).as("what the live delegate of the other model was asked").containsExactly(STOP_RETURNED);
    }

    @Test
    @Timeout(40)
    void a_stop_from_the_live_action_of_a_replay_a_subclass_started_without_the_subscriptions_record_returns_while_the_model_still_runs_and_stops_it_once_the_action_has_returned() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore();
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent("history")));
        StreamCatchupSubscriptionModel withoutTheRecord = new StreamCatchupSubscriptionModel(live, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
                CompletableFuture<Subscription> started = new CompletableFuture<>();
                Thread.ofVirtual().start(() -> {
                    try {
                        started.complete(super.startCatchupAsync(subscriptionId, catchup, holdPaused).get());
                    } catch (Exception e) {
                        started.completeExceptionally(e);
                    }
                });
                return started;
            }
        };
        otherModels.add(withoutTheRecord);
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean runningRightAfterTheStop = new AtomicBoolean();
        AtomicBoolean theActionRan = new AtomicBoolean();
        withoutTheRecord.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
            if ("live1".equals(event.getId())) {
                thrown.set(catchThrowable(withoutTheRecord::stop));
                runningRightAfterTheStop.set(withoutTheRecord.isRunning());
                theActionRan.set(true);
            }
        });
        await().atMost(WAIT).until(() -> live.isRunning(ID));

        // When
        run(() -> live.callTheKeptAction(ID)).awaitEnded();

        // Then
        assertThat(theActionRan.get()).as("whether the action of the live call ran").isTrue();
        assertThat(thrown.get()).as("what the stop from the live action threw").isNull();
        assertThat(runningRightAfterTheStop.get()).as("whether the model still runs right after a stop from the live action of a replay that has no record, which only takes effect once the action has returned").isTrue();
        await().atMost(WAIT).untilAsserted(() -> assertThat(withoutTheRecord.isRunning()).as("whether the model runs once the action has returned").isFalse());
    }

    @Test
    @Timeout(40)
    void a_stop_from_the_live_action_of_a_subscription_made_in_the_wrapped_model_without_the_subscriptions_record_returns_while_the_model_still_runs_and_stops_it_once_the_action_has_returned() {
        // Given
        AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();
        AtomicBoolean runningRightAfterTheStop = new AtomicBoolean();
        AtomicBoolean theActionRan = new AtomicBoolean();
        WithoutARecord subclass = new WithoutARecord(live);
        otherModels.add(subclass);
        subclass.subscribeWithoutARecord(event -> {
            thrown.set(catchThrowable(subclass::stop));
            runningRightAfterTheStop.set(subclass.isRunning());
            theActionRan.set(true);
        });

        // When
        run(() -> live.callTheKeptAction(ID)).awaitEnded();

        // Then
        assertThat(theActionRan.get()).as("whether the action of the live call ran").isTrue();
        assertThat(thrown.get()).as("what the stop from the live action threw").isNull();
        assertThat(runningRightAfterTheStop.get()).as("whether the model still runs right after a stop from the live action of a subscription made without a record, which only takes effect once the action has returned").isTrue();
        await().atMost(WAIT).untilAsserted(() -> assertThat(subclass.isRunning()).as("whether the model runs once the action has returned").isFalse());
    }

    // Calls the protected subscribeInTheWrappedModel from a thread that is not inside a subscribe, so no record is found
    private static final class WithoutARecord extends StreamCatchupSubscriptionModel {
        WithoutARecord(GatedLiveSubscriptionModel live) {
            super(live, new InMemoryEventStore(), new CatchupSubscriptionModelConfig(100));
        }

        void subscribeWithoutARecord(Consumer<CloudEvent> action) {
            subscribeInTheWrappedModel(ID, null, StartAt.now(), action, false);
        }
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private CallOnAThread run(Runnable body) {
        CallOnAThread call = new CallOnAThread(body);
        calls.add(call);
        return call;
    }
}
