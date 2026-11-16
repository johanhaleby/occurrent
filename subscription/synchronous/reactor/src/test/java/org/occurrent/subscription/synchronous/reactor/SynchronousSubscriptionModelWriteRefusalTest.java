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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.reactor.dcb.GenericDcbApplicationService;
import org.occurrent.application.service.reactor.generic.GenericApplicationService;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The reactive counterpart of the blocking write refusal test. A refusal is a failed {@link Mono}, and nothing is
 * written. These tests drive the real reactive application service over a real model, which is never given to the
 * test extension that turns the refusals off.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteRefusalTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final long WAIT_SECONDS = 10;

    private InMemoryReactiveEventStore eventStore;
    private SynchronousSubscriptionModel model;
    private List<String> handled;
    private GenericApplicationService<TestEvent> applicationService;

    @BeforeEach
    void initialize() {
        eventStore = new InMemoryReactiveEventStore();
        model = new SynchronousSubscriptionModel();
        handled = new CopyOnWriteArrayList<>();
        applicationService = GenericApplicationService.builder(eventStore, new TestEventConverter())
                .synchronousSubscriptions(model)
                .build();
    }

    @Test
    void a_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_after_the_model_is_stopped_goes_through_when_no_handler_is_registered() {
        model.stop();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_after_the_model_is_started_again_goes_through_and_reaches_the_handler() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        model.start();
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_to_the_stream_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(applicationService.execute("x", events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains("x")).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_to_another_stream_than_the_one_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(applicationService.execute("y", events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains("y")).as("the write was stored").isTrue();
        assertThat(eventStore.contains("x")).as("the other stream was written").isFalse();
    }

    @Test
    void a_write_that_gives_an_event_the_stream_version_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(1)), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_that_gives_no_event_the_stream_version_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(2)), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_that_a_paused_subscription_does_not_match_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add("orders-projection")));
        model.subscribe("other-projection", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> Mono.fromRunnable(() -> handled.add("other-projection")));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamId, events -> List.of(new TestEvent("NameWasChanged"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
        assertThat(handled).containsExactly("other-projection");
    }

    @Test
    void a_pause_from_another_thread_waits_for_the_write_in_flight_and_the_paused_subscription_still_gets_its_event() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block());
        held.awaitEntered();

        Thread pauser = start(() -> model.pauseSubscription("p"));

        assertThat(waitingOrDone(pauser)).as("what the pausing thread does while the write is in flight").isEqualTo(Thread.State.WAITING);
        assertThat(model.isPaused("p")).as("paused while the write is in flight").isFalse();
        held.release();
        assertThat(joined(writer)).as("the write finished").isTrue();
        assertThat(joined(pauser)).as("the pause returned once the write closed").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isPaused("p")).as("paused once the pause returned").isTrue();
    }

    @Test
    void a_handler_that_pauses_its_own_subscription_does_not_wait_for_its_own_write_and_the_pause_applies_once_the_write_closes() {
        List<Boolean> pausedInsideTheHandler = new CopyOnWriteArrayList<>();
        model.subscribe("p", event -> Mono.fromRunnable(() -> {
            handled.add(event.getType());
            model.pauseSubscription("p");
            pausedInsideTheHandler.add(model.isPaused("p"));
        }));
        CompletableFuture<?> write = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block(), daemonThreads());

        assertThat(write).as("the write whose handler paused its own subscription").succeedsWithin(Duration.ofSeconds(WAIT_SECONDS));
        assertThat(handled).containsExactly("NameDefined");
        assertThat(pausedInsideTheHandler).as("paused inside the handler, while its write was open").containsExactly(false);
        assertThat(model.isPaused("p")).as("paused once the write returned").isTrue();
    }

    @Test
    void a_handler_that_pauses_its_own_subscription_does_not_wait_for_an_earlier_write_of_another_subscription_and_the_pause_applies_once_that_write_closes() throws Exception {
        HeldHandler held = subscribeHeldHandler("other", Filter.type("NameWasChanged"));
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> {
            handled.add(event.getType());
            model.pauseSubscription("p");
        }));
        Thread earlierWrite = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameWasChanged"))).block());
        held.awaitEntered();

        CompletableFuture<?> pausingWrite = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block(), daemonThreads());

        assertThat(pausingWrite).as("the write whose handler paused its own subscription").succeedsWithin(Duration.ofSeconds(WAIT_SECONDS));
        assertThat(model.isPaused("p")).as("paused once its own write returned, while the earlier write is open").isFalse();
        held.release();
        assertThat(joined(earlierWrite)).as("the earlier write finished").isTrue();
        assertThat(handled).containsExactlyInAnyOrder("NameWasChanged", "NameDefined");
        assertThat(model.isPaused("p")).as("paused once the earlier write closed").isTrue();
    }

    @Test
    void a_stop_from_another_thread_refuses_a_new_write_and_waits_for_the_write_in_flight() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block());
        held.awaitEntered();
        Thread stopper = start(() -> model.stop());
        assertThat(waitingOrDone(stopper)).as("what the stopping thread does while the write is in flight").isEqualTo(Thread.State.WAITING);
        String streamIdAfterTheStop = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(streamIdAfterTheStop, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(eventStore.contains(streamIdAfterTheStop)).as("the refused write was stored").isFalse();
        held.release();
        assertThat(joined(writer)).as("the write in flight finished").isTrue();
        assertThat(joined(stopper)).as("the stop returned once the write closed").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isRunning()).as("running once the stop returned").isFalse();
    }

    @Test
    void while_a_pause_waits_a_new_write_its_subscription_matches_is_refused_and_one_it_does_not_match_goes_through() throws Exception {
        HeldHandler held = subscribeHeldHandler("orders-projection", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block());
        held.awaitEntered();
        Thread pauser = start(() -> model.pauseSubscription("orders-projection"));
        assertThat(waitingOrDone(pauser)).as("what the pausing thread does while the write is in flight").isEqualTo(Thread.State.WAITING);
        String matchingStreamId = UUID.randomUUID().toString();
        String otherStreamId = UUID.randomUUID().toString();

        StepVerifier.create(applicationService.execute(matchingStreamId, events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();
        StepVerifier.create(applicationService.execute(otherStreamId, events -> List.of(new TestEvent("NameWasChanged"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(eventStore.contains(matchingStreamId)).as("the refused write was stored").isFalse();
        assertThat(eventStore.contains(otherStreamId)).as("the write the pausing subscription does not match was stored").isTrue();
        assertThat(pauser.isAlive()).as("the pause is still waiting for the write in flight").isTrue();
        held.release();
        assertThat(joined(writer)).as("the write in flight finished").isTrue();
        assertThat(joined(pauser)).as("the pause returned once the write closed").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isPaused("orders-projection")).as("paused once the pause returned").isTrue();
    }

    @Test
    void a_pause_on_a_non_blocking_thread_does_not_block_it_and_applies_once_the_write_in_flight_closes() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block());
        held.awaitEntered();

        CompletableFuture<Void> pause = Mono.<Void>fromRunnable(() -> model.pauseSubscription("p")).subscribeOn(Schedulers.parallel()).toFuture();

        assertThat(pause).as("the pause on a non-blocking thread while the write is in flight").succeedsWithin(Duration.ofSeconds(WAIT_SECONDS));
        assertThat(model.isPaused("p")).as("paused while the write is in flight").isFalse();
        held.release();
        assertThat(joined(writer)).as("the write finished").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isPaused("p")).as("paused once the write closed").isTrue();
    }

    @Test
    void a_stop_on_a_non_blocking_thread_does_not_block_it_and_applies_once_the_write_in_flight_closes() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block());
        held.awaitEntered();

        CompletableFuture<Void> stop = Mono.<Void>fromRunnable(() -> model.stop()).subscribeOn(Schedulers.parallel()).toFuture();

        assertThat(stop).as("the stop on a non-blocking thread while the write is in flight").succeedsWithin(Duration.ofSeconds(WAIT_SECONDS));
        assertThat(model.isRunning()).as("running while the write is in flight").isTrue();
        held.release();
        assertThat(joined(writer)).as("the write finished").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isRunning()).as("running once the write closed").isFalse();
    }

    @Test
    void a_handler_that_stops_its_own_model_does_not_wait_for_its_own_write_and_the_stop_applies_once_the_write_closes() {
        List<Boolean> runningInsideTheHandler = new CopyOnWriteArrayList<>();
        model.subscribe("p", event -> Mono.fromRunnable(() -> {
            handled.add(event.getType());
            model.stop();
            runningInsideTheHandler.add(model.isRunning());
        }));
        CompletableFuture<?> write = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))).block(), daemonThreads());

        assertThat(write).as("the write whose handler stopped its own model").succeedsWithin(Duration.ofSeconds(WAIT_SECONDS));
        assertThat(handled).containsExactly("NameDefined");
        assertThat(runningInsideTheHandler).as("running inside the handler, while its write was open").containsExactly(true);
        assertThat(model.isRunning()).as("running once the write returned").isFalse();
    }

    @Test
    void a_write_built_while_the_model_is_stopped_goes_through_when_it_is_subscribed_to_after_the_model_started() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        String streamId = UUID.randomUUID().toString();
        AtomicReference<Mono<WriteResult>> write = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> write.set(applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")))));
        model.start();

        assertThat(thrown).as("what building the write threw").isNull();
        StepVerifier.create(write.get()).expectNextCount(1).verifyComplete();
        assertThat(eventStore.contains(streamId)).as("the write was stored").isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_built_while_the_model_runs_is_refused_when_it_is_subscribed_to_after_the_model_stopped() {
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        String streamId = UUID.randomUUID().toString();
        Mono<WriteResult> write = applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")));
        model.stop();

        StepVerifier.create(write)
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(eventStore.contains(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_built_while_the_model_is_stopped_goes_through_when_it_is_subscribed_to_after_the_model_started() {
        var dcbEventStore = new InMemoryReactiveDcbEventStore();
        var dcbApplicationService = dcbApplicationService(dcbEventStore);
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();
        AtomicReference<Mono<DcbAppendResult>> write = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> write.set(dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined")))));
        model.start();

        assertThat(thrown).as("what building the write threw").isNull();
        StepVerifier.create(write.get()).expectNextCount(1).verifyComplete();
        assertThat(dcbEventStore.stored()).as("events stored by the write").hasSize(1);
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_dcb_write_built_while_the_model_runs_is_refused_when_it_is_subscribed_to_after_the_model_stopped() {
        var dcbEventStore = new InMemoryReactiveDcbEventStore();
        var dcbApplicationService = dcbApplicationService(dcbEventStore);
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        Mono<DcbAppendResult> write = dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined")));
        model.stop();

        StepVerifier.create(write)
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(dcbEventStore.stored()).as("events stored by the refused write").isEmpty();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        var dcbEventStore = new InMemoryReactiveDcbEventStore();
        var dcbApplicationService = dcbApplicationService(dcbEventStore);
        model.subscribe("handler", event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.stop();

        StepVerifier.create(dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped"))
                .verify();

        assertThat(dcbEventStore.stored()).as("events stored by the refused write").isEmpty();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        var dcbEventStore = new InMemoryReactiveDcbEventStore();
        var dcbApplicationService = dcbApplicationService(dcbEventStore);
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add(event.getType())));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))))
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection"))
                .verify();

        assertThat(dcbEventStore.stored()).as("events stored by the refused write").isEmpty();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_does_not_match_goes_through() {
        var dcbEventStore = new InMemoryReactiveDcbEventStore();
        var dcbApplicationService = dcbApplicationService(dcbEventStore);
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> Mono.fromRunnable(() -> handled.add("orders-projection")));
        model.pauseSubscription("orders-projection");

        StepVerifier.create(dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameWasChanged"))))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(dcbEventStore.stored()).as("events stored by the write").hasSize(1);
        assertThat(handled).isEmpty();
    }

    private GenericDcbApplicationService<TestEvent> dcbApplicationService(DcbEventStore dcbEventStore) {
        return GenericDcbApplicationService.builder(dcbEventStore, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
    }

    private HeldHandler subscribeHeldHandler(String subscriptionId, Filter filter) {
        HeldHandler held = new HeldHandler();
        model.subscribe(subscriptionId, StreamSubscriptionFilter.filter(filter), event -> Mono.fromRunnable(() -> {
            handled.add(event.getType());
            held.holdTheFirstDelivery();
        }));
        return held;
    }

    // Holds the first delivery inside the handler, so the write that made it stays open until the test releases it
    private static final class HeldHandler {
        private final AtomicBoolean held = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void holdTheFirstDelivery() {
            if (held.compareAndSet(false, true)) {
                entered.countDown();
                try {
                    released.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void awaitEntered() throws InterruptedException {
            assertThat(entered.await(WAIT_SECONDS, TimeUnit.SECONDS)).as("the held write reached its handler").isTrue();
        }

        void release() {
            released.countDown();
        }
    }

    private static Thread start(Runnable action) {
        Thread thread = new Thread(action);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Executor daemonThreads() {
        return action -> start(action);
    }

    // Answers once the thread waits on a monitor or has ended, whichever comes first. A thread that blocks only on
    // the model's own wait is WAITING, so the answer tells a pause or a stop that waits from one that returned.
    private static Thread.State waitingOrDone(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        Thread.State state = thread.getState();
        while (state != Thread.State.WAITING && state != Thread.State.TERMINATED && System.nanoTime() < deadline) {
            Thread.sleep(5);
            state = thread.getState();
        }
        return state;
    }

    private static boolean joined(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        return !thread.isAlive();
    }

    // Keeps every appended event at its position, one based, and reads them back between the positions asked for.
    // It evaluates no condition, since these tests never append concurrently to what one of them read.
    private static final class InMemoryReactiveDcbEventStore implements DcbEventStore {
        private final List<CloudEvent> events = new CopyOnWriteArrayList<>();

        List<CloudEvent> stored() {
            return events;
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.fromSupplier(() -> {
                List<CloudEvent> all = List.copyOf(events);
                long after = options.afterPosition().orElse(0);
                long upTo = options.upToPosition().orElse(all.size());
                List<CloudEvent> slice = all.subList((int) Math.min(after, all.size()), (int) Math.min(upTo, all.size()));
                return new DcbEventStream(slice, all.size(), DcbConsistencyToken.of(criteria, all.size()));
            });
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> newEvents) {
            return Mono.fromSupplier(() -> {
                synchronized (events) {
                    long first = events.size() + 1;
                    events.addAll(newEvents);
                    return new DcbAppendResult(first, events.size(), newEvents.size());
                }
            });
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> newEvents, DcbAppendCondition condition) {
            return append(newEvents);
        }
    }

    private static final class InMemoryReactiveEventStore implements EventStore {
        private final ConcurrentHashMap<String, List<CloudEvent>> streams = new ConcurrentHashMap<>();

        boolean contains(String streamId) {
            return streams.containsKey(streamId);
        }

        @Override
        public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
            return Mono.fromSupplier(() -> {
                List<CloudEvent> all = streams.getOrDefault(streamId, List.of());
                List<CloudEvent> slice = all.stream().skip(skip).limit(limit).toList();
                return new EventStream<>() {
                    @Override
                    public String id() {
                        return streamId;
                    }

                    @Override
                    public long version() {
                        return all.size();
                    }

                    @Override
                    public Flux<CloudEvent> events() {
                        return Flux.fromIterable(slice);
                    }
                };
            });
        }

        @Override
        public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
            return write(streamId, events);
        }

        @Override
        public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
            return events.collectList().map(newEvents -> {
                List<CloudEvent> stream = streams.computeIfAbsent(streamId, id -> new ArrayList<>());
                long oldVersion = stream.size();
                stream.addAll(newEvents);
                return new WriteResult(streamId, oldVersion, stream.size());
            });
        }

        @Override
        public Mono<Boolean> exists(String streamId) {
            return Mono.fromSupplier(() -> streams.containsKey(streamId));
        }
    }

    private record TestEvent(String type) {
    }

    private static final class TestEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(domainEvent.type())
                    .build();
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return new TestEvent(cloudEvent.getType());
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            throw new UnsupportedOperationException();
        }
    }
}
