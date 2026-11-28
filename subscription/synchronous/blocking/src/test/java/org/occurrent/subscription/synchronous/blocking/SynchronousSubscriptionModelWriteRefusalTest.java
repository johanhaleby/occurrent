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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * A synchronous handler has no replay, so a write it does not see is lost to it. The application service therefore
 * asks the synchronous model before it writes, and the model refuses the write while it is stopped or when a paused
 * subscription matches the events. These tests drive the real application service over a real model, since the
 * refusal is the two working together. The model is never given to the test extension that turns the refusals off.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteRefusalTest {

    private static final Tag TAG = Tag.parse("name:1");
    private static final long WAIT_SECONDS = 10;

    private InMemoryEventStore eventStore;
    private SynchronousSubscriptionModel model;
    private List<String> handled;
    private GenericApplicationService<TestEvent> applicationService;

    @BeforeEach
    void initialize() {
        eventStore = new InMemoryEventStore();
        model = new SynchronousSubscriptionModel();
        handled = new CopyOnWriteArrayList<>();
        applicationService = GenericApplicationService.builder(eventStore, new TestEventConverter())
                .synchronousSubscriptions(model)
                .build();
    }

    @Test
    void a_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped");
        assertThat(eventStore.exists(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_after_the_model_is_stopped_goes_through_when_no_handler_is_registered() {
        model.stop();
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_after_the_model_is_started_again_goes_through_and_reaches_the_handler() {
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();
        model.start();
        String streamId = UUID.randomUUID().toString();

        applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")));

        assertThat(eventStore.exists(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.exists(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_that_a_paused_subscription_does_not_match_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add("orders-projection"));
        model.subscribe("other-projection", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> handled.add("other-projection"));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameWasChanged"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists(streamId)).as("the write was stored").isTrue();
        assertThat(handled).containsExactly("other-projection");
    }

    @Test
    void a_write_goes_through_once_the_paused_subscription_is_resumed() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        model.resumeSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")));

        assertThat(eventStore.exists(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_to_the_stream_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> applicationService.execute("x", events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.exists("x")).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_to_another_stream_than_the_one_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamId("x")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> applicationService.execute("y", events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists("y")).as("the write was stored").isTrue();
        assertThat(eventStore.exists("x")).as("the other stream was written").isFalse();
    }

    @Test
    void a_write_that_gives_an_event_the_stream_version_a_paused_subscription_filters_on_is_refused_and_stores_nothing() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(1)), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.exists(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_that_gives_no_event_the_stream_version_a_paused_subscription_filters_on_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.streamVersion(2)), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the refused write").isEmpty();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_does_not_match_goes_through() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add("orders-projection"));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameWasChanged"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the write").hasSize(1);
    }

    @Test
    void a_dcb_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped");
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the refused write").isEmpty();
    }

    @Test
    void a_pause_from_another_thread_waits_for_the_write_in_flight_and_the_paused_subscription_still_gets_its_event() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));
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
    void a_handler_that_pauses_its_own_subscription_does_not_wait_for_its_own_write_and_the_pause_applies_once_the_write_closes() throws Exception {
        List<Boolean> pausedInsideTheHandler = new CopyOnWriteArrayList<>();
        model.subscribe("p", event -> {
            handled.add(event.getType());
            model.pauseSubscription("p");
            pausedInsideTheHandler.add(model.isPaused("p"));
        });
        CompletableFuture<?> write = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))), daemonThreads());

        assertThat(write).as("the write whose handler paused its own subscription").succeedsWithin(Duration.ofSeconds(10));
        assertThat(handled).containsExactly("NameDefined");
        assertThat(pausedInsideTheHandler).as("paused inside the handler, while its write was open").containsExactly(false);
        assertThat(model.isPaused("p")).as("paused once the write returned").isTrue();
    }

    @Test
    void a_handler_that_pauses_its_own_subscription_does_not_wait_for_an_earlier_write_of_another_subscription_and_the_pause_applies_once_that_write_closes() throws Exception {
        HeldHandler held = subscribeHeldHandler("other", Filter.type("NameWasChanged"));
        model.subscribe("p", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> {
            handled.add(event.getType());
            model.pauseSubscription("p");
        });
        Thread earlierWrite = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameWasChanged"))));
        held.awaitEntered();

        CompletableFuture<?> pausingWrite = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))), daemonThreads());

        assertThat(pausingWrite).as("the write whose handler paused its own subscription").succeedsWithin(Duration.ofSeconds(10));
        assertThat(model.isPaused("p")).as("paused once its own write returned, while the earlier write is open").isFalse();
        held.release();
        assertThat(joined(earlierWrite)).as("the earlier write finished").isTrue();
        assertThat(handled).containsExactlyInAnyOrder("NameWasChanged", "NameDefined");
        assertThat(model.isPaused("p")).as("paused once the earlier write closed").isTrue();
    }

    @Test
    void a_handler_that_stops_its_own_model_does_not_wait_for_its_own_write_and_the_stop_applies_once_the_write_closes() {
        List<Boolean> runningInsideTheHandler = new CopyOnWriteArrayList<>();
        model.subscribe("p", event -> {
            handled.add(event.getType());
            model.stop();
            runningInsideTheHandler.add(model.isRunning());
        });
        CompletableFuture<?> write = CompletableFuture.runAsync(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))), daemonThreads());

        assertThat(write).as("the write whose handler stopped its own model").succeedsWithin(Duration.ofSeconds(10));
        assertThat(handled).containsExactly("NameDefined");
        assertThat(runningInsideTheHandler).as("running inside the handler, while its write was open").containsExactly(true);
        assertThat(model.isRunning()).as("running once the write returned").isFalse();
    }

    @Test
    void a_stop_from_another_thread_refuses_a_new_write_and_waits_for_the_write_in_flight() throws Exception {
        HeldHandler held = subscribeHeldHandler("p", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));
        held.awaitEntered();
        Thread stopper = start(() -> model.stop());
        assertThat(waitingOrDone(stopper)).as("what the stopping thread does while the write is in flight").isEqualTo(Thread.State.WAITING);
        String streamIdAfterTheStop = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamIdAfterTheStop, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped");
        assertThat(eventStore.exists(streamIdAfterTheStop)).as("the refused write was stored").isFalse();
        held.release();
        assertThat(joined(writer)).as("the write in flight finished").isTrue();
        assertThat(joined(stopper)).as("the stop returned once the write closed").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isRunning()).as("running once the stop returned").isFalse();
    }

    @Test
    void while_a_pause_waits_a_new_write_its_subscription_matches_is_refused_and_one_it_does_not_match_goes_through() throws Exception {
        HeldHandler held = subscribeHeldHandler("orders-projection", Filter.type("NameDefined"));
        Thread writer = start(() -> applicationService.execute(UUID.randomUUID().toString(), events -> List.of(new TestEvent("NameDefined"))));
        held.awaitEntered();
        Thread pauser = start(() -> model.pauseSubscription("orders-projection"));
        assertThat(waitingOrDone(pauser)).as("what the pausing thread does while the write is in flight").isEqualTo(Thread.State.WAITING);
        String matchingStreamId = UUID.randomUUID().toString();
        String otherStreamId = UUID.randomUUID().toString();

        Throwable matching = catchThrowable(() -> applicationService.execute(matchingStreamId, events -> List.of(new TestEvent("NameDefined"))));
        Throwable other = catchThrowable(() -> applicationService.execute(otherStreamId, events -> List.of(new TestEvent("NameWasChanged"))));

        assertThat(matching).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.exists(matchingStreamId)).as("the refused write was stored").isFalse();
        assertThat(other).isNull();
        assertThat(eventStore.exists(otherStreamId)).as("the write the pausing subscription does not match was stored").isTrue();
        assertThat(pauser.isAlive()).as("the pause is still waiting for the write in flight").isTrue();
        held.release();
        assertThat(joined(writer)).as("the write in flight finished").isTrue();
        assertThat(joined(pauser)).as("the pause returned once the write closed").isTrue();
        assertThat(handled).containsExactly("NameDefined");
        assertThat(model.isPaused("orders-projection")).as("paused once the pause returned").isTrue();
    }

    private HeldHandler subscribeHeldHandler(String subscriptionId, Filter filter) {
        HeldHandler held = new HeldHandler();
        model.subscribe(subscriptionId, StreamSubscriptionFilter.filter(filter), event -> {
            handled.add(event.getType());
            held.holdTheFirstDelivery();
        });
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

    private GenericDcbApplicationService<TestEvent> dcbApplicationService() {
        return GenericDcbApplicationService.builder(eventStore, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
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
