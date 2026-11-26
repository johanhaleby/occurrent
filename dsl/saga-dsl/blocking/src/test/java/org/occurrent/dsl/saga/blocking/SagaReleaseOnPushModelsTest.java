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

package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A push saga quarantines like any other, and releasing it replays the events it holds, so no subscription model has to
 * retain history for it, <a href="https://github.com/johanhaleby/occurrent/issues/918">#918</a> and
 * <a href="https://github.com/johanhaleby/occurrent/issues/952">#952</a>. Driven through {@link SagaRunner} on the models
 * a push saga runs on, with events written to an in-memory event store that feeds them, so each event reaches the saga
 * as a subscription hands it over.
 */
@DisplayName("Releasing a quarantined push saga instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseOnPushModelsTest {

    private static final String ORDER = "o1";
    // Short, so the failing event is past its budget on the next redelivery. The redeliveries are awaited, never timed
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMillis(1));

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record Pack(String orderId, String item) {
    }

    record Order(String orderId, List<String> items) {
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    // The items whose reaction throws for as long as they are in here
    private final Set<String> failing = ConcurrentHashMap.newKeySet();

    private Saga<OrderEvent, Order, Pack> saga() {
        return Saga.<OrderEvent, Order, Pack>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final SagaStateStore<Order> store = SagaStateStore.inMemory();
    private final List<Pack> packed = new CopyOnWriteArrayList<>();
    private final List<CloudEvent> published = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
    }

    private SagaSubscription run(Subscribable model, SagaRunnerConfig config) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, Pack>agnostic(model, converter)
                .run("orders", saga(), store, packed::add, null, config);
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    private OrderEvent placed() {
        return new OrderPlaced(nextEventId(), ORDER);
    }

    private OrderEvent added(String item) {
        return new ItemAdded(nextEventId(), ORDER, item);
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<Order> instance() {
        return SagaEnvelopes.of(store, null).find(ORDER).orElseThrow();
    }

    private List<SkippedInput> held() {
        return ((SagaStateStoreSkippedInputs) store).findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE);
    }

    private static String streamKey(CloudEvent event) {
        return event.getExtension(OccurrentCloudEventExtension.STREAM_ID) + "@" + event.getExtension(OccurrentCloudEventExtension.STREAM_VERSION);
    }

    // The write fails because the saga's reaction throws inside the listener that feeds it, which the store lets out
    private void writeFailing(Runnable write) {
        assertThat(catchThrowable(write::run)).as("control: the delivery fails").isInstanceOf(IllegalStateException.class);
    }

    // The source offers the failing event again until the instance is past its budget and quarantined on it
    private void redeliverUntilQuarantined(CloudEvent failed, Consumer<CloudEvent> feed) {
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            catchThrowable(() -> feed.accept(failed));
            return instance().status() == SagaStatus.QUARANTINED;
        });
    }

    private void releaseAndExpectEveryInputApplied(SagaSubscription subscription, String... items) {
        failing.clear();

        ReleaseResult result = subscription.release(ORDER);

        SagaEnvelope<Order> released = instance();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(released.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(released.state().items()).containsExactly(items),
                () -> assertThat(packed).extracting(Pack::item).containsExactly(items),
                () -> assertThat(released.failure()).isNull()
        );
    }

    @Nested
    @DisplayName("on a push subscription model with no catch-up")
    class OnAPushSubscriptionModelWithNoCatchUp {

        private final PushSubscriptionModel pushModel = new PushSubscriptionModel();
        private final InMemoryEventStore eventStore = new InMemoryEventStore(pushModel::accept);

        private CloudEvent write(OrderEvent event) {
            eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
            return eventStore.read(ORDER).eventList().getLast();
        }

        @Test
        void applies_the_input_it_was_quarantined_on_and_then_the_three_it_skipped_in_order_and_ends_active() {
            SagaSubscription subscription = run(pushModel, CONFIG);
            write(placed());
            failing.add("poison");
            writeFailing(() -> write(added("poison")));
            CloudEvent k = eventStore.read(ORDER).eventList().getLast();
            redeliverUntilQuarantined(k, pushModel::accept);

            write(added("a"));
            write(added("b"));
            write(added("c"));

            assertThat(held()).as("control: K and the three skipped inputs are held").extracting(SkippedInput::key).hasSize(4);
            releaseAndExpectEveryInputApplied(subscription, "poison", "a", "b", "c");
        }
    }

    @Nested
    @DisplayName("on a catch-up then push subscription model starting from the event store")
    class OnACatchupThenPushSubscriptionModelStartingFromTheEventStore {

        private final PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        private final InMemoryEventStore eventStore = new InMemoryEventStore(liveFeed::accept);
        private final CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(eventStore, liveFeed, null);

        private CloudEvent write(OrderEvent event) {
            eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
            return eventStore.read(ORDER).eventList().getLast();
        }

        @Test
        void applies_the_input_it_was_quarantined_on_and_then_the_three_it_skipped_in_order_and_ends_active() {
            // The instance is started by the replay of the event store's history, and everything after it is live
            write(placed());
            SagaSubscription subscription = run(model, CONFIG);
            assertThat(instance().state()).as("control: the replay started the instance").isEqualTo(new Order(ORDER, List.of()));
            failing.add("poison");
            writeFailing(() -> write(added("poison")));
            CloudEvent k = eventStore.read(ORDER).eventList().getLast();
            redeliverUntilQuarantined(k, liveFeed::accept);

            write(added("a"));
            write(added("b"));
            write(added("c"));

            assertThat(held()).as("control: K and the three skipped inputs are held").extracting(SkippedInput::key).hasSize(4);
            releaseAndExpectEveryInputApplied(subscription, "poison", "a", "b", "c");
        }
    }

    @Nested
    @DisplayName("on an event appended through the DCB path")
    class OnAnEventAppendedThroughTheDcbPath {

        private final PushSubscriptionModel pushModel = new PushSubscriptionModel();
        private final InMemoryEventStore eventStore = new InMemoryEventStore(events -> {
            published.addAll(events);
            pushModel.accept(events);
        });

        private CloudEvent appendThroughDcb(OrderEvent event) {
            CloudEvent tagged = DcbCloudEvents.withTags(converter.toCloudEvents(List.of(event)).getFirst(), Set.of(Tag.of("order", ORDER)));
            eventStore.append(List.of(tagged));
            return published.getLast();
        }

        // Events appended through DCB are placed in a storage stream like any other, so they reach the saga with the
        // stream key a redelivery is recognised by, under either detection mode, and an instance holds them by that key
        @ParameterizedTest
        @EnumSource(RedeliveryDetection.class)
        void holds_the_event_a_quarantined_instance_skips_with_the_stream_key_it_was_stored_with_and_applies_it_on_release(RedeliveryDetection detection) {
            SagaSubscription subscription = run(pushModel, CONFIG.withRedeliveryDetection(detection));
            appendThroughDcb(placed());
            failing.add("poison");
            writeFailing(() -> appendThroughDcb(added("poison")));
            CloudEvent k = published.getLast();
            redeliverUntilQuarantined(k, pushModel::accept);

            CloudEvent a = appendThroughDcb(added("a"));
            CloudEvent b = appendThroughDcb(added("b"));

            List<SkippedInput> held = held();
            assertAll(
                    () -> assertThat(DcbCloudEvents.isDcbEvent(a)).as("control: a was appended through DCB").isTrue(),
                    () -> assertThat(a.getExtension(OccurrentCloudEventExtension.STREAM_ID)).as("stored with a streamid").isNotNull(),
                    () -> assertThat(a.getExtension(OccurrentCloudEventExtension.STREAM_VERSION)).as("stored with a streamversion").isNotNull(),
                    () -> assertThat(held).extracting(SkippedInput::key).containsExactly(streamKey(k), streamKey(a), streamKey(b)),
                    () -> assertThat(held).extracting(input -> input.event().getId()).containsExactly(k.getId(), a.getId(), b.getId())
            );
            releaseAndExpectEveryInputApplied(subscription, "poison", "a", "b");
        }
    }
}
