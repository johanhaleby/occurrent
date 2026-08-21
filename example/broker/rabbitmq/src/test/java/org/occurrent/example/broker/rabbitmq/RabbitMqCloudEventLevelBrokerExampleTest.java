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

package org.occurrent.example.broker.rabbitmq;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.springboot.blocking.OccurrentBlockingAnnotationConfiguration;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The CloudEvent-level half of the broker example. {@code RabbitMqCloudEventBridge} bridges a queue into a
 * {@code PushSubscriptionModel}, and a {@code @Projection(source = PUSH)} keeps a read model up to date from it.
 * Proves the two parts of ADR 133's contract that a fake in the middle cannot prove on its own. A handler that
 * throws does not lose the message, published straight to the broker through {@code RabbitMqCloudEventSink} rather
 * than through {@code CloudEventForwarder}, since the forwarder's own at-least-once retries could otherwise supply
 * a legal duplicate that satisfies the redelivery assertion without the bridge itself ever having redelivered
 * anything. A restarted consumer, the bridge and the push model, resumes from the broker instead of replaying its
 * whole history again, proven with the forwarder in front of it as a real application would run it. The forwarder
 * keeps running across that restart on purpose. Its own resumption is the {@code DurableSubscriptionModel}
 * checkpoint contract, already tested where the forwarder lives, and restarting it here would only duplicate that
 * coverage without showing anything specific to the consume side.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class RabbitMqCloudEventLevelBrokerExampleTest extends AbstractBrokerExampleTest {

    @Test
    void a_handler_that_throws_does_not_lose_the_message() throws Exception {
        CloudEventConverter<OrderEvent> converter = newConverter();
        CloudEventTypeMapper<OrderEvent> typeMapper = newTypeMapper();
        RabbitMqTopicExchangeDestinationResolver resolver = newResolver(typeMapper);
        MongoEventStore eventStore = newEventStore();

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel pushModel = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(rabbitConnection, pushModel, outcomeChannel, queue)
                .resolver(resolver)
                .pollInterval(Duration.ofMillis(50))
                .build();
             RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(rabbitConnection, resolver).build()) {

            // Published directly through the sink below, deliberately without a CloudEventForwarder in front of it.
            // The forwarder is at-least-once and can legally redeliver a lost confirm as a second physical copy of
            // the same event, which the redelivery assertion below could then satisfy without the bridge itself
            // ever having redelivered anything. Publishing straight to the broker guarantees exactly one physical
            // copy per event instead, so a matching save can only be the bridge's own redelivery. Forwarder
            // integration, and its own at-least-once retry behavior, is proven separately by the restart test below.

            // The read model store fails the very first write it is asked to make, whichever event that turns out
            // to be, then succeeds on every write after. If the bridge lost the message that failing write belonged
            // to instead of redelivering it, the order would never reach SHIPPED.
            AtomicBoolean failedOnce = new AtomicBoolean(false);
            // The exact view the failed save attempt tried to write, saved here so a later save can be checked for
            // content equality against it rather than merely counted, proving a genuine redelivery of the same
            // message rather than merely some other save.
            AtomicReference<OrderStatusProjection.OrderStatusView> failedAttemptView = new AtomicReference<>();
            AtomicInteger redeliveredSaveAttempts = new AtomicInteger();
            Map<String, OrderStatusProjection.OrderStatusView> store = new ConcurrentHashMap<>();
            ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(store::get, (id, value) -> {
                if (failedOnce.compareAndSet(false, true)) {
                    failedAttemptView.set(value);
                    throw new RuntimeException("Simulated read-model failure while saving " + id);
                }
                if (value.equals(failedAttemptView.get())) {
                    redeliveredSaveAttempts.incrementAndGet();
                }
                store.put(id, value);
            });

            CheckpointStorage pushCatchupMarker = new NativeMongoCheckpointStorage(mongoClient.getDatabase(databaseName), "push-catchup-checkpoints");

            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
            context.register(PropertiesConfig.class, OccurrentBlockingAnnotationConfiguration.class);
            context.registerBean("pushModel", PushSubscriptionModel.class, () -> pushModel);
            context.registerBean("pushCatchupMarker", CheckpointStorage.class, () -> pushCatchupMarker);
            context.registerBean("positionOrderedReader", PositionOrderedReader.class, () -> eventStore);
            context.registerBean("cloudEventConverter", CloudEventConverter.class, () -> converter);
            context.registerBean("viewStateRepository", ViewStateRepository.class, () -> repository);
            context.registerBean("orderProjectionHolder", NoLossProjectionHolder.class, NoLossProjectionHolder::new);
            try {
                context.refresh();

                String orderId = "order-" + UUID.randomUUID();
                eventStore.write(orderId, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderId, "Widget")));
                eventStore.write(orderId, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderId)));
                // Published directly and exactly once each, from the store's own stamped copies (streamid,
                // streamversion and position, which OrderStatusProjection reads off EventMetadata), not the bare
                // CloudEvent converter.toCloudEvent(...) built above and never stamped. See the comment above the
                // try block for why this bypasses the forwarder.
                eventStore.read(orderId).events().forEach(sink::publish);

                await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                        assertThat(store.get(orderId)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED"));

                // redeliveredSaveAttempts is what proves the specific failed delivery came back, not just that some
                // save happened again. With exactly one physical copy of each event ever on the broker, a matching
                // save can only be the bridge's own redelivery of the failed one, never a coincidental duplicate.
                assertThat(failedOnce).isTrue();
                assertThat(redeliveredSaveAttempts.get()).isGreaterThanOrEqualTo(1);
            } finally {
                context.close();
            }
        }
    }

    @Test
    void a_consumer_restart_resumes_from_the_broker_without_replaying_the_whole_history() throws Exception {
        CloudEventConverter<OrderEvent> converter = newConverter();
        CloudEventTypeMapper<OrderEvent> typeMapper = newTypeMapper();
        RabbitMqTopicExchangeDestinationResolver resolver = newResolver(typeMapper);
        MongoEventStore eventStore = newEventStore();

        NativeMongoSubscriptionModel forwarderSubscriptionModel = new NativeMongoSubscriptionModel(
                mongoClient.getDatabase(databaseName), EVENTS_COLLECTION, org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING,
                Executors.newVirtualThreadPerTaskExecutor());
        CheckpointStorage forwarderCheckpoints = new NativeMongoCheckpointStorage(mongoClient.getDatabase(databaseName), "forwarder-checkpoints");
        DurableSubscriptionModel forwarderSubscription = new DurableSubscriptionModel(forwarderSubscriptionModel, forwarderCheckpoints);
        // The forwarder and its subscription are built once, below, and run continuously through both boots.
        // Only the consumer side, the bridge, the push model and the projection's Spring context, is torn
        // down and rebuilt between them. The same catch-up-complete marker backs both boots, which is the
        // one thing a real consumer restart must preserve for resume (rather than replay) to be possible.
        CheckpointStorage pushCatchupMarker = new NativeMongoCheckpointStorage(mongoClient.getDatabase(databaseName), "push-catchup-checkpoints");
        // The read-model store also survives the restart below, by being the same Java object both boots share
        // rather than a fresh one per boot, so the assertions can tell "resumed with state intact" apart from
        // "started fresh and got lucky". That is also why the durable pushCatchupMarker above is safe to pair with
        // a plain map here despite the rule the two bootstrap classes document. This simulated restart keeps both
        // alive across the boundary, so neither ever outlives the other the way a real process restart would let
        // the marker outlive an in-memory map recreated from nothing.
        Map<String, OrderStatusProjection.OrderStatusView> store = new ConcurrentHashMap<>();
        ViewStateRepository<OrderStatusProjection.OrderStatusView, String> repository = ViewStateRepository.create(store::get, store::put);

        // Outer try/finally, not a bare statement after the block, so a failure anywhere inside
        // (either boot, either context, either bridge) still shuts the forwarder subscription down
        // rather than leaking its background subscription for the rest of the test run.
        try {
            try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(rabbitConnection, resolver).build()) {
                CloudEventForwarder forwarder = new CloudEventForwarder(forwarderSubscription, sink);
                forwarder.forward("forward-orders-restart");

                String orderIdBeforeRestart = "order-" + UUID.randomUUID();

                // Boot 1: build the queue, forward and process one order to completion, then tear the consumer down.
                // The bridge closes where this try-with-resources block ends, right after the context does in its own
                // nested finally, which is also exactly the point the deliberate teardown belongs at. Writing it this
                // way instead of two explicit close() calls after the await means a failure anywhere above, the
                // context registration, refresh(), the writes, or the await itself, still closes both rather than
                // leaking a running context and a live consumer for the rest of the test run.
                RoutingOutcomeChannel outcomeChannel1 = new RoutingOutcomeChannel();
                PushSubscriptionModel pushModel1 = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel1);
                try (RabbitMqCloudEventBridge bridge1 = RabbitMqCloudEventBridge.builder(rabbitConnection, pushModel1, outcomeChannel1, queue)
                        .resolver(resolver)
                        .pollInterval(Duration.ofMillis(50))
                        .build()) {
                    AnnotationConfigApplicationContext context1 = new AnnotationConfigApplicationContext();
                    context1.register(PropertiesConfig.class, OccurrentBlockingAnnotationConfiguration.class);
                    context1.registerBean("pushModel", PushSubscriptionModel.class, () -> pushModel1);
                    context1.registerBean("pushCatchupMarker", CheckpointStorage.class, () -> pushCatchupMarker);
                    context1.registerBean("positionOrderedReader", PositionOrderedReader.class, () -> eventStore);
                    context1.registerBean("cloudEventConverter", CloudEventConverter.class, () -> converter);
                    context1.registerBean("viewStateRepository", ViewStateRepository.class, () -> repository);
                    context1.registerBean("orderProjectionHolder", RestartProjectionHolder.class, RestartProjectionHolder::new);
                    try {
                        context1.refresh();

                        eventStore.write(orderIdBeforeRestart, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderIdBeforeRestart, "Widget")));
                        eventStore.write(orderIdBeforeRestart, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderIdBeforeRestart)));

                        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                                assertThat(store.get(orderIdBeforeRestart)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED"));
                    } finally {
                        context1.close();
                    }
                }

                // Written with the consumer already torn down and boot 2 not yet built, so this queues on the broker
                // exactly like a real restart leaves work behind. The forwarder keeps running (it never stopped), but
                // nothing is consuming queue until boot 2's bridge exists. Proving this arrives once boot 2 comes up,
                // without a full replay, is the other half of "resumes", not just that catch-up itself is skipped.
                String orderIdQueuedWhileConsumerWasDown = "order-" + UUID.randomUUID();
                OrderPlaced placedWhileConsumerWasDown = new OrderPlaced(UUID.randomUUID().toString(), orderIdQueuedWhileConsumerWasDown, "Widget");
                OrderShipped shippedWhileConsumerWasDown = new OrderShipped(UUID.randomUUID().toString(), orderIdQueuedWhileConsumerWasDown);
                eventStore.write(orderIdQueuedWhileConsumerWasDown, converter.toCloudEvent(placedWhileConsumerWasDown));
                eventStore.write(orderIdQueuedWhileConsumerWasDown, converter.toCloudEvent(shippedWhileConsumerWasDown));

                // Waited out here, before boot 2 exists, not left to a race between the forwarder's own background
                // publish and boot 2 becoming ready. Without this wait, boot 2 could come up before the forwarder had
                // published either event, and then just consume them live as they arrived, proving nothing about
                // backlog already sitting on the broker.
                //
                // Keyed on the two distinct event ids, not a raw count, since the forwarder is at-least-once and a
                // lost confirm can legally redeliver either one, which would satisfy a bare count of 2 without both
                // events actually being there. distinctEventIdsOnQueue dedupes by id, so any number of copies of
                // the same event still reads as one id, and this only turns green once both are genuinely present.
                //
                // Removing this wait was confirmed, not assumed, to open the race it guards against. Across five
                // runs with the wait deleted, the queue held only one of the two ids at the moment boot 2 started
                // rather than both. That does not turn the test's own final-state assertions red, because delivery
                // is at-least-once and eventually consistent either way, live or from backlog, the order still
                // reaches SHIPPED and the replay counter still reads zero. This wait is what exercises the backlog
                // path rather than the final state, which is why it is a direct assertion and not incidental to one.
                Set<String> expectedIdsWhileConsumerWasDown = Set.of(placedWhileConsumerWasDown.eventId(), shippedWhileConsumerWasDown.eventId());
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(distinctEventIdsOnQueue(queue)).isEqualTo(expectedIdsWhileConsumerWasDown));

                // Boot 2: a fresh model, a fresh bridge on the same queue, and a fresh Spring context, but the same
                // catch-up marker and the same read-model store. A reader that counts its own replay calls proves the
                // catch-up is skipped this time, not merely fast.
                CountingPositionOrderedReader countingReader = new CountingPositionOrderedReader(eventStore);
                RoutingOutcomeChannel outcomeChannel2 = new RoutingOutcomeChannel();
                PushSubscriptionModel pushModel2 = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel2);
                try (RabbitMqCloudEventBridge bridge2 = RabbitMqCloudEventBridge.builder(rabbitConnection, pushModel2, outcomeChannel2, queue)
                        .resolver(resolver)
                        .pollInterval(Duration.ofMillis(50))
                        .build()) {
                    AnnotationConfigApplicationContext context2 = new AnnotationConfigApplicationContext();
                    context2.register(PropertiesConfig.class, OccurrentBlockingAnnotationConfiguration.class);
                    context2.registerBean("pushModel", PushSubscriptionModel.class, () -> pushModel2);
                    context2.registerBean("pushCatchupMarker", CheckpointStorage.class, () -> pushCatchupMarker);
                    context2.registerBean("positionOrderedReader", PositionOrderedReader.class, () -> countingReader);
                    context2.registerBean("cloudEventConverter", CloudEventConverter.class, () -> converter);
                    context2.registerBean("viewStateRepository", ViewStateRepository.class, () -> repository);
                    context2.registerBean("orderProjectionHolder", RestartProjectionHolder.class, RestartProjectionHolder::new);
                    try {
                        context2.refresh();

                        // The default startup mode waits for the catch-up before refresh() returns, so if it had
                        // replayed, the counter would already be nonzero right here.
                        assertThat(countingReader.replays()).isZero();
                        assertThat(store.get(orderIdBeforeRestart)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED");

                        // The order queued while the consumer was down arrives now that boot 2's bridge is live,
                        // delivered from the broker's own backlog rather than from a replay, since the catch-up
                        // marker already said this feed was caught up before this order was ever written.
                        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                                assertThat(store.get(orderIdQueuedWhileConsumerWasDown)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED"));
                        assertThat(countingReader.replays()).isZero();

                        String orderIdAfterRestart = "order-" + UUID.randomUUID();
                        eventStore.write(orderIdAfterRestart, converter.toCloudEvent(new OrderPlaced(UUID.randomUUID().toString(), orderIdAfterRestart, "Gadget")));
                        eventStore.write(orderIdAfterRestart, converter.toCloudEvent(new OrderShipped(UUID.randomUUID().toString(), orderIdAfterRestart)));

                        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                                assertThat(store.get(orderIdAfterRestart)).extracting(OrderStatusProjection.OrderStatusView::status).isEqualTo("SHIPPED"));

                        assertThat(countingReader.replays()).isZero();
                    } finally {
                        context2.close();
                    }
                }
            }
        } finally {
            forwarderSubscription.shutdown();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(OccurrentProperties.class)
    static class PropertiesConfig {
    }

    static class NoLossProjectionHolder {
        @Projection(id = "order-status-no-loss", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<OrderStatusProjection.OrderStatusView, OrderEvent, String> projection() {
            return OrderStatusProjection.orderStatusProjection();
        }
    }

    static class RestartProjectionHolder {
        @Projection(id = "order-status-restart", source = Source.PUSH)
        org.occurrent.dsl.projection.Projection<OrderStatusProjection.OrderStatusView, OrderEvent, String> projection() {
            return OrderStatusProjection.orderStatusProjection();
        }
    }

    /** Counts how many times a catch-up replay actually reads from the store, so a skipped catch-up is provable. */
    private static final class CountingPositionOrderedReader implements PositionOrderedReader {
        private final PositionOrderedReader delegate;
        private final AtomicInteger replayCount = new AtomicInteger();

        CountingPositionOrderedReader(PositionOrderedReader delegate) {
            this.delegate = delegate;
        }

        int replays() {
            return replayCount.get();
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(org.occurrent.filter.Filter filter, org.occurrent.eventstore.api.PositionRange range) {
            replayCount.incrementAndGet();
            return delegate.readInPositionOrder(filter, range);
        }

        @Override
        public long currentPosition() {
            return delegate.currentPosition();
        }

        @Override
        public boolean writesPosition() {
            return delegate.writesPosition();
        }
    }
}
