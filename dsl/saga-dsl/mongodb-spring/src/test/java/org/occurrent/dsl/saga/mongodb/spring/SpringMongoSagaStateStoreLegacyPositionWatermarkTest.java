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

package org.occurrent.dsl.saga.mongodb.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.RedeliveryDetection;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. 0.31.0 to 0.34.0 stored the highest global position a saga had applied in a {@code positionWatermark}
 * field, and 0.35.0 no longer reads it. A document written that way has to keep loading, the executor has to ignore the
 * number in it, and the first save after that has to take the field away. A failure record keyed {@code position:N} is
 * the other thing those versions wrote, and it has to load too.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreLegacyPositionWatermarkTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-legacy-position-watermark";
    private static final String SAGA_ID = "order-1";
    private static final String POSITION_WATERMARK = "positionWatermark";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration BUDGET = Duration.ofMinutes(5);

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private MongoOperations mongoOperations;
    private SagaStateStore<String> stateStore;
    private final List<String> dispatched = new CopyOnWriteArrayList<>();
    private volatile boolean paymentReactionFails;
    private @Nullable SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        // RetryStrategy.none(), so a failing event is offered once and the stored document is what the test reads back
        // rather than what a later redelivery made of it.
        subscriptionModel = new InMemorySubscriptionModel(RetryStrategy.none());
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-lpw-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    /** Starts on an order being placed, which asks for the stock, and a payment ships it, or fails when told to. */
    private Saga<OrderEvent, String, String> orderFulfillment() {
        return Saga.<OrderEvent, String, String>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "awaiting-payment")
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue("reserve-stock")))
                .evolve(PaymentReserved.class, (state, e) -> "paid")
                .react(PaymentReserved.class, (state, e) -> {
                    if (paymentReactionFails) {
                        throw new IllegalStateException("the payment cannot be handled");
                    }
                    return List.of(SagaEffect.issue("ship-order"));
                })
                .build();
    }

    /**
     * What the saga sees. A feed over the in-memory subscription model that also answers that it retains everything,
     * because {@code SagaRunner} switches quarantine off for a model it cannot ask, and that can hand the events over
     * without the stream key the event store wrote them with.
     */
    private record Feed(InMemorySubscriptionModel delegate, UnaryOperator<CloudEvent> deliveredAs) implements Subscribable, HistoryRetainingSubscriptions {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, event -> action.accept(deliveredAs.apply(event)));
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    private void runSagaOn(UnaryOperator<CloudEvent> deliveredAs, SagaRunnerConfig config) {
        subscription = SagaRunner.<OrderEvent, String>agnostic(new Feed(subscriptionModel, deliveredAs), converter)
                .run("orders", orderFulfillment(), stateStore, dispatched::add, null, config);
    }

    private void runSaga() {
        runSagaOn(UnaryOperator.identity(), SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET));
    }

    private void write(OrderEvent... events) {
        eventStore.write(SAGA_ID, converter.toCloudEvents(List.of(events)));
    }

    /** Exactly the fields {@code SpringMongoSagaStateStore} wrote in 0.34.0 for an active instance, which includes the position watermark. */
    private static Document documentWrittenBy0_34(long positionWatermark) {
        return new Document("_id", SAGA_ID)
                .append("status", "ACTIVE")
                .append("version", 1L)
                .append("timers", List.of())
                .append("streamWatermarks", new Document(SAGA_ID, 1L))
                .append(POSITION_WATERMARK, positionWatermark)
                .append("createdAt", NOW.toEpochMilli())
                .append("updatedAt", NOW.toEpochMilli())
                .append("state", "awaiting-payment");
    }

    private static Document withAFailureRecordFor(Document document, String input, long position, Instant firstFailedAt) {
        return document
                .append("failureInput", input)
                .append("failureFirstFailedAt", firstFailedAt.toEpochMilli())
                .append("failureType", IllegalStateException.class.getName())
                .append("failurePosition", position)
                .append("failureMessage", "boom");
    }

    private void insertDirectly(Document document) {
        mongoOperations.insert(document, COLLECTION);
    }

    private Document rawDocument() {
        return requireNonNull(mongoOperations.findOne(Query.query(where("_id").is(SAGA_ID)), Document.class, COLLECTION));
    }

    @Test
    void the_document_written_by_0_34_has_the_fields_the_store_writes_for_an_instance_with_a_position_watermark() {
        // Checks the fixture used below against what the store really writes.
        stateStore.compareAndSave(SAGA_ID, new SagaEnvelope<>(SAGA_ID, "awaiting-payment", SagaStatus.ACTIVE, 1, List.of(),
                Map.of(SAGA_ID, 1L), 7L, NOW, NOW, null, null, true, null), 0);

        Document writtenByTheStore = rawDocument();

        assertThat(documentWrittenBy0_34(7L)).isEqualTo(writtenByTheStore);
    }

    @Test
    void loads_a_document_written_in_the_0_34_shape_and_hands_back_the_stored_number() {
        insertDirectly(documentWrittenBy0_34(7L));

        SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

        assertAll(
                () -> assertThat(read.positionWatermark()).isEqualTo(7L),
                () -> assertThat(read.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(read.state()).isEqualTo("awaiting-payment"),
                () -> assertThat(read.streamWatermarks()).isEqualTo(Map.of(SAGA_ID, 1L))
        );
    }

    @Test
    void loads_a_stored_number_as_large_as_a_long_can_be() {
        insertDirectly(documentWrittenBy0_34(Long.MAX_VALUE));

        SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

        assertThat(read.positionWatermark()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void reads_the_stored_number_without_the_state_too() {
        insertDirectly(documentWrittenBy0_34(7L));

        SagaEnvelope<String> read = stateStore.findWithoutState(SAGA_ID).orElseThrow();

        assertThat(read.positionWatermark()).isEqualTo(7L);
    }

    @Test
    void loads_a_failure_record_keyed_by_a_position() {
        insertDirectly(withAFailureRecordFor(documentWrittenBy0_34(7L), "position:7", 7L, NOW.minusSeconds(300)));

        SagaEnvelope<String> read = stateStore.find(SAGA_ID).orElseThrow();

        assertThat(read.failure()).isEqualTo(new SagaFailure("position:7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom"));
    }

    @Test
    void ignores_the_stored_number_and_removes_it_with_the_save_that_processes_a_stream_keyed_event() {
        insertDirectly(documentWrittenBy0_34(Long.MAX_VALUE));
        runSaga();

        // The instance has applied stream version 1, so only the payment at version 2 is new to it.
        write(new OrderPlaced(SAGA_ID), new PaymentReserved(SAGA_ID));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Document raw = rawDocument();
            assertAll(
                    () -> assertThat(dispatched).containsExactly("ship-order"),
                    () -> assertThat(raw.get("state")).isEqualTo("paid"),
                    () -> assertThat(raw.get("streamWatermarks")).isEqualTo(new Document(SAGA_ID, 2L)),
                    () -> assertThat(raw.containsKey(POSITION_WATERMARK)).isFalse()
            );
        });
    }

    @Test
    void ignores_a_stored_number_above_every_position_for_an_event_that_carries_only_a_position() {
        insertDirectly(documentWrittenBy0_34(Long.MAX_VALUE));
        runSagaOn(positionOnly(5), SagaRunnerConfig.defaults().withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));

        write(new PaymentReserved(SAGA_ID));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Document raw = rawDocument();
            assertAll(
                    () -> assertThat(dispatched).containsExactly("ship-order"),
                    () -> assertThat(raw.get("state")).isEqualTo("paid"),
                    () -> assertThat(raw.containsKey(POSITION_WATERMARK)).isFalse()
            );
        });
    }

    @Test
    void removes_the_stored_number_with_the_save_that_records_a_failure() {
        insertDirectly(documentWrittenBy0_34(7L));
        paymentReactionFails = true;
        runSaga();

        write(new OrderPlaced(SAGA_ID), new PaymentReserved(SAGA_ID));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Document raw = rawDocument();
            assertAll(
                    () -> assertThat(raw.getString("failureInput")).isEqualTo(SAGA_ID + "@2"),
                    () -> assertThat(raw.getString("status")).isEqualTo("ACTIVE"),
                    () -> assertThat(raw.get("state")).isEqualTo("awaiting-payment"),
                    () -> assertThat(raw.containsKey(POSITION_WATERMARK)).isFalse()
            );
        });
    }

    @Test
    void starts_a_fresh_budget_for_a_failure_after_a_position_keyed_failure_record_instead_of_quarantining_at_once() {
        insertDirectly(withAFailureRecordFor(documentWrittenBy0_34(7L), "position:7", 7L, NOW));
        paymentReactionFails = true;
        Instant before = Instant.now();
        runSaga();

        write(new OrderPlaced(SAGA_ID), new PaymentReserved(SAGA_ID));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            SagaEnvelope<String> instance = stateStore.find(SAGA_ID).orElseThrow();
            assertAll(
                    () -> assertThat(instance.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure().input()).isEqualTo(SAGA_ID + "@2"),
                    () -> assertThat(instance.failure().firstFailedAt()).isAfterOrEqualTo(before.truncatedTo(ChronoUnit.MILLIS))
            );
        });
    }

    @Test
    void clears_a_position_keyed_failure_record_with_the_next_successful_stream_keyed_delivery() {
        insertDirectly(withAFailureRecordFor(documentWrittenBy0_34(7L), "position:7", 7L, NOW));
        runSaga();

        write(new OrderPlaced(SAGA_ID), new PaymentReserved(SAGA_ID));

        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Document raw = rawDocument();
            assertAll(
                    () -> assertThat(dispatched).containsExactly("ship-order"),
                    () -> assertThat(raw.containsKey("failureInput")).isFalse(),
                    () -> assertThat(raw.containsKey("failureFirstFailedAt")).isFalse(),
                    () -> assertThat(raw.containsKey("failurePosition")).isFalse(),
                    () -> assertThat(raw.containsKey(POSITION_WATERMARK)).isFalse()
            );
        });
    }

    // What a feed that drops the stream extensions but keeps a position hands the saga.
    private static UnaryOperator<CloudEvent> positionOnly(long position) {
        return event -> OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1(event)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_ID)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_VERSION)
                .build(), position);
    }
}
