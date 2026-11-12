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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Docker-based. An instance keeps the events that arrived before its start, and the executor has already acknowledged
 * each of them to the subscription, so a store that drops one loses it. This checks that the events survive a round trip
 * through real MongoDB, that the reads and writes that leave the state alone treat them as part of the instance, and that
 * a runner on this store applies an event that arrived before the start.
 */
@DisplayName("SpringMongoSagaStateStore early events")
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreEarlyEventsTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-early-events";
    private static final String ORDER = "order-1";

    private MongoOperations mongoOperations;
    private SagaStateStore<String> store;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-early-" + UUID.randomUUID()));
        mongoOperations = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        store = new SpringMongoSagaStateStore<>(mongoOperations, COLLECTION, String.class);
    }

    private static CloudEvent cloudEvent(String id, String payload) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("test.event")
                .withDataContentType("application/json")
                .withData(("{\"payload\":\"" + payload + "\"}").getBytes(StandardCharsets.UTF_8))
                .withExtension(OccurrentCloudEventExtension.occurrent("orders-1", 2))
                .build();
    }

    /** Everything a CloudEvent carries, as text, because the data comes back as parsed JSON rather than the bytes it was built from. */
    private static List<String> summaries(List<CloudEvent> events) {
        return events.stream().map(event -> {
            Map<String, String> extensions = new TreeMap<>();
            event.getExtensionNames().forEach(name -> extensions.put(name, String.valueOf(event.getExtension(name))));
            return event.getId() + "|" + event.getSource() + "|" + event.getType() + "|" + new String(requireNonNull(event.getData()).toBytes(), StandardCharsets.UTF_8) + "|" + extensions;
        }).toList();
    }

    /** An instance that has not started, which is the only kind that keeps events. */
    private static SagaEnvelope<String> unstartedKeeping(List<CloudEvent> early) {
        return new SagaEnvelope<>(ORDER, "initial", SagaStatus.ACTIVE, 1, List.of(), Map.of(), null, NOW, NOW, null, null,
                false, null, Map.of(), early);
    }

    private static SagaFailure failure() {
        return new SagaFailure("order-1@2", 2L, NOW, IllegalStateException.class.getName(), "boom");
    }

    private Document storedDocument() {
        return requireNonNull(mongoOperations.findById(ORDER, Document.class, COLLECTION));
    }

    @Test
    void the_kept_events_round_trip_in_the_order_they_arrived() {
        // Given
        List<CloudEvent> early = List.of(cloudEvent("b", "second arrived first"), cloudEvent("a", "first arrived second"));
        store.compareAndSave(ORDER, unstartedKeeping(early), 0);

        // When
        SagaEnvelope<String> read = store.find(ORDER).orElseThrow();

        // Then
        assertThat(summaries(read.early())).isEqualTo(summaries(early));
    }

    @Test
    void an_instance_that_keeps_nothing_writes_no_early_field() {
        // Given
        store.compareAndSave(ORDER, unstartedKeeping(List.of()), 0);

        // When
        Document document = storedDocument();

        // Then
        assertThat(document.containsKey("early")).isFalse();
    }

    @Test
    void the_kept_events_are_stored_as_cloud_events_json_strings() {
        // Given
        store.compareAndSave(ORDER, unstartedKeeping(List.of(cloudEvent("a", "x"))), 0);

        // When
        Object early = storedDocument().get("early");

        // Then
        assertThat(early).asInstanceOf(LIST).singleElement().asString().contains("\"id\":\"a\"");
    }

    @Test
    void a_document_without_the_field_reads_back_as_an_instance_that_keeps_nothing() {
        // Given
        store.compareAndSave(ORDER, unstartedKeeping(List.of(cloudEvent("a", "x"))), 0);
        mongoOperations.updateFirst(Query.query(where("_id").is(ORDER)), new Update().unset("early"), COLLECTION);

        // When
        SagaEnvelope<String> read = store.find(ORDER).orElseThrow();

        // Then
        assertThat(read.early()).isEmpty();
    }

    @Test
    void reading_without_the_state_returns_the_kept_events() {
        // Given
        List<CloudEvent> early = List.of(cloudEvent("a", "x"), cloudEvent("b", "y"));
        store.compareAndSave(ORDER, unstartedKeeping(early), 0);

        // When
        SagaEnvelope<String> read = store.findWithoutState(ORDER).orElseThrow();

        // Then
        assertThat(summaries(read.early())).isEqualTo(summaries(early));
    }

    @Test
    void a_failure_written_without_the_state_on_an_instance_that_has_not_started_keeps_its_events() {
        // Given
        List<CloudEvent> early = List.of(cloudEvent("a", "x"), cloudEvent("b", "y"));
        store.compareAndSave(ORDER, unstartedKeeping(early), 0);
        SagaEnvelope<String> loaded = store.findWithoutState(ORDER).orElseThrow();
        SagaEnvelope<String> withFailure = new SagaEnvelope<>(ORDER, loaded.state(), loaded.status(), loaded.version() + 1, loaded.timers(),
                loaded.streamWatermarks(), loaded.positionWatermark(), loaded.createdAt(), NOW, null, null, loaded.started(), failure(),
                loaded.streamMarks(), loaded.early());

        // When
        boolean saved = store.compareAndSaveWithoutState(ORDER, withFailure, loaded.version());

        // Then
        SagaEnvelope<String> read = store.find(ORDER).orElseThrow();
        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(read.failure()).isEqualTo(failure()),
                () -> assertThat(summaries(read.early())).isEqualTo(summaries(early)),
                () -> assertThat(read.state()).isEqualTo("initial")
        );
    }

    @Test
    void saving_without_the_state_sets_the_kept_events_the_stored_instance_did_not_have() {
        // Given
        store.compareAndSave(ORDER, unstartedKeeping(List.of()), 0);
        List<CloudEvent> early = List.of(cloudEvent("a", "x"));

        // When
        boolean saved = store.compareAndSaveWithoutState(ORDER, new SagaEnvelope<>(ORDER, "ignored", SagaStatus.ACTIVE, 2, List.of(),
                Map.of(), null, NOW, NOW, null, null, false, null, Map.of(), early), 1);

        // Then
        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(summaries(store.find(ORDER).orElseThrow().early())).isEqualTo(summaries(early))
        );
    }

    @Test
    void saving_without_the_state_removes_the_kept_events_the_envelope_no_longer_carries() {
        // Given
        store.compareAndSave(ORDER, unstartedKeeping(List.of(cloudEvent("a", "x"))), 0);

        // When
        boolean saved = store.compareAndSaveWithoutState(ORDER, new SagaEnvelope<>(ORDER, "ignored", SagaStatus.ACTIVE, 2, List.of(),
                Map.of(), null, NOW, NOW, null, null, false, null, Map.of(), List.of()), 1);

        // Then
        assertAll(
                () -> assertThat(saved).isTrue(),
                () -> assertThat(storedDocument().containsKey("early")).isFalse()
        );
    }

    sealed interface OrderEvent permits OrderPlaced, PaymentReceived {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReceived(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder, CapturePayment {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CapturePayment(String orderId) implements OrderCommand {
    }

    @Test
    void a_runner_on_this_store_applies_an_event_that_arrived_before_the_start() {
        // Given
        InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
        InMemoryEventStore eventStore = new InMemoryEventStore(subscriptionModel);
        CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .evolve(PaymentReceived.class, (state, e) -> "paid")
                .react(PaymentReceived.class, (state, e) -> List.of(SagaEffect.issue(new CapturePayment(e.orderId()))))
                .build();
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run("orders", saga, store, dispatched::add, null, SagaRunnerConfig.defaults());
        try {
            eventStore.write("payments-1", converter.toCloudEvents(List.of(new PaymentReceived(ORDER))));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(store.find(ORDER)).isPresent());

            // When
            eventStore.write("orders-1", converter.toCloudEvents(List.of(new OrderPlaced(ORDER))));

            // Then
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                SagaEnvelope<String> read = store.find(ORDER).orElseThrow();
                assertAll(
                        () -> assertThat(dispatched).containsExactly(new ShipOrder(ORDER), new CapturePayment(ORDER)),
                        () -> assertThat(read.state()).isEqualTo("paid"),
                        () -> assertThat(read.started()).isTrue(),
                        () -> assertThat(read.early()).isEmpty()
                );
            });
        } finally {
            subscription.close();
            subscriptionModel.shutdown();
        }
    }
}
