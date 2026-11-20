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
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A quarantined instance holds the events of the inputs it skips in the records collection, and a
 * release applies those held events, with no read of the event store. Checks against a MongoDB replica set that a held
 * event, with its data, its time and the extensions the event store gave it, reads back with the id, type, source,
 * data, time and extension names of the event the subscription delivered, and that the release applies what was read
 * back.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreHeldEventTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-held-event";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final OffsetDateTime HAPPENED_AT = OffsetDateTime.parse("2026-01-01T10:15:30.123Z");

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item, int quantity) implements OrderEvent {
    }

    record Pack(String item, int quantity) {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private SpringMongoSagaStateStore<String> stateStore;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test"))
                .timeMapper(event -> HAPPENED_AT).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-held-" + UUID.randomUUID()));
        stateStore = new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase())), COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private Saga<OrderEvent, String, Pack> saga() {
        return Saga.<OrderEvent, String, Pack>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.item(), e.quantity())));
                })
                .build();
    }

    private void write(OrderEvent event) {
        eventStore.write(STREAM, converter.toCloudEvents(List.of(event)));
    }

    private SagaEnvelope<String> instance() {
        return SagaEnvelopes.of(stateStore, null).find(ORDER).orElseThrow();
    }

    private List<CloudEvent> stored() {
        return eventStore.read(STREAM).eventList();
    }

    @Test
    void a_held_event_with_data_a_time_and_extensions_reads_back_equal_and_is_applied_by_a_release() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        List<CloudEvent> delivered = stored();
        CloudEvent book = delivered.get(1);
        CloudEvent pen = delivered.get(2);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(held()).hasSize(2));
        List<SkippedInput> held = held();

        assertAll(
                () -> assertThat(held).extracting(input -> input.event().getId()).containsExactly(book.getId(), pen.getId()),
                () -> assertThat(held).extracting(input -> input.event().getType()).containsExactly(book.getType(), pen.getType()),
                () -> assertThat(held).extracting(input -> input.event().getSource()).containsExactly(book.getSource(), pen.getSource()),
                () -> assertThat(held).extracting(input -> input.event().getExtensionNames()).containsExactly(book.getExtensionNames(), pen.getExtensionNames()),
                () -> assertThat(held).extracting(input -> requireNonNull(input.event().getData()).toBytes()).containsExactly(requireNonNull(book.getData()).toBytes(), requireNonNull(pen.getData()).toBytes()),
                () -> assertThat(held.get(1).event().getTime()).isEqualTo(HAPPENED_AT),
                () -> assertThat(held.get(1).event().getExtension(OccurrentCloudEventExtension.STREAM_ID)).isEqualTo(STREAM),
                () -> assertThat(held.get(1).event().getExtension(OccurrentCloudEventExtension.STREAM_VERSION)).hasToString(String.valueOf(pen.getExtension(OccurrentCloudEventExtension.STREAM_VERSION))),
                () -> assertThat(new String(requireNonNull(held.get(1).event().getData()).toBytes())).contains("\"pen\"")
        );

        failing.clear();
        subscription.release(ORDER);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
        assertThat(dispatched).as("the release applied the events as they were read back").containsExactly(new Pack("book", 2), new Pack("pen", 5));
    }

    private List<SkippedInput> held() {
        return stateStore.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE);
    }
}
