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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a standalone MongoDB, which has no replica set, no failover and refuses transactions and read concern
 * {@code linearizable}. With quarantine off a saga runs on it, and with quarantine on the runner refuses to start it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaStateStoreStandaloneRunnerTest {

    @Container
    private static final GenericContainer<?> standaloneMongo = new GenericContainer<>("mongo:8.0")
            .withExposedPorts(27017)
            // As in test-support. Without it mongod 8.0 refuses to start on the CI runners' kernel (SERVER-121912)
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
            .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1))
            .withStartupTimeout(Duration.ofMinutes(5));

    private static final String COLLECTION = "saga-standalone-runner";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded, OrderShipped {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item) implements OrderEvent {
    }

    record OrderShipped(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits Pack, Remind {
    }

    record Pack(String item) implements OrderCommand {
    }

    record Remind(String orderId) implements OrderCommand {
    }

    private final InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
    private final InMemoryEventStore eventStore = new InMemoryEventStore(subscriptionModel);
    private final AtomicInteger itemAddedDeliveries = new AtomicInteger();
    private final CloudEventConverter<OrderEvent> converter = new DeliveryCountingConverter(new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build());
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger handled = new AtomicInteger();
    private MongoTemplate mongoTemplate;
    private SpringMongoSagaStateStore<String> stateStore;
    private @Nullable SagaSubscription subscription;
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;

    @BeforeEach
    void createTheStore() {
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger("org.occurrent.dsl.saga.blocking.SagaExecution");
        executionLog.addAppender(appender);
        ConnectionString connectionString = new ConnectionString("mongodb://" + standaloneMongo.getHost() + ":" + standaloneMongo.getMappedPort(27017) + "/saga-standalone-" + UUID.randomUUID());
        mongoTemplate = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoTemplate, COLLECTION, String.class);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
        executionLog.detachAppender(appender);
        appender.stop();
    }

    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private Saga<OrderEvent, String, OrderCommand> saga() {
        return Saga.<OrderEvent, String, OrderCommand>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .isTerminal("shipped"::equals)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .react(OrderPlaced.class, (state, e) -> {
                    handled.incrementAndGet();
                    return List.of(SagaEffect.startTimeout("reminder", Duration.ofMillis(100)));
                })
                .evolve(ItemAdded.class, (state, e) -> "packing")
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Pack(e.item()))))
                .evolve(OrderShipped.class, (state, e) -> "shipped")
                .reactOnTimeout("reminder", (state, timeout) -> List.of(SagaEffect.issue(new Remind(timeout.sagaId()))))
                .build();
    }

    private SagaSubscription runWithQuarantineOff() {
        subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run(STREAM, saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().disableQuarantine().withTimerPollInterval(Duration.ofMillis(50)));
        return subscription;
    }

    private SagaSubscription runWithQuarantineOn() {
        subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run(STREAM, saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50)));
        return subscription;
    }

    private void write(OrderEvent event) {
        write(ORDER, event);
    }

    private void write(String streamId, OrderEvent event) {
        eventStore.write(streamId, converter.toCloudEvents(List.of(event)));
    }

    private SagaEnvelopes<String> envelopes() {
        return SagaEnvelopes.of(stateStore, null);
    }

    @Test
    void a_saga_with_quarantine_off_starts_updates_fires_its_timer_and_completes_on_a_standalone_server() {
        runWithQuarantineOff().waitUntilStarted();

        write(new OrderPlaced(ORDER));
        write(new ItemAdded(ORDER, "book"));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(dispatched)
                .as("the input that started the instance armed a timer that fired, and the input that updated it issued its command")
                .containsExactlyInAnyOrder(new Pack("book"), new Remind(ORDER)));
        write(new OrderShipped(ORDER));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(envelopes().find(ORDER).map(SagaEnvelope::status))
                .as("the input that ends the saga completes the instance")
                .contains(SagaStatus.COMPLETED));
        SagaEnvelope<String> stored = envelopes().find(ORDER).orElseThrow();
        assertAll(
                () -> assertThat(stored.state()).as("the instance holds the state every input evolved it to").isEqualTo("shipped"),
                () -> assertThat(stored.timers()).as("the timer that fired is gone").isEmpty(),
                () -> assertThat(dispatched).as("nothing is issued twice").hasSize(2)
        );
    }

    @Test
    void the_store_reads_and_deletes_instances_on_a_standalone_server() {
        runWithQuarantineOff().waitUntilStarted();
        assertThat(envelopes().find(ORDER)).as("an absent instance is read as absent").isEmpty();
        assertThat(envelopes().findWithoutState(ORDER)).as("also without its state").isEmpty();
        write(new OrderPlaced(ORDER));
        write(new OrderShipped(ORDER));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(envelopes().findWithoutState(ORDER).map(SagaEnvelope::status)).contains(SagaStatus.COMPLETED));

        assertThat(envelopes().findByStatus(SagaStatus.COMPLETED, Instant.now().plusSeconds(60), 10).stream().map(SagaEnvelope::sagaId))
                .as("the completed instance is found by its status")
                .containsExactly(ORDER);
        stateStore.delete(ORDER);

        assertThat(envelopes().find(ORDER)).as("the instance is gone once deleted").isEmpty();
    }

    @Test
    void a_saga_with_quarantine_on_refuses_to_run_on_a_standalone_server_before_it_handles_an_input() {
        assertThatThrownBy(this::runWithQuarantineOn)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("occurrent.saga.quarantine-enabled=false")
                .hasMessageContaining("SagaRunnerConfig.disableQuarantine()");

        write(new OrderPlaced(ORDER));

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertAll(
                () -> assertThat(handled).as("no input is handled").hasValue(0),
                () -> assertThat(dispatched).as("no command is issued").isEmpty(),
                () -> assertThat(envelopes().find(ORDER)).as("no instance is stored").isEmpty()
        ));
    }

    @Test
    void a_quarantined_instance_restored_from_a_replica_set_refuses_its_inputs_and_holds_the_subscription_when_quarantine_is_off() {
        Instant now = Instant.parse("2026-03-04T05:06:07Z");
        SagaFailure failure = new SagaFailure(ORDER + "@1", 1L, now.minusSeconds(300), IllegalStateException.class.getName(), "boom", null, "quarantine-restored");
        // What 0.35.0 wrote on a replica set, which the standalone server now holds as restored data
        assertThat(envelopes().compareAndSave(ORDER, new SagaEnvelope<>(ORDER, "placed", SagaStatus.QUARANTINED, 3L, List.of(), Map.of(), null,
                now.minusSeconds(600), now, null, null, true, failure, Map.of(), 1L), 0)).as("control: the instance is written").isTrue();
        Document restored = requireNonNull(mongoTemplate.findById(ORDER, Document.class, COLLECTION));
        assertThat(restored.getString("status")).as("control: stored as a quarantine that records skipped inputs").isEqualTo("QUARANTINED_RECORDING");
        runWithQuarantineOff().waitUntilStarted();

        write(ORDER, new ItemAdded(ORDER, "book"));
        write("order-2", new OrderPlaced("order-2"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(itemAddedDeliveries)
                .as("the input is delivered again, since the refused delivery did not acknowledge it")
                .hasValueGreaterThanOrEqualTo(2));
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(envelopes().find("order-2"))
                .as("the input after the refused one is not processed, so the subscription did not move past it")
                .isEmpty());
        SagaEnvelope<String> after = envelopes().find(ORDER).orElseThrow();
        assertAll(
                () -> assertThat(after.version()).as("the quarantined instance's version").isEqualTo(3L),
                () -> assertThat(after.quarantineSkipCount()).as("the quarantined instance's skip count").isEqualTo(1L),
                () -> assertThat(after.status()).as("still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.state()).as("its state is untouched").isEqualTo("placed"),
                () -> assertThat(mongoTemplate.findById(ORDER, Document.class, COLLECTION)).as("the stored document is untouched").isEqualTo(restored),
                () -> assertThat(mongoTemplate.getCollection(COLLECTION + "_skipped_inputs").countDocuments()).as("no record of the refused input").isZero(),
                () -> assertThat(dispatched).as("the refused input issued no Pack").isEmpty(),
                () -> assertThat(handled).as("no OrderPlaced is handled, including the one for order-2").hasValue(0)
        );
    }

    // Data restored from a replica set, an instance quarantined on the ItemAdded it holds as its first record. A release
    // marks it by setting releasedAt, as one that was running when the data was copied. The record is written straight into
    // its collection, since the standalone server refuses the transaction that records one. The events are written before
    // the runner subscribes, so it does not deliver them
    private void restoredQuarantinedOnItemAdded(@Nullable Instant releasedAt) {
        write(new OrderPlaced(ORDER));
        write(new ItemAdded(ORDER, "book"));
        CloudEvent held = eventStore.read(ORDER).eventList().getLast();
        String key = ORDER + "@" + held.getExtension("streamversion");
        Instant now = Instant.parse("2026-03-04T05:06:07Z");
        SagaFailure failure = new SagaFailure(key, null, now.minusSeconds(300), IllegalStateException.class.getName(), "boom", releasedAt, "quarantine-restored");
        assertThat(envelopes().compareAndSave(ORDER, new SagaEnvelope<>(ORDER, "placed", SagaStatus.QUARANTINED, 3L, List.of(), Map.of(), null,
                now.minusSeconds(600), now, null, null, true, failure, Map.of(), 1L), 0)).as("control: the instance is written").isTrue();
        EventFormat json = requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE));
        mongoTemplate.getCollection(COLLECTION + "_skipped_inputs").insertOne(new Document("sagaId", ORDER).append("quarantineId", "quarantine-restored").append("seq", 1L).append("key", key)
                .append("event", new String(json.serialize(held), StandardCharsets.UTF_8)));
        Document stored = requireNonNull(mongoTemplate.findById(ORDER, Document.class, COLLECTION));
        assertThat(stored.get(SagaCodec.FAILURE_RELEASED_AT) != null).as("control: marked releasing as asked").isEqualTo(releasedAt != null);
        assertThat(heldRecords()).as("control: the first record is held").hasSize(1);
    }

    private List<Document> heldRecords() {
        return mongoTemplate.getCollection(COLLECTION + "_skipped_inputs").find().into(new ArrayList<>());
    }

    @Test
    void a_release_of_a_quarantined_instance_on_a_standalone_server_throws_dispatches_nothing_and_leaves_the_instance_and_its_record_as_they_were() {
        restoredQuarantinedOnItemAdded(null);
        Document instanceBefore = requireNonNull(mongoTemplate.findById(ORDER, Document.class, COLLECTION));
        List<Document> recordsBefore = heldRecords();
        SagaSubscription running = runWithQuarantineOff();
        running.waitUntilStarted();

        Throwable thrown = catchThrowable(() -> running.release(ORDER));

        Document instanceAfter = requireNonNull(mongoTemplate.findById(ORDER, Document.class, COLLECTION));
        assertAll(
                () -> assertThat(thrown).as("the release throws, naming the instance and the atomic write the server cannot take")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("cannot release instance '" + ORDER + "'")
                        .hasMessageContaining("one atomic write"),
                () -> assertThat(dispatched).as("no command is dispatched").isEmpty(),
                () -> assertThat(instanceAfter).as("the stored instance document is unchanged").isEqualTo(instanceBefore),
                () -> assertThat(instanceAfter.get(SagaCodec.FAILURE_RELEASED_AT)).as("the instance is not marked releasing").isNull(),
                () -> assertThat(heldRecords()).as("the skipped-input collection is unchanged").isEqualTo(recordsBefore)
        );
    }

    @Test
    void the_timer_poller_dispatches_and_writes_nothing_for_an_instance_marked_releasing_on_a_standalone_server_and_logs_one_error() {
        restoredQuarantinedOnItemAdded(Instant.parse("2026-03-04T05:20:00Z"));
        Document instanceBefore = requireNonNull(mongoTemplate.findById(ORDER, Document.class, COLLECTION));
        List<Document> recordsBefore = heldRecords();

        runWithQuarantineOff().waitUntilStarted();

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertAll(
                () -> assertThat(dispatched).as("no command is dispatched by any poll").isEmpty(),
                () -> assertThat(mongoTemplate.findById(ORDER, Document.class, COLLECTION)).as("the stored instance document is unchanged").isEqualTo(instanceBefore),
                () -> assertThat(heldRecords()).as("the skipped-input collection is unchanged").isEqualTo(recordsBefore)
        ));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(errors()).as("one ERROR is logged")
                .singleElement().asString().contains("does not go on with the release of instances").contains(ORDER));
    }

    // Counts a conversion of an ItemAdded to a domain event, which each delivery attempt of the input does before it is handled
    private final class DeliveryCountingConverter implements CloudEventConverter<OrderEvent> {
        private final CloudEventConverter<OrderEvent> delegate;

        private DeliveryCountingConverter(CloudEventConverter<OrderEvent> delegate) {
            this.delegate = delegate;
        }

        @Override
        public CloudEvent toCloudEvent(OrderEvent domainEvent) {
            return delegate.toCloudEvent(domainEvent);
        }

        @Override
        public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
            OrderEvent event = delegate.toDomainEvent(cloudEvent);
            if (event instanceof ItemAdded) {
                itemAddedDeliveries.incrementAndGet();
            }
            return event;
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return delegate.getCloudEventType(type);
        }
    }
}
