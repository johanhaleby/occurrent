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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import org.bson.Document;
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
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A live input asks the store whether it can write the instance with the input applied before it
 * dispatches a command, and MongoDB stores no document over 16 MiB. The instances here hold a state that takes the
 * stored document to a given room under 16,777,216 bytes, checked against the size MongoDB reports for it, so the real
 * store's {@link SpringMongoSagaStateStore#fits} answers for real. The state is seeded through the state store, and
 * applying an input grows it by {@link #STATE_GROWTH} bytes.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(300)
class SpringMongoSagaStateStoreLiveWriteTooLargeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-live-write-too-large";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String FAILING_INPUT = STREAM + "@2";
    private static final int MAX_DOCUMENT_SIZE = 16 * 1024 * 1024;
    private static final int STATE_GROWTH = 4_000;
    // Room left under the limit, each checked by the tests against the size MongoDB reports. Applying an input needs
    // STATE_GROWTH. The failure record takes about 600 bytes with its message and listed as pending, and about 300
    // without both
    private static final int ROOM_FOR_A_FAILURE_RECORD_ONLY = 2_000;
    private static final int ROOM_FOR_A_BARE_FAILURE_RECORD_ONLY = 700;
    private static final int ROOM_FOR_NOTHING = 20;

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
    private MongoTemplate mongoTemplate;
    private SpringMongoSagaStateStore<String> stateStore;
    private SagaEnvelopes<String> envelopes;
    private final List<Pack> dispatched = new CopyOnWriteArrayList<>();
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    private final AtomicInteger failedReactions = new AtomicInteger();
    private SagaSubscription subscription;
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-live-large-" + UUID.randomUUID().toString().substring(0, 8)));
        mongoTemplate = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoTemplate, COLLECTION, String.class);
        envelopes = SagaEnvelopes.of(stateStore, null);
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger("org.occurrent.dsl.saga.blocking.SagaExecution");
        executionLog.addAppender(appender);
    }

    @AfterEach
    void shutdown() {
        executionLog.detachAppender(appender);
        appender.stop();
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    // The ERRORs other than the one that announces a quarantine, which every quarantine logs
    private List<String> errors() {
        // The subscription thread appends while this streams, so it reads a copy taken under the appender's own lock
        List<ILoggingEvent> logged;
        synchronized (appender) {
            logged = List.copyOf(appender.list);
        }
        return logged.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage)
                .filter(message -> !message.contains("is now QUARANTINED")).toList();
    }

    private Saga<OrderEvent, String, Pack> saga() {
        return Saga.<OrderEvent, String, Pack>builder("new")
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .evolve(ItemAdded.class, (state, e) -> state + "y".repeat(STATE_GROWTH))
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        failedReactions.incrementAndGet();
                        throw new IllegalStateException("cannot pack " + e.item() + " " + "m".repeat(5_000));
                    }
                    return List.of(SagaEffect.issue(new Pack(e.item(), e.quantity())));
                })
                .build();
    }

    private void write(OrderEvent event) {
        eventStore.write(STREAM, converter.toCloudEvents(List.of(event)));
    }

    private SagaEnvelope<String> instance() {
        return envelopes.find(ORDER).orElseThrow();
    }

    private int storedSize() {
        return requireNonNull(mongoTemplate.getCollection(COLLECTION)
                .aggregate(List.of(Aggregates.match(new Document("_id", ORDER)), Aggregates.project(Projections.computed("size", new Document("$bsonSize", "$$ROOT")))))
                .first()).getInteger("size");
    }

    private void saveState(String state) {
        SagaEnvelope<String> current = instance();
        SagaEnvelope<String> seeded = new SagaEnvelope<>(current.sagaId(), state, current.status(), current.version() + 1, current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
        assertThat(envelopes.compareAndSave(ORDER, seeded, current.version())).isTrue();
    }

    // The subscription offers a failed event again only after delay, instead of the model's default 200 ms
    private void redeliveringAfter(Duration delay) {
        subscriptionModel.shutdown();
        subscriptionModel = new InMemorySubscriptionModel(RetryStrategy.fixed(delay));
        eventStore = new InMemoryEventStore(subscriptionModel);
    }

    // The runner has applied the start event, and the instance then holds a state that takes it to room bytes under the limit
    private void startedWithRoomLeft(SagaRunnerConfig config, int room) {
        started(config);
        fillTo(room);
    }

    private void started(SagaRunnerConfig config) {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, config.withTimerPollInterval(Duration.ofHours(1)));
        write(new OrderPlaced(ORDER));
        await().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE));
    }

    // The instance then holds a state that takes it to room bytes under the limit
    private void fillTo(int room) {
        saveState("p".repeat(MAX_DOCUMENT_SIZE - 200_000));
        int size = storedSize();
        saveState("p".repeat(MAX_DOCUMENT_SIZE - 200_000 + (MAX_DOCUMENT_SIZE - room - size)));
        assertThat(storedSize()).as("control: the instance is stored with %s bytes of room", room).isEqualTo(MAX_DOCUMENT_SIZE - room);
        appender.list.clear();
    }

    @Test
    void an_input_whose_instance_the_store_cannot_write_dispatches_nothing_records_the_failure_on_the_event_and_quarantines_the_instance_on_it() {
        startedWithRoomLeft(SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMillis(300)), ROOM_FOR_A_FAILURE_RECORD_ONLY);
        int stateLength = instance().state().length();

        write(new ItemAdded(ORDER, "book", 2));

        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        SagaEnvelope<String> quarantined = instance();
        assertAll(
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().failureMessage()).contains("cannot apply the event").contains("No attempt of this delivery dispatched any of its commands."),
                () -> assertThat(quarantined.state()).as("the event is not applied").hasSize(stateLength),
                () -> assertThat(storedSize()).isLessThanOrEqualTo(MAX_DOCUMENT_SIZE),
                () -> assertThat(errors()).as("every failure write fit").isEmpty()
        );
    }

    @Test
    void with_quarantine_off_an_input_whose_instance_the_store_cannot_write_dispatches_nothing_and_is_listed_as_pending() {
        startedWithRoomLeft(SagaRunnerConfig.defaults().disableQuarantine(), ROOM_FOR_A_FAILURE_RECORD_ONLY);
        int stateLength = instance().state().length();

        write(new ItemAdded(ORDER, "book", 2));

        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(instance().streamMarks().get(STREAM).pending().values()).anyMatch(versions -> !versions.isEmpty()));
        SagaEnvelope<String> after = instance();
        assertAll(
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.state()).as("the event is not applied").hasSize(stateLength),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(errors()).isEmpty()
        );
    }

    @Test
    void a_failure_record_the_store_cannot_write_in_full_is_written_without_its_message_and_the_budget_still_runs_to_a_quarantine_with_a_bare_record() {
        // A redelivery inside the budget lists the event as pending, since that write alone fits, so the next delivery comes
        // after the budget and the quarantine is the only write after the first bare record
        redeliveringAfter(Duration.ofSeconds(1));
        startedWithRoomLeft(SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMillis(300)), ROOM_FOR_A_BARE_FAILURE_RECORD_ONLY);
        failing.add("book");

        write(new ItemAdded(ORDER, "book", 2));

        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        SagaEnvelope<String> quarantined = instance();
        assertAll(
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().failureType()).isEqualTo(IllegalStateException.class.getName()),
                () -> assertThat(quarantined.failure().failureMessage()).as("no message").isNull(),
                () -> assertThat(quarantined.streamMarks().get(STREAM).pending().values()).as("the input is not listed as pending").allMatch(List::isEmpty),
                () -> assertThat(storedSize()).isLessThanOrEqualTo(MAX_DOCUMENT_SIZE),
                () -> assertThat(errors()).as("a bare record fit").isEmpty()
        );
    }

    @Test
    void a_bare_quarantine_write_for_an_input_at_or_below_the_floor_that_the_marks_do_not_list_keeps_it_pending_so_the_release_applies_it_instead_of_dropping_it() {
        // An instance stored before 0.35.0 applied the stream up to version 30 and has no marks, and its failure record names version 2
        // with the budget used up. Version 2 is at or below the floor, so only a pending entry makes a release apply it, since
        // the release ignores the failure record. The full record with its message does not fit and the bare one does, and
        // without the entry the release would delete the record and return RELEASED with the reaction of the event lost
        started(SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMillis(300)));
        SagaEnvelope<String> current = instance();
        SagaFailure budgetUsedUp = new SagaFailure(FAILING_INPUT, null, Instant.parse("2026-01-01T00:00:00Z"), IllegalStateException.class.getName(), null, null, null);
        assertThat(envelopes.compareAndSave(ORDER, new SagaEnvelope<>(ORDER, current.state(), SagaStatus.ACTIVE, current.version() + 1, List.of(),
                Map.of(STREAM, 30L), null, current.createdAt(), current.updatedAt(), null, current.currentStep(), true, budgetUsedUp, Map.of(), null),
                current.version())).isTrue();
        fillTo(ROOM_FOR_A_BARE_FAILURE_RECORD_ONLY);
        failing.add("book");

        write(new ItemAdded(ORDER, "book", 2));

        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        SagaEnvelope<String> quarantined = instance();
        failing.clear();
        saveState("placed");
        SagaSubscription.ReleaseResult released = subscription.release(ORDER);
        assertAll(
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().failureMessage()).as("the bare record has no message").isNull(),
                () -> assertThat(quarantined.streamMarks().values().stream().flatMap(marks -> marks.pending().values().stream()).flatMap(List::stream).toList())
                        .as("the marks list the input as pending").containsExactly(2L),
                () -> assertThat(errors()).isEmpty(),
                () -> assertThat(released).isEqualTo(SagaSubscription.ReleaseResult.RELEASED),
                () -> assertThat(dispatched).as("the reaction of the input ran").containsExactly(new Pack("book", 2)),
                () -> assertThat(instance().failure()).as("the record is deleted").isNull(),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_failure_record_the_store_cannot_write_even_without_its_message_is_not_written_logs_an_error_and_the_instance_is_never_quarantined() {
        startedWithRoomLeft(SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMillis(300)), ROOM_FOR_NOTHING);
        failing.add("book");
        SagaEnvelope<String> before = instance();

        write(new ItemAdded(ORDER, "book", 2));

        // Long enough for the budget to run out between the first and the last delivery counted
        await().pollDelay(Duration.ofMillis(600)).ignoreExceptions().atMost(Duration.ofSeconds(60)).until(() -> errors().size() >= 3);
        assertAll(
                () -> assertThat(errors()).allSatisfy(error -> assertThat(error).contains("'" + ORDER + "'").contains("could not record the failure")),
                () -> assertThat(instance()).as("nothing is written, the version included").isEqualTo(before),
                () -> assertThat(instance().status()).as("never quarantined").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(dispatched).isEmpty()
        );
    }

    @Test
    void the_error_that_a_failure_record_cannot_be_written_even_without_its_message_is_logged_once_across_the_redeliveries_of_the_event() {
        startedWithRoomLeft(SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)), ROOM_FOR_NOTHING);
        failing.add("book");

        write(new ItemAdded(ORDER, "book", 2));

        // Redelivered every 200 ms, so six failed reactions take a second, inside the interval of the ERROR
        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).until(() -> failedReactions.get() >= 6);
        assertAll(
                () -> assertThat(errors()).as("the first delivery said it and the five after it did not").singleElement().asString()
                        .contains("'" + ORDER + "'").contains("could not record the failure").contains("This is logged at most once per PT5M for the instance."),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(dispatched).isEmpty()
        );
    }
}
