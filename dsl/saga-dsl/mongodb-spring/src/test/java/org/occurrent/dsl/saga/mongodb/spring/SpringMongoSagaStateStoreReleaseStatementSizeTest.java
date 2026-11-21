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
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A release write whose document fits 16 MiB but whose statement goes over what the driver sends in one
 * update, 16 MiB plus 16 KiB, because the compare-and-set filter holds a long saga id. The release has to see that
 * before it dispatches the commands of the record.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(300)
class SpringMongoSagaStateStoreReleaseStatementSizeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-release-statement-size";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-" + "x".repeat(32 * 1024 - "order-".length());
    private static final int MAX_DOCUMENT_SIZE = 16 * 1024 * 1024;
    private static final int MAX_STATEMENT_SIZE = MAX_DOCUMENT_SIZE + 16 * 1024;
    private static final int STATE_GROWTH = 4_000;
    // Room for the mark the applied record adds
    private static final int MARK_ALLOWANCE = 500;
    // Stored size of the seeded instance. Its own write statement, the document and a filter of 32 KiB, fits the driver's
    // limit, and the instance with the record applied does not
    private static final int SEEDED_SIZE = 16_758_700;

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
    private SagaSubscription subscription;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-stmt-" + UUID.randomUUID()));
        mongoTemplate = new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase()));
        stateStore = new SpringMongoSagaStateStore<>(mongoTemplate, COLLECTION, String.class);
        envelopes = SagaEnvelopes.of(stateStore, null);
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
                .evolve(ItemAdded.class, (state, e) -> "packing" + "x".repeat(STATE_GROWTH))
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
        return envelopes.find(ORDER).orElseThrow();
    }

    // Quarantined on "book", with "pen" recorded after it
    private void quarantinedOnBookWithPenRecorded() {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(new ItemAdded(ORDER, "pen", 5));
        await().ignoreExceptions().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(stateStore.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE)).hasSize(2));
        failing.clear();
        dispatched.clear();
    }

    // One stream whose id is keyLength characters long, which sizes the stored document to the byte
    private SagaEnvelope<String> withPadding(SagaEnvelope<String> base, int keyLength) {
        Map<String, StreamMarks> marks = new HashMap<>(base.streamMarks());
        marks.keySet().removeIf(key -> key.startsWith("pad-"));
        marks.put("pad-" + "x".repeat(keyLength), new StreamMarks(base.streamMarks().get(STREAM).floor(), Map.of(), Map.of()));
        return new SagaEnvelope<>(base.sagaId(), base.state(), base.status(), base.version(), base.timers(),
                base.streamWatermarks(), base.positionWatermark(), base.createdAt(), base.updatedAt(), base.completedAt(),
                base.currentStep(), base.started(), base.failure(), marks, base.quarantineSkipCount());
    }

    private boolean save(SagaEnvelope<String> seeded) {
        return envelopes.compareAndSave(ORDER, seeded, instance().version());
    }

    // The size MongoDB reports for the stored instance, which no code of the store takes part in
    private int storedSize() {
        return requireNonNull(mongoTemplate.getCollection(COLLECTION)
                .aggregate(List.of(Aggregates.match(new Document("_id", ORDER)), Aggregates.project(Projections.computed("size", new Document("$bsonSize", "$$ROOT")))))
                .first()).getInteger("size");
    }

    @Test
    void a_release_whose_applied_document_fits_but_whose_replace_statement_with_a_32_KiB_saga_id_does_not_stops_before_dispatching_the_record() {
        quarantinedOnBookWithPenRecorded();
        boolean firstSeedWritten = save(withPadding(instance(), 1));
        int sizeWithOnePadByte = storedSize();
        boolean seedWritten = save(withPadding(instance(), 1 + SEEDED_SIZE - sizeWithOnePadByte));
        int sizeStored = storedSize();

        ReleaseResult[] result = new ReleaseResult[1];
        Throwable thrown = catchThrowable(() -> result[0] = subscription.release(ORDER));

        long appliedDocumentAtMost = (long) sizeStored + STATE_GROWTH + MARK_ALLOWANCE;
        long replaceStatementAtLeast = (long) sizeStored + STATE_GROWTH + ORDER.length();
        assertAll(
                () -> assertThat(ORDER).as("control: the saga id is 32 KiB").hasSize(32 * 1024),
                () -> assertThat(firstSeedWritten && seedWritten).as("control: the seed writes took, so the instance was writable at its stored size").isTrue(),
                () -> assertThat(sizeStored).as("control: stored with the size the window names").isEqualTo(SEEDED_SIZE),
                () -> assertThat(appliedDocumentAtMost).as("window: the applied document is at most 16,777,216 bytes, so a document-only check says yes").isLessThanOrEqualTo(MAX_DOCUMENT_SIZE),
                () -> assertThat(replaceStatementAtLeast).as("window: the applied replace statement, the document and the 32 KiB filter, is over 16,793,600 bytes").isGreaterThan(MAX_STATEMENT_SIZE),
                () -> assertThat(thrown).as("no exception escapes the release").isNull(),
                () -> assertThat(result[0]).as("the release stops on the record").isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatched).as("nothing is dispatched for the record before the write the store cannot take").isEmpty()
        );
    }
}
