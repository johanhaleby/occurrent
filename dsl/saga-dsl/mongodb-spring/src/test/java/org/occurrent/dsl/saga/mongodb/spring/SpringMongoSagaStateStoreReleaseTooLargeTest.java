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
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntFunction;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A release keeps every version it applies in the stream marks of the instance until it ends, so a long
 * backlog grows the document, and MongoDB refuses one over 16,777,216 bytes. The store answers whether it can write a
 * candidate instance, see {@link SpringMongoSagaStateStore#fits}, and a release asks before it dispatches a record's
 * commands. The instances are large because the marks are seeded through the state store, since processing the events one by one
 * would take too long. Each seeded instance is saved by the store, so none is over what MongoDB stores.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(300)
class SpringMongoSagaStateStoreReleaseTooLargeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-release-too-large";
    private static final String STREAM = "orders";
    private static final String ORDER = "order-1";
    private static final String SEEDED_TYPE = "seeded";
    private static final long MAX_DOCUMENT_SIZE = 16L * 1024 * 1024;
    // Found by searching with the store's fits(..) until it refused. Each is checked against the size MongoDB reports
    // for the stored instance, so a change to the document layout fails a control and not a guess
    private static final int VERSIONS_PAST_WHAT_FITS = 1_051_234 + 200;
    private static final int STREAMS_PAST_WHAT_FITS = 76_581 + 20;
    private static final int PADDING_PAST_WHAT_FITS = 16_759_938;
    private static final long FAILURE_RECORD_ROOM = 16L * 1024;
    private static final String RELEASE_AGAIN = "so release the instance again to go on from that event";
    private static final String STOPS_AGAIN = "even with the stream versions the release applied trimmed. A further release stops on this event too.";

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
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-too-large-" + UUID.randomUUID()));
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

    private List<String> errorsSinceTheRelease() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
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
        return envelopes.find(ORDER).orElseThrow();
    }

    // Quarantined on "book", with "pen" recorded after it, so a release has two records to replay
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
        appender.list.clear();
    }

    private SagaEnvelope<String> withMarks(Map<String, StreamMarks> marks) {
        SagaEnvelope<String> current = instance();
        return new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), marks, current.quarantineSkipCount());
    }

    private SagaEnvelope<String> withAppliedVersionsInTheStream(int versions) {
        SagaEnvelope<String> current = instance();
        List<Long> applied = new ArrayList<>();
        for (long version = 0; version < versions; version++) {
            applied.add(1_000_000L + version);
        }
        StreamMarks existing = current.streamMarks().get(STREAM);
        Map<String, TypeMark> byType = new HashMap<>(existing.byType());
        byType.put(SEEDED_TYPE, new TypeMark(1_000_000L + versions, applied, 0));
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        marks.put(STREAM, new StreamMarks(existing.floor(), byType, existing.pending(), existing.legacyFailed(), existing.pendingSince()));
        return withMarks(marks);
    }

    // Streams with a UUID id, each keeping the versions it applied behind the reorder window
    private SagaEnvelope<String> withStreams(int streams, int versionsPerStream) {
        SagaEnvelope<String> current = instance();
        long floor = current.streamMarks().get(STREAM).floor();
        List<Long> applied = new ArrayList<>();
        for (long version = 0; version < versionsPerStream; version++) {
            applied.add(1_000L + version);
        }
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        for (int stream = 0; stream < streams; stream++) {
            marks.put("seeded-" + UUID.randomUUID(), new StreamMarks(floor, Map.of(SEEDED_TYPE, new TypeMark(1_000L + versionsPerStream, applied, 0)), Map.of()));
        }
        return withMarks(marks);
    }

    private void save(SagaEnvelope<String> seeded) {
        assertThat(envelopes.compareAndSave(ORDER, seeded, instance().version())).isTrue();
    }

    // The size MongoDB reports for the stored instance, which no code of the store takes part in
    private int storedSize() {
        return requireNonNull(mongoTemplate.getCollection(COLLECTION)
                .aggregate(List.of(Aggregates.match(new Document("_id", ORDER)), Aggregates.project(Projections.computed("size", new Document("$bsonSize", "$$ROOT")))))
                .first()).getInteger("size");
    }

    private static long appliedVersions(SagaEnvelope<String> envelope) {
        long count = 0;
        for (StreamMarks marks : envelope.streamMarks().values()) {
            for (TypeMark typeMark : marks.byType().values()) {
                count += typeMark.applied().size();
            }
        }
        return count;
    }

    @Test
    void an_instance_that_keeps_510_000_applied_versions_fits_and_a_release_of_it_applies_both_records_and_ends_released() {
        quarantinedOnBookWithPenRecorded();
        SagaEnvelope<String> seeded = withStreams(2_000, 255);
        save(seeded);
        long keptBeforeTheRelease = appliedVersions(instance());

        ReleaseResult result = subscription.release(ORDER);

        assertAll(
                () -> assertThat(keptBeforeTheRelease).as("control: at least 500,000 versions are kept").isGreaterThanOrEqualTo(510_000),
                () -> assertThat(envelopes.fits(stateStore, seeded)).as("control: the store takes the instance").isTrue(),
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).containsExactly(new Pack("book", 2), new Pack("pen", 5)),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(errorsSinceTheRelease()).isEmpty()
        );
    }

    @Test
    void a_release_whose_next_write_would_not_fit_stops_before_dispatching_the_record_and_a_further_release_applies_it_once() {
        quarantinedOnBookWithPenRecorded();
        save(withAppliedVersionsInTheStream(VERSIONS_PAST_WHAT_FITS));
        int sizeStored = storedSize();

        ReleaseResult stopped = subscription.release(ORDER);

        SagaEnvelope<String> afterTheStop = instance();
        List<Pack> dispatchedAtTheStop = List.copyOf(dispatched);
        List<String> errorsAtTheStop = errorsSinceTheRelease();
        int recordsAtTheStop = afterTheStop.quarantineId() == null ? 0 : stateStore.findSkippedInputs(ORDER, afterTheStop.quarantineId(), Integer.MAX_VALUE).size();
        ReleaseResult further = subscription.release(ORDER);

        assertAll(
                () -> assertThat(sizeStored).as("control: stored, and past what fits").isBetween((int) (MAX_DOCUMENT_SIZE - FAILURE_RECORD_ROOM) + 1, (int) MAX_DOCUMENT_SIZE),
                () -> assertThat(stopped).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatchedAtTheStop).as("nothing is dispatched for the record").isEmpty(),
                () -> assertThat(afterTheStop.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(appliedVersions(afterTheStop)).as("the marks are trimmed").isLessThan(1_000),
                () -> assertThat(recordsAtTheStop).as("both records stay recorded").isEqualTo(2),
                () -> assertThat(errorsAtTheStop).singleElement().asString().contains(RELEASE_AGAIN).doesNotContain(STOPS_AGAIN),
                () -> assertThat(further).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).as("each record once, overall").containsExactly(new Pack("book", 2), new Pack("pen", 5)),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void an_instance_the_store_cannot_write_even_with_the_marks_trimmed_stays_quarantined_dispatches_nothing_and_logs_that_a_further_release_stops_too() {
        quarantinedOnBookWithPenRecorded();
        save(withStreams(STREAMS_PAST_WHAT_FITS, 0));
        int sizeStored = storedSize();

        ReleaseResult first = subscription.release(ORDER);
        List<String> errorsAfterTheFirst = errorsSinceTheRelease();
        ReleaseResult second = subscription.release(ORDER);

        SagaEnvelope<String> after = instance();
        assertAll(
                () -> assertThat(sizeStored).as("control: stored, and past what fits").isBetween((int) (MAX_DOCUMENT_SIZE - FAILURE_RECORD_ROOM) + 1, (int) MAX_DOCUMENT_SIZE),
                () -> assertThat(first).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(second).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(errorsAfterTheFirst).singleElement().asString().contains(STOPS_AGAIN).doesNotContain(RELEASE_AGAIN),
                () -> assertThat(errorsSinceTheRelease()).as("the further release logs it again").hasSize(2).allSatisfy(message -> assertThat(message).contains(STOPS_AGAIN)),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(stateStore.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE)).hasSize(2)
        );
    }

    @Test
    void fits_takes_a_document_of_exactly_the_most_MongoDB_stores_less_16_KiB_and_refuses_one_byte_more() {
        quarantinedOnBookWithPenRecorded();
        IntFunction<SagaEnvelope<String>> padded = length -> {
            SagaEnvelope<String> current = instance();
            return new SagaEnvelope<>(current.sagaId(), "x".repeat(length), current.status(), current.version(), current.timers(),
                    current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                    current.currentStep(), current.started(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
        };
        save(padded.apply(PADDING_PAST_WHAT_FITS - 1));

        assertAll(
                () -> assertThat(storedSize()).as("the stored document at the last length that fits, in bytes").isEqualTo((int) (MAX_DOCUMENT_SIZE - FAILURE_RECORD_ROOM)),
                () -> assertThat(envelopes.fits(stateStore, padded.apply(PADDING_PAST_WHAT_FITS - 1))).isTrue(),
                () -> assertThat(envelopes.fits(stateStore, padded.apply(PADDING_PAST_WHAT_FITS))).isFalse()
        );
    }
}
