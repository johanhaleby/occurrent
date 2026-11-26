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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
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
    // Seed sizes, each checked against the size MongoDB reports for the stored instance, so a change to the document
    // layout fails a control and not a guess. A stored size is the largest document less the room the next writes get.
    // A start write adds 27 bytes, applying a record grows the state by STATE_GROWTH bytes, and quarantining the
    // instance again with the failure message adds about 200. The long type test leaves room for the first record
    // to be applied and not for a pending entry of 32 KiB
    private static final int STATE_GROWTH = 4_000;
    private static final int VERSIONS_IN_THE_STREAM = 1_040_000;
    private static final int VERSIONS_ROOM = 2_000;
    private static final int VERSIONS_PAD = 205_249;
    private static final int STREAMS = 70_000;
    private static final int STREAMS_ROOM = 2_000;
    private static final int STREAMS_PAD = 1_455_311;
    private static final int LONG_EVENT_TYPE_LENGTH = 32 * 1024;
    private static final int LONG_TYPE_ROOM = 10_000;
    private static final int LONG_TYPE_PAD = 1_447_311;
    private static final int HARD_LIMIT_PAD = 1_457_311;
    private static final String RELEASE_AGAIN = "so release the instance again to go on from that event";
    private static final String STOPS_AGAIN = "even with the stream versions the release applied trimmed. A further release stops on this event too.";

    sealed interface OrderEvent permits OrderPlaced, ItemAdded, LongTypedItem {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ItemAdded(String orderId, String item, int quantity) implements OrderEvent {
    }

    record LongTypedItem(String orderId, String item, int quantity) implements OrderEvent {
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
                .evolve(ItemAdded.class, (state, e) -> "packing" + "x".repeat(STATE_GROWTH))
                .evolve(LongTypedItem.class, (state, e) -> "packing" + "x".repeat(STATE_GROWTH))
                .react(LongTypedItem.class, (state, e) -> List.of(SagaEffect.issue(new Pack(e.item(), e.quantity()))))
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
        quarantinedOnBookWith(new ItemAdded(ORDER, "pen", 5));
    }

    // Quarantined on "book", with the given event recorded after it
    private void quarantinedOnBookWith(OrderEvent recorded) {
        subscription = SagaRunner.<OrderEvent, Pack>agnostic(subscriptionModel, converter)
                .run("orders", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)).withQuarantineAfter(Duration.ofMillis(300)));
        write(new OrderPlaced(ORDER));
        failing.add("book");
        write(new ItemAdded(ORDER, "book", 2));
        await().dontCatchUncaughtExceptions().ignoreExceptions().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED));
        write(recorded);
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

    // Every LongTypedItem gets the given CloudEvent type, so a recorded event can have a long type
    private static CloudEventConverter<OrderEvent> converterWithLongTypedItems(String longType) {
        CloudEventTypeMapper<OrderEvent> types = new CloudEventTypeMapper<>() {
            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return type == LongTypedItem.class ? longType : type.getName();
            }

            @Override
            @SuppressWarnings("unchecked")
            public <E extends OrderEvent> Class<E> getDomainEventType(String cloudEventType) {
                if (cloudEventType.equals(longType)) {
                    return (Class<E>) LongTypedItem.class;
                }
                return (Class<E>) (cloudEventType.equals(ItemAdded.class.getName()) ? ItemAdded.class : OrderPlaced.class);
            }
        };
        return new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).typeMapper(types).build();
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
        save(withPadding(withAppliedVersionsInTheStream(VERSIONS_IN_THE_STREAM), VERSIONS_PAD));
        int sizeStored = storedSize();

        ReleaseResult stopped = subscription.release(ORDER);

        SagaEnvelope<String> afterTheStop = instance();
        List<Pack> dispatchedAtTheStop = List.copyOf(dispatched);
        List<String> errorsAtTheStop = errorsSinceTheRelease();
        int recordsAtTheStop = afterTheStop.quarantineId() == null ? 0 : stateStore.findSkippedInputs(ORDER, afterTheStop.quarantineId(), Integer.MAX_VALUE).size();
        ReleaseResult further = subscription.release(ORDER);

        assertAll(
                () -> assertThat(sizeStored).as("control: stored with the room the versions test names").isEqualTo((int) MAX_DOCUMENT_SIZE - VERSIONS_ROOM),
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
        save(withPadding(withStreams(STREAMS, 0), STREAMS_PAD));
        int sizeStored = storedSize();

        ReleaseResult first = subscription.release(ORDER);
        List<String> errorsAfterTheFirst = errorsSinceTheRelease();
        ReleaseResult second = subscription.release(ORDER);

        SagaEnvelope<String> after = instance();
        assertAll(
                () -> assertThat(sizeStored).as("control: stored with the room the streams test names").isEqualTo((int) MAX_DOCUMENT_SIZE - STREAMS_ROOM),
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
    void a_recorded_event_with_a_32_KiB_type_makes_the_full_requarantine_too_large_so_the_release_stops_on_the_bare_write_without_dispatching_that_event() {
        String longType = "type-" + "t".repeat(LONG_EVENT_TYPE_LENGTH - 5);
        converter = converterWithLongTypedItems(longType);
        quarantinedOnBookWith(new LongTypedItem(ORDER, "pen", 5));
        save(withPadding(withStreams(STREAMS, 0), LONG_TYPE_PAD));
        SagaEnvelope<String> before = instance();
        int sizeBefore = storedSize();
        SkippedInput recorded = stateStore.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE).get(1);

        ReleaseResult result = subscription.release(ORDER);

        SagaEnvelope<String> after = instance();
        int sizeAfter = storedSize();
        List<SkippedInput> remaining = after.quarantineId() == null ? List.of() : stateStore.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE);
        SagaExecutionSupport.EventMeta meta = new SagaExecutionSupport.EventMeta(String.valueOf(recorded.event().getExtension("streamid")),
                Long.parseLong(String.valueOf(recorded.event().getExtension("streamversion"))), null, recorded.event().getType(), recorded.event().getId(), String.valueOf(recorded.event().getSource()));
        SagaEnvelope<String> releasing = releasingCopyOf(before);
        Instant at = Instant.now();
        Throwable reason = new IllegalStateException("The release of saga instance '" + ORDER + "' stopped on the event '" + recorded.key() + "', because its state store cannot write the instance with the event applied.");
        SagaEnvelope<String> full = requireNonNull(SagaExecutionSupport.onReleaseTooLarge(releasing, meta, reason, at, SagaRunnerConfig.defaults().reorderWindow())).envelope();
        SagaEnvelope<String> bare = requireNonNull(SagaExecutionSupport.onReleaseTooLargeBare(releasing, meta, reason, at, SagaRunnerConfig.defaults().reorderWindow())).envelope();
        assertAll(
                () -> assertThat(holdsTheType(before, longType)).as("control: the instance does not hold the long type before the release").isFalse(),
                () -> assertThat(sizeBefore).as("control: stored with the room the long type test names").isEqualTo((int) MAX_DOCUMENT_SIZE - LONG_TYPE_ROOM),
                () -> assertThat(envelopes.fits(stateStore, full)).as("control: the full requarantine with a pending entry for the type is over the limit").isFalse(),
                () -> assertThat(envelopes.fits(stateStore, bare)).as("control: the bare requarantine is at or under the limit").isTrue(),
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(after.failure().input()).isEqualTo(recorded.key()),
                () -> assertThat(after.failure().failureMessage()).as("the bare write has no failure message").isNull(),
                () -> assertThat(remaining).extracting(SkippedInput::key).as("the event stays recorded").containsExactly(recorded.key()),
                () -> assertThat(dispatched).as("book was applied before the stop, the long typed event was not").containsExactly(new Pack("book", 2)),
                () -> assertThat(sizeAfter).isLessThanOrEqualTo((int) MAX_DOCUMENT_SIZE)
        );
    }

    private static boolean holdsTheType(SagaEnvelope<String> envelope, String type) {
        boolean inMarks = envelope.streamMarks().values().stream().anyMatch(marks -> marks.byType().containsKey(type) || marks.pending().containsKey(type));
        SagaFailure failure = envelope.failure();
        return inMarks || failure != null && (failure.input().contains(type) || failure.failureMessage() != null && failure.failureMessage().contains(type));
    }

    private static SagaEnvelope<String> releasingCopyOf(SagaEnvelope<String> stopped) {
        SagaFailure failure = stopped.failure();
        SagaFailure releasing = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(), failure.failureType(), failure.failureMessage(), Instant.now(), failure.quarantineId());
        return new SagaEnvelope<>(stopped.sagaId(), stopped.state(), stopped.status(), stopped.version(), stopped.timers(),
                stopped.streamWatermarks(), stopped.positionWatermark(), stopped.createdAt(), stopped.updatedAt(), stopped.completedAt(),
                stopped.currentStep(), stopped.started(), releasing, stopped.streamMarks(), stopped.quarantineSkipCount());
    }

    @Test
    void an_instance_stored_at_the_hard_limit_cannot_start_a_release_so_release_throws_naming_the_instance_and_the_delete_and_changes_nothing() {
        quarantinedOnBookWithPenRecorded();
        save(withPadding(withStreams(STREAMS, 0), HARD_LIMIT_PAD));
        int sizeBefore = storedSize();
        long versionBefore = instance().version();

        Throwable thrown = catchThrowable(() -> subscription.release(ORDER));

        SagaEnvelope<String> after = instance();
        assertAll(
                () -> assertThat(sizeBefore).as("control: stored at the hard limit").isEqualTo((int) MAX_DOCUMENT_SIZE),
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining(ORDER).hasMessageContaining("SagaStateStore.delete"),
                () -> assertThat(after.version()).isEqualTo(versionBefore),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(dispatched).isEmpty()
        );
    }

    @Test
    void fits_takes_a_document_of_exactly_the_most_MongoDB_stores_and_refuses_one_byte_more_and_a_release_write_of_that_size_succeeds() {
        quarantinedOnBookWithPenRecorded();
        save(withPadding(withStreams(STREAMS, 0), HARD_LIMIT_PAD));
        SagaEnvelope<String> stored = instance();
        SkippedInput first = stateStore.findSkippedInputs(ORDER, stored.quarantineId(), 1).getFirst();
        int sizeBefore = storedSize();
        SagaEnvelope<String> candidate = withVersionAndState(stored, stored.version() + 1, stored.state());
        SagaEnvelope<String> oneByteMore = withVersionAndState(stored, stored.version() + 1, stored.state() + "x");

        boolean exactlyAtTheLimitFits = envelopes.fits(stateStore, candidate);
        boolean oneByteMoreFits = envelopes.fits(stateStore, oneByteMore);
        boolean written = envelopes.compareAndSaveReplayed(stateStore, candidate, stored.version(), stored.quarantineId(), first);

        assertAll(
                () -> assertThat(sizeBefore).as("control: the stored document is exactly 16,777,216 bytes").isEqualTo((int) MAX_DOCUMENT_SIZE),
                () -> assertThat(exactlyAtTheLimitFits).isTrue(),
                () -> assertThat(oneByteMoreFits).isFalse(),
                () -> assertThat(written).as("MongoDB takes the release write of a document of exactly 16,777,216 bytes").isTrue(),
                () -> assertThat(storedSize()).isEqualTo((int) MAX_DOCUMENT_SIZE),
                () -> assertThat(instance().version()).isEqualTo(stored.version() + 1),
                () -> assertThat(stateStore.findSkippedInputs(ORDER, stored.quarantineId(), Integer.MAX_VALUE)).extracting(SkippedInput::key).doesNotContain(first.key()).hasSize(1)
        );
    }

    private static SagaEnvelope<String> withVersionAndState(SagaEnvelope<String> current, long version, String state) {
        return new SagaEnvelope<>(current.sagaId(), state, current.status(), version, current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
    }

}
