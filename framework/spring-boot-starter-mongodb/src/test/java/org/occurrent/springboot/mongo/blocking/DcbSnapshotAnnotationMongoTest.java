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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the blocking {@code @Snapshot} registrar maintains a per-boundary snapshot for a {@code DcbSnapshotView}: events
 * matching the criteria fold into one snapshot keyed by the canonical criteria key and saved at the marker versions of
 * the read that produced it, backed by the zero-config MongoDB snapshot store.
 */
@DisplayName("Dcb Snapshot annotation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSnapshotAnnotationMongoTest.DcbSnapshotApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-snapshot-test"
        }
)
@Import(DcbSnapshotAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbSnapshotAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:dcb-snapshot-test");
    private static final String TAG = "account:1";
    private static final String RANGE_TAG = "account:range-meta";
    private static final String HELD_TAG = "account:held";
    private static final String LEGACY_TAG = "account:legacy";
    private static final int SCHEMA_VERSION = 2;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<CounterEvent> converter;

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoTransactionManager transactionManager;

    @Autowired
    private EventStoreConfig eventStoreConfig;

    @Test
    void maintains_a_per_boundary_snapshot() {
        appendTagged(dcbEventStore, converter, new Incremented(1), new Incremented(2), new Incremented(3));

        DcbCriteria criteria = DcbCriteria.tagsAnyOf(Tag.parse(TAG));
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        SnapshotStore<Counter> store = new SpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-dcb-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(6);
                    assertThat(snapshot.schemaVersion()).isEqualTo(SCHEMA_VERSION);
                    assertThat(snapshot.markerVersions()).as("marker versions the snapshot resumes from").hasValueSatisfying(versions -> assertThat(versions.keys()).containsExactly("tag:" + TAG));
                    assertThat(snapshot.version()).isZero();
                }));
    }

    @Test
    void folds_an_event_that_commits_after_a_later_event_was_folded_and_saved() {
        SnapshotStore<Counter> store = new SpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-held-dcb-counter");
        String key = DcbSnapshotKeys.canonicalKey(DcbCriteria.tagsAnyOf(Tag.parse(HELD_TAG)));
        appendTagged(dcbEventStore, converter, HELD_TAG, new Incremented(1));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> assertThat(snapshot.state().total()).isEqualTo(1)));
        CountDownLatch held = new CountDownLatch(1);
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        SpringMongoEventStore holding = new SpringMongoEventStore(mongoTemplate, new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventStoreConfig.eventStoreCollectionName)
                .transactionConfig(new HoldingTransactionTemplate(transactionManager, () -> {
                    if (first.compareAndSet(true, false)) {
                        held.countDown();
                        release.join();
                    }
                }))
                .timeRepresentation(eventStoreConfig.timeRepresentation)
                .eventStoreCapabilities(eventStoreConfig.eventStoreCapabilities)
                .build());
        CompletableFuture<Void> heldAppend = CompletableFuture.runAsync(() -> appendTagged(holding, converter, HELD_TAG, new Incremented(10)));
        assertThat(awaitHeld(held)).as("the append reached the hold").isTrue();

        appendTagged(dcbEventStore, converter, HELD_TAG, new Incremented(100));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> assertThat(snapshot.state().total()).isEqualTo(101)));
        release.complete(null);
        heldAppend.orTimeout(30, TimeUnit.SECONDS).join();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> assertThat(snapshot.state().total()).isEqualTo(111)));
    }

    @Test
    void replaces_a_snapshot_saved_at_a_position_with_one_at_marker_versions_read_from_the_whole_boundary() {
        String collection = "occurrent-snapshot-legacy-dcb-counter";
        String key = DcbSnapshotKeys.canonicalKey(DcbCriteria.tagsAnyOf(Tag.parse(LEGACY_TAG)));
        Document document = new Document("_id", key).append("version", 1_000_000L).append("schemaVersion", SCHEMA_VERSION).append("state", new Document("total", 999));
        mongoOperations.save(document, collection);

        appendTagged(dcbEventStore, converter, LEGACY_TAG, new Incremented(1), new Incremented(2));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            Document stored = mongoOperations.findById(key, Document.class, collection);
            assertThat(stored).isNotNull();
            assertThat(stored.keySet()).as("the 0.34 snapshot was replaced").contains("markerVersions").doesNotContain("version");
            assertThat(stored.get("state", Document.class).getInteger("total")).as("total folded from the whole boundary, not from the position snapshot").isEqualTo(3);
        });
    }

    @Test
    void a_range_update_folds_each_dcb_event_with_its_own_metadata() {
        appendTagged(dcbEventStore, converter, RANGE_TAG, new Incremented(5), new Incremented(9));

        DcbCriteria criteria = DcbCriteria.tagsAnyOf(Tag.parse(RANGE_TAG));
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        SnapshotStore<MetaRangeCounter> store = new SpringMongoSnapshotStore<>(mongoOperations, MetaRangeCounter.class, "occurrent-snapshot-meta-range-dcb-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest(key)).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(14);
                    assertThat(snapshot.state().streamVersionSum()).isEqualTo(3L); // 1 + 2, not e.g. the last event's version counted twice
                }));
    }

    private static boolean awaitHeld(CountDownLatch held) {
        try {
            return held.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class HoldingTransactionTemplate extends TransactionTemplate {

        private final transient Runnable hold;

        HoldingTransactionTemplate(MongoTransactionManager transactionManager, Runnable hold) {
            super(transactionManager);
            this.hold = hold;
        }

        @Override
        public <T> T execute(TransactionCallback<T> action) {
            hold.run();
            return super.execute(action);
        }
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<CounterEvent> converter, CounterEvent... events) {
        appendTagged(dcbEventStore, converter, TAG, events);
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<CounterEvent> converter, String tag, CounterEvent... events) {
        List<CloudEvent> cloudEvents = Arrays.stream(events)
                .map(event -> converter.toCloudEvent(event))
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(tag))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DcbSnapshotApplication {
        @Bean
        CloudEventTypeMapper<CounterEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<CounterEvent> converter(CloudEventTypeMapper<CounterEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<CounterEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(CounterEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Component
        static class CounterSnapshot {
            @Snapshot(id = "dcb-counter")
            DcbSnapshotView<Counter, CounterEvent> counterSnapshot() {
                return new DcbSnapshotView<>(
                        SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                                .schemaVersion(SCHEMA_VERSION)
                                .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                                .build(),
                        DcbCriteria.tagsAnyOf(Tag.parse(TAG)));
            }
        }

        @Component
        static class MetaRangeCounterSnapshot {
            @Snapshot(id = "meta-range-dcb-counter", everyNEvents = 2)
            DcbSnapshotView<MetaRangeCounter, CounterEvent> metaRangeCounterSnapshot() {
                return new DcbSnapshotView<>(
                        SnapshotView.<MetaRangeCounter, CounterEvent>builder(new MetaRangeCounter(0, 0))
                                .on(Incremented.class, (state, metadata, event) -> new MetaRangeCounter(state.total() + event.amount(), state.streamVersionSum() + metadata.getStreamVersion()))
                                .build(),
                        DcbCriteria.tagsAnyOf(Tag.parse(RANGE_TAG)));
            }
        }

        @Component
        static class LegacyCounterSnapshot {
            @Snapshot(id = "legacy-dcb-counter")
            DcbSnapshotView<Counter, CounterEvent> legacyCounterSnapshot() {
                return new DcbSnapshotView<>(
                        SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                                .schemaVersion(SCHEMA_VERSION)
                                .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                                .build(),
                        DcbCriteria.tagsAnyOf(Tag.parse(LEGACY_TAG)));
            }
        }

        @Component
        static class HeldCounterSnapshot {
            @Snapshot(id = "held-dcb-counter")
            DcbSnapshotView<Counter, CounterEvent> heldCounterSnapshot() {
                return new DcbSnapshotView<>(
                        SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                                .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                                .build(),
                        DcbCriteria.tagsAnyOf(Tag.parse(HELD_TAG)));
            }
        }
    }

    record Counter(int total) {
    }

    record MetaRangeCounter(int total, long streamVersionSum) {
    }

    sealed interface CounterEvent {
        String eventId();

        Date timestamp();

        int amount();
    }

    record Incremented(String eventId, Date timestamp, int amount) implements CounterEvent {
        Incremented(int amount) {
            this(UUID.randomUUID().toString(), new Date(), amount);
        }
    }
}
