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

package org.occurrent.springboot.mongo.reactor;

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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.mongodb.spring.reactor.ReactiveSpringMongoSnapshotStore;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.stereotype.Component;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the reactive {@code @Snapshot} registrar maintains a per-stream, resume-ready snapshot: each stream folds into
 * its own snapshot document keyed by the stream id, carrying the folded state, the stream version it reached, and the
 * declared schema version, backed by the zero-config reactive MongoDB snapshot store.
 */
@DisplayName("Reactive Snapshot annotation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSnapshotAnnotationMongoTest.SnapshotApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-snapshot-test"
        }
)
@Import(ReactiveSnapshotAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSnapshotAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-snapshot-test");
    private static final int SCHEMA_VERSION = 4;

    @Autowired
    private ApplicationService<CounterEvent> applicationService;

    @Autowired
    private org.occurrent.eventstore.api.reactor.EventStoreOperations eventStoreOperations;

    @Autowired
    private ReactiveMongoOperations mongoOperations;

    @Test
    void maintains_a_resume_ready_snapshot_per_stream() {
        applicationService.execute("counter-1", __ -> List.of(new Incremented(1), new Incremented(2), new Incremented(3))).block();
        applicationService.execute("counter-2", __ -> List.of(new Incremented(10))).block();

        ReactiveSnapshotStore<Counter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-reactive-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(store.findLatest("counter-1").blockOptional()).hasValueSatisfying(snapshot -> {
                assertThat(snapshot.state().total()).isEqualTo(6);
                assertThat(snapshot.version()).isEqualTo(3L);
                assertThat(snapshot.schemaVersion()).isEqualTo(SCHEMA_VERSION);
            });
            assertThat(store.findLatest("counter-2").blockOptional()).hasValueSatisfying(snapshot -> {
                assertThat(snapshot.state().total()).isEqualTo(10);
                assertThat(snapshot.version()).isEqualTo(1L);
            });
        });
    }

    @Test
    void a_reset_stream_rebuilds_the_snapshot_instead_of_freezing_the_maintainer() {
        ReactiveSnapshotStore<Counter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-reactive-counter");

        applicationService.execute("counter-reset", __ -> List.of(new Incremented(1), new Incremented(2), new Incremented(3))).block();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest("counter-reset").blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.version()).isEqualTo(3L);
                    assertThat(snapshot.state().total()).isEqualTo(6);
                }));

        // Reset the stream below the surviving snapshot (still at version 3) and rewrite it shorter. The maintainer must
        // not freeze on the stale snapshot: the head probe sees head 1 < snapshot 3, demotes to initial, folds the reset
        // stream fresh, and self-heals the snapshot down to the reset stream's state.
        eventStoreOperations.deleteEventStream("counter-reset").block();
        applicationService.execute("counter-reset", __ -> List.of(new Incremented(100))).block();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest("counter-reset").blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.version()).isEqualTo(1L);
                    assertThat(snapshot.state().total()).isEqualTo(100);
                }));
    }

    @Test
    void a_single_event_update_gives_the_fold_the_events_metadata() {
        ReactiveSnapshotStore<MetaCounter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, MetaCounter.class, "occurrent-snapshot-reactive-meta-counter");

        applicationService.execute("counter-meta", __ -> List.of(new Incremented(7))).block();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest("counter-meta").blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(7);
                    assertThat(snapshot.state().lastSeenStreamVersion()).isEqualTo(1L);
                }));
    }

    @Test
    void a_range_update_folds_each_event_with_its_own_metadata() {
        ReactiveSnapshotStore<MetaRangeCounter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, MetaRangeCounter.class, "occurrent-snapshot-reactive-meta-range-counter");

        // everyNEvents = 2 means the first delivery (version 1) is throttled and the second delivery (version 2)
        // takes the range branch (folding versions 1 and 2 from a store read, not the single-event branch).
        applicationService.execute("counter-meta-range", __ -> List.of(new Incremented(5), new Incremented(9))).block();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest("counter-meta-range").blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(14);
                    assertThat(snapshot.state().streamVersionSum()).isEqualTo(3L); // 1 + 2, not e.g. the last event's version counted twice
                }));
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
    @EnableOccurrentReactive
    static class SnapshotApplication {

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
            @Snapshot(id = "reactive-counter")
            SnapshotView<Counter, CounterEvent> counterSnapshot() {
                return SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                        .schemaVersion(SCHEMA_VERSION)
                        .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                        .build();
            }
        }

        @Component
        static class MetaCounterSnapshot {
            @Snapshot(id = "reactive-meta-counter")
            SnapshotView<MetaCounter, CounterEvent> metaCounterSnapshot() {
                return SnapshotView.<MetaCounter, CounterEvent>builder(new MetaCounter(0, 0))
                        .on(Incremented.class, (state, metadata, event) -> new MetaCounter(state.total() + event.amount(), metadata.getStreamVersion()))
                        .build();
            }
        }

        @Component
        static class MetaRangeCounterSnapshot {
            @Snapshot(id = "reactive-meta-range-counter", everyNEvents = 2)
            SnapshotView<MetaRangeCounter, CounterEvent> metaRangeCounterSnapshot() {
                return SnapshotView.<MetaRangeCounter, CounterEvent>builder(new MetaRangeCounter(0, 0))
                        .on(Incremented.class, (state, metadata, event) -> new MetaRangeCounter(state.total() + event.amount(), state.streamVersionSum() + metadata.getStreamVersion()))
                        .build();
            }
        }
    }

    record Counter(int total) {
    }

    record MetaCounter(int total, long lastSeenStreamVersion) {
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
