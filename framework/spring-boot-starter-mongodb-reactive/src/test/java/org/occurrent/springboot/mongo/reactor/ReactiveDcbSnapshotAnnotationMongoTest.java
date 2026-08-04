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

import org.occurrent.dsl.snapshot.mongodb.spring.reactor.ReactiveSpringMongoSnapshotStore;
import io.cloudevents.CloudEvent;
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
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the reactive {@code @Snapshot} registrar maintains a per-boundary snapshot for a {@code DcbSnapshotView}: events
 * matching the criteria fold into one snapshot keyed by the canonical criteria key and versioned by the global DCB
 * position, backed by the zero-config reactive MongoDB snapshot store.
 */
@DisplayName("Reactive Dcb Snapshot annotation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveDcbSnapshotAnnotationMongoTest.DcbSnapshotApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-dcb-snapshot-test"
        }
)
@Import(ReactiveDcbSnapshotAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveDcbSnapshotAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-dcb-snapshot-test");
    private static final String TAG = "account:1";
    private static final String RANGE_TAG = "account:range-meta";
    private static final int SCHEMA_VERSION = 5;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<CounterEvent> converter;

    @Autowired
    private ReactiveMongoOperations mongoOperations;

    @Test
    void maintains_a_per_boundary_snapshot() {
        appendTagged(dcbEventStore, converter, new Incremented(1), new Incremented(2), new Incremented(3));

        DcbCriteria criteria = DcbCriteria.tagsAnyOf(Tag.parse(TAG));
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        ReactiveSnapshotStore<Counter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-reactive-dcb-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest(key).blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(6);
                    assertThat(snapshot.schemaVersion()).isEqualTo(SCHEMA_VERSION);
                }));
    }

    @Test
    void a_range_update_folds_each_dcb_event_with_its_own_metadata() {
        appendTagged(dcbEventStore, converter, RANGE_TAG, new Incremented(5), new Incremented(9));

        DcbCriteria criteria = DcbCriteria.tagsAnyOf(Tag.parse(RANGE_TAG));
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        ReactiveSnapshotStore<MetaRangeCounter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, MetaRangeCounter.class, "occurrent-snapshot-reactive-meta-range-dcb-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(store.findLatest(key).blockOptional()).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(14);
                    assertThat(snapshot.state().streamVersionSum()).isEqualTo(3L); // 1 + 2, not e.g. the last event's version counted twice
                }));
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<CounterEvent> converter, CounterEvent... events) {
        appendTagged(dcbEventStore, converter, TAG, events);
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<CounterEvent> converter, String tag, CounterEvent... events) {
        List<CloudEvent> cloudEvents = Arrays.stream(events)
                .map(converter::toCloudEvent)
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(tag))))
                .toList();
        dcbEventStore.append(cloudEvents).block();
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
            @Snapshot(id = "reactive-dcb-counter")
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
            @Snapshot(id = "reactive-meta-range-dcb-counter", everyNEvents = 2)
            DcbSnapshotView<MetaRangeCounter, CounterEvent> metaRangeCounterSnapshot() {
                return new DcbSnapshotView<>(
                        SnapshotView.<MetaRangeCounter, CounterEvent>builder(new MetaRangeCounter(0, 0))
                                .on(Incremented.class, (state, metadata, event) -> new MetaRangeCounter(state.total() + event.amount(), state.streamVersionSum() + metadata.getStreamVersion()))
                                .build(),
                        DcbCriteria.tagsAnyOf(Tag.parse(RANGE_TAG)));
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
