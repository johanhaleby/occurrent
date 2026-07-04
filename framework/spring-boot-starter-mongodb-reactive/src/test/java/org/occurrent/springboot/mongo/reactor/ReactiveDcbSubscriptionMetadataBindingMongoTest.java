/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.DcbSubscription.DcbStartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reactive counterpart of {@code DcbSubscriptionMetadataBindingMongoTest}: proves the reactive
 * {@link DcbSubscription} annotation processor binds the metadata parameter correctly regardless of its declared
 * position in the method signature, through the reactive {@code subscribeWithMetadata} path. Three binding
 * configurations are exercised:
 *
 * - event-first, DcbEventMetadata second (position and dcbTags populated)
 * - metadata-first, event second (the argument-ordering the blocking bindArguments fix covers)
 * - event-first, generic EventMetadata second (dcbTags readable through the raw data map)
 */
@DisplayName("Reactive DcbSubscription metadata parameter binding")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveDcbSubscriptionMetadataBindingMongoTest.MetadataBindingApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-dcb-metadata-binding-test"
        }
)
@Import(ReactiveDcbSubscriptionMetadataBindingMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveDcbSubscriptionMetadataBindingMongoTest {

    static final String TAG = "test:reactive-metadata-binding";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-dcb-metadata-binding-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<MyEvent> cloudEventConverter;

    @Autowired
    private EventFirstDcbMetadataReceiver eventFirstDcbMetadataReceiver;

    @Autowired
    private MetadataFirstReceiver metadataFirstReceiver;

    @Autowired
    private EventFirstGenericMetadataReceiver eventFirstGenericMetadataReceiver;

    // --- tests ---

    @Test
    void event_first_dcb_metadata_second_binds_both_with_position_and_tags_populated() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(eventFirstDcbMetadataReceiver.receivedEvents()).hasSize(1);
            assertThat(eventFirstDcbMetadataReceiver.receivedEvents().get(0).name()).isEqualTo("meta-event-1");
        });
        DcbEventMetadata m = eventFirstDcbMetadataReceiver.receivedMetadata().get(0);
        assertThat(m.position()).isPresent();
        assertThat(m.dcbTags()).contains(TAG);
    }

    @Test
    void metadata_first_event_second_binds_the_correct_parameter_slots() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(metadataFirstReceiver.receivedEvents()).hasSize(1);
            assertThat(metadataFirstReceiver.receivedEvents().get(0).name()).isEqualTo("meta-event-2");
        });
        DcbEventMetadata m = metadataFirstReceiver.receivedMetadata().get(0);
        assertThat(m.position()).isPresent();
        assertThat(m.dcbTags()).contains(TAG);
    }

    @Test
    void generic_event_metadata_second_exposes_dcb_tags_through_raw_data_map() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(eventFirstGenericMetadataReceiver.receivedEvents()).hasSize(1);
            assertThat(eventFirstGenericMetadataReceiver.receivedEvents().get(0).name()).isEqualTo("meta-event-3");
        });
        EventMetadata m = eventFirstGenericMetadataReceiver.receivedMetadata().get(0);
        assertThat(m.getData()).containsKey(DcbCloudEvents.TAGS);
        assertThat(DcbEventMetadata.from(m).dcbTags()).contains(TAG);
    }

    // --- container configuration ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    // --- application under test ---

    @SpringBootApplication
    @EnableOccurrentReactive
    static class MetadataBindingApplication {

        @Bean
        CloudEventTypeMapper<MyEvent> myEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<MyEvent> myEventCloudEventConverter(CloudEventTypeMapper<MyEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<MyEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(MyEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        HistorySeeder historySeeder(DcbEventStore store, CloudEventConverter<MyEvent> converter) {
            return new HistorySeeder(store, converter);
        }

        @Bean
        @DependsOn("historySeeder")
        EventFirstDcbMetadataReceiver eventFirstDcbMetadataReceiver() {
            return new EventFirstDcbMetadataReceiver();
        }

        @Bean
        @DependsOn("historySeeder")
        MetadataFirstReceiver metadataFirstReceiver() {
            return new MetadataFirstReceiver();
        }

        @Bean
        @DependsOn("historySeeder")
        EventFirstGenericMetadataReceiver eventFirstGenericMetadataReceiver() {
            return new EventFirstGenericMetadataReceiver();
        }
    }

    // --- history seeder ---

    static class HistorySeeder {

        HistorySeeder(DcbEventStore store, CloudEventConverter<MyEvent> converter) {
            List<CloudEvent> batch = converter.toCloudEvents(Stream.of(
                            new MyEvent("meta-event-1"),
                            new MyEvent("meta-event-2"),
                            new MyEvent("meta-event-3")
                    ))
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(TAG)))
                    .toList();
            store.append(batch).block();
        }
    }

    // --- receivers (one per subscription so ids are unique) ---

    static class EventFirstDcbMetadataReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<DcbEventMetadata> metadata = new CopyOnWriteArrayList<>();

        @DcbSubscription(
                id = "reactive-meta-binding-event-first-dcb",
                eventTypes = MyEvent.class,
                startAt = DcbStartPosition.BEGINNING
        )
        Mono<Void> on(MyEvent event, DcbEventMetadata meta) {
            if ("meta-event-1".equals(event.name())) {
                events.add(event);
                metadata.add(meta);
            }
            return Mono.empty();
        }

        List<MyEvent> receivedEvents() { return events; }
        List<DcbEventMetadata> receivedMetadata() { return metadata; }
    }

    static class MetadataFirstReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<DcbEventMetadata> metadata = new CopyOnWriteArrayList<>();

        // Metadata is declared FIRST -- this is the ordering the fixed bindArguments must handle correctly.
        @DcbSubscription(
                id = "reactive-meta-binding-metadata-first",
                eventTypes = MyEvent.class,
                startAt = DcbStartPosition.BEGINNING
        )
        Mono<Void> on(DcbEventMetadata meta, MyEvent event) {
            if ("meta-event-2".equals(event.name())) {
                events.add(event);
                metadata.add(meta);
            }
            return Mono.empty();
        }

        List<MyEvent> receivedEvents() { return events; }
        List<DcbEventMetadata> receivedMetadata() { return metadata; }
    }

    static class EventFirstGenericMetadataReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<EventMetadata> metadata = new CopyOnWriteArrayList<>();

        @DcbSubscription(
                id = "reactive-meta-binding-event-first-generic",
                eventTypes = MyEvent.class,
                startAt = DcbStartPosition.BEGINNING
        )
        Mono<Void> on(MyEvent event, EventMetadata meta) {
            if ("meta-event-3".equals(event.name())) {
                events.add(event);
                metadata.add(meta);
            }
            return Mono.empty();
        }

        List<MyEvent> receivedEvents() { return events; }
        List<EventMetadata> receivedMetadata() { return metadata; }
    }

    // --- domain model ---

    record MyEvent(String eventId, Date timestamp, String name) {
        MyEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
