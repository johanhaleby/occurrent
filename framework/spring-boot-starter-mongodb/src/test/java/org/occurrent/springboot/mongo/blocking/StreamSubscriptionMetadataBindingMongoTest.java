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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that the {@link StreamSubscription} annotation processor binds the {@link EventMetadata} parameter
 * correctly regardless of its declared position, mirroring {@code DcbSubscriptionMetadataBindingMongoTest} for the
 * stream side, which previously had no annotation-level metadata binding coverage at all.
 */
@DisplayName("StreamSubscription metadata parameter binding")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = StreamSubscriptionMetadataBindingMongoTest.MetadataBindingApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:stream-metadata-binding-test"
        }
)
@Import(StreamSubscriptionMetadataBindingMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class StreamSubscriptionMetadataBindingMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:stream-metadata-binding-test");

    @Autowired
    private ApplicationService<MyEvent> applicationService;

    @Autowired
    private EventOnlyReceiver eventOnlyReceiver;

    @Autowired
    private EventFirstMetadataSecondReceiver eventFirstMetadataSecondReceiver;

    @Autowired
    private MetadataFirstEventSecondReceiver metadataFirstEventSecondReceiver;

    @Test
    void event_only_binds_the_event() {
        append(new MyEvent("meta-event-1"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(eventOnlyReceiver.receivedEvents()).extracting(MyEvent::name).containsExactly("meta-event-1"));
    }

    @Test
    void event_first_metadata_second_binds_both() {
        append(new MyEvent("meta-event-2"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(eventFirstMetadataSecondReceiver.receivedEvents()).extracting(MyEvent::name).containsExactly("meta-event-2"));
        EventMetadata m = eventFirstMetadataSecondReceiver.receivedMetadata().get(0);
        assertThat(m.getStreamId()).isNotBlank();
    }

    @Test
    void metadata_first_event_second_binds_the_correct_parameter_slots() {
        // Before the blocking bindArguments fix (mirrored here for the stream side) this ordering threw
        // "argument type mismatch" at invocation time.
        append(new MyEvent("meta-event-3"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(metadataFirstEventSecondReceiver.receivedEvents()).extracting(MyEvent::name).containsExactly("meta-event-3"));
        EventMetadata m = metadataFirstEventSecondReceiver.receivedMetadata().get(0);
        assertThat(m.getStreamId()).isNotBlank();
    }

    private void append(MyEvent event) {
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(event));
    }

    // --- container configuration ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    // --- application under test ---

    @SpringBootApplication
    @EnableOccurrent
    static class MetadataBindingApplication {

        @Bean
        CloudEventTypeMapper<MyEvent> myEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<MyEvent> myEventCloudEventConverter(CloudEventTypeMapper<MyEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<MyEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        EventOnlyReceiver eventOnlyReceiver() {
            return new EventOnlyReceiver();
        }

        @Bean
        EventFirstMetadataSecondReceiver eventFirstMetadataSecondReceiver() {
            return new EventFirstMetadataSecondReceiver();
        }

        @Bean
        MetadataFirstEventSecondReceiver metadataFirstEventSecondReceiver() {
            return new MetadataFirstEventSecondReceiver();
        }
    }

    // --- receivers (one per subscription so ids are unique) ---

    static class EventOnlyReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "stream-meta-binding-event-only")
        void on(MyEvent event) {
            if ("meta-event-1".equals(event.name())) {
                events.add(event);
            }
        }

        List<MyEvent> receivedEvents() { return events; }
    }

    static class EventFirstMetadataSecondReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<EventMetadata> metadata = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "stream-meta-binding-event-first")
        void on(MyEvent event, EventMetadata meta) {
            if ("meta-event-2".equals(event.name())) {
                events.add(event);
                metadata.add(meta);
            }
        }

        List<MyEvent> receivedEvents() { return events; }
        List<EventMetadata> receivedMetadata() { return metadata; }
    }

    static class MetadataFirstEventSecondReceiver {
        private final CopyOnWriteArrayList<MyEvent> events = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<EventMetadata> metadata = new CopyOnWriteArrayList<>();

        // Metadata is declared FIRST -- this is the ordering the fixed bindArguments must handle correctly.
        @StreamSubscription(id = "stream-meta-binding-metadata-first")
        void on(EventMetadata meta, MyEvent event) {
            if ("meta-event-3".equals(event.name())) {
                events.add(event);
                metadata.add(meta);
            }
        }

        List<MyEvent> receivedEvents() { return events; }
        List<EventMetadata> receivedMetadata() { return metadata; }
    }

    record MyEvent(String eventId, Date timestamp, String name) {
        MyEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
