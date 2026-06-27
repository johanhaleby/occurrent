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

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
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
 * Proves that a {@link DcbSubscription} method is wired in DCB-only mode: it replays the DCB history appended before
 * startup, then receives live events, and its {@code eventTypes} narrowing keeps non-matching events out. The history
 * is appended by a bean the subscriber {@link DependsOn}, so it exists before the annotation subscription starts.
 */
@DisplayName("DcbSubscription annotation (DCB-only mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSubscriptionAnnotationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-annotation-test"
        }
)
@Import(DcbSubscriptionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbSubscriptionAnnotationMongoTest {

    static final String TAG = "test:annotation";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-annotation-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private RecordingDashboard recordingDashboard;

    @Test
    void replays_history_then_receives_live_events_and_excludes_non_matching_types() {
        // The two Included events were appended before startup by HistoryAppender, so the BEGINNING subscription
        // replays them.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingDashboard.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));

        // A live Included event arrives after the catch-up phase.
        append(new Included("live-1"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingDashboard.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "live-1"));

        // An Excluded event must never reach the subscription, since eventTypes narrows to Included only.
        append(new Excluded("excluded-1"));
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                assertThat(recordingDashboard.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "live-1"));
    }

    private void append(TestEvent... events) {
        appendTagged(dcbEventStore, cloudEventConverter, events);
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(Stream.of(events))
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(TAG)))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- inner application and configuration classes ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DcbOnlyApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // Appends the DCB history before the subscriber starts, so the BEGINNING subscription has something to replay.
        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        // DependsOn the appender so the history is in place before this bean's @DcbSubscription starts replaying.
        @Bean
        @DependsOn("historyAppender")
        RecordingDashboard recordingDashboard() {
            return new RecordingDashboard();
        }
    }

    static class HistoryAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        HistoryAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendHistory() {
            appendTagged(dcbEventStore, cloudEventConverter, new Included("historic-1"), new Included("historic-2"));
        }
    }

    static class RecordingDashboard {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-annotation-dashboard", eventTypes = Included.class, startAt = DcbStartPosition.BEGINNING)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Included(String eventId, Date timestamp, String name) implements TestEvent {
        Included(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record Excluded(String eventId, Date timestamp, String name) implements TestEvent {
        Excluded(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
