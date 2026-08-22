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
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Transactional;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that a {@link Transactional} {@link DcbSubscription} handler with {@code startAt = BEGINNING} and
 * {@code startupMode = WAIT_UNTIL_STARTED} does not hang Spring Boot startup. That combination replays its history
 * synchronously inside the {@code BeanPostProcessor}, on the same thread Spring is using to create the handler bean,
 * so a handler lookup that unconditionally asked the {@code ApplicationContext} for that bean by name deadlocked
 * against its own creation. The context reaching {@link SpringBootTest} at all is the assertion.
 */
@DisplayName("DcbSubscription WAIT_UNTIL_STARTED replay with a Transactional handler")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSubscriptionWaitUntilStartedTransactionalStartupMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-wait-until-started-transactional-startup-test"
        }
)
@Import(DcbSubscriptionWaitUntilStartedTransactionalStartupMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbSubscriptionWaitUntilStartedTransactionalStartupMongoTest {

    static final String TAG = "test:wait-until-started-transactional-startup";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-wait-until-started-transactional-startup-test");

    @Autowired
    private RecordingDashboard recordingDashboard;

    @Test
    void the_context_starts_and_the_replayed_history_is_delivered() {
        assertThat(recordingDashboard.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2");
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

        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

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
            List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new TestEvent(UUID.randomUUID().toString(), new Date(), "historic-1"), new TestEvent(UUID.randomUUID().toString(), new Date(), "historic-2")))
                    .stream()
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                    .toList();
            dcbEventStore.append(cloudEvents);
        }
    }

    static class RecordingDashboard {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @Transactional
        @DcbSubscription(id = "wait-until-started-transactional-startup-dashboard", eventTypes = TestEvent.class, startAt = StartPosition.BEGINNING, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
