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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies startAtDcbPosition. Three events are appended before the subscriptions start (positions 1, 2, 3). A
 * subscription with startAtDcbPosition = 0 replays all of them. One with startAtDcbPosition = 1 is a bare position
 * above 0, which holds no live start, so its catch-up is refused and nothing is delivered.
 */
@DisplayName("DcbSubscription startAtDcbPosition (DCB-only mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSubscriptionStartAtPositionAnnotationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-start-at-position-test"
        }
)
@Import(DcbSubscriptionStartAtPositionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbSubscriptionStartAtPositionAnnotationMongoTest {

    static final String TAG = "test:start-at-position";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-start-at-position-test");

    @Autowired
    private RecordingSubscriber recordingSubscriber;

    @Autowired
    private RefusedPositionSubscriber refusedPositionSubscriber;

    @Autowired
    private CatchupLog catchupLog;

    @Test
    void subscription_with_startAtDcbPosition_0_delivers_positions_1_2_and_3() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(recordingSubscriber.received())
                        .extracting(TestEvent::name)
                        .containsExactly("event-1", "event-2", "event-3"));
    }

    @Test
    void subscription_with_startAtDcbPosition_1_is_refused_and_delivers_nothing() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(catchupLog.events()).anySatisfy(event -> {
                    assertThat(event.getThrowableProxy()).isNotNull();
                    assertThat(event.getThrowableProxy().getClassName()).isEqualTo(CatchupCannotResumeException.class.getName());
                    assertThat(event.getThrowableProxy().getMessage())
                            .contains("Cannot start catch-up subscription dcb-start-at-refused-position-subscriber after position 1");
                }));

        assertThat(refusedPositionSubscriber.received()).isEmpty();
    }

    // --- inner application and configuration classes ---

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
        CatchupLog catchupLog() {
            return new CatchupLog();
        }

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

        // Appends three distinguishable events before the subscription starts.
        // DCB positions are assigned sequentially from 1 on a fresh store, so they get 1, 2, 3.
        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        // DependsOn the appender so all three events exist before the subscription begins replaying.
        @Bean
        @DependsOn("historyAppender")
        RecordingSubscriber recordingSubscriber() {
            return new RecordingSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        RefusedPositionSubscriber refusedPositionSubscriber() {
            return new RefusedPositionSubscriber();
        }
    }

    // Attached as the context starts, before any subscription does, so it holds the refusal however soon after startup a
    // catch-up thread logs it
    static class CatchupLog {
        private final Logger logger = (Logger) LoggerFactory.getLogger("org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel");
        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        CatchupLog() {
            appender.start();
            logger.addAppender(appender);
        }

        List<ILoggingEvent> events() {
            synchronized (appender) {
                return List.copyOf(appender.list);
            }
        }

        @PreDestroy
        void detach() {
            logger.detachAppender(appender);
            appender.stop();
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
            appendTagged(dcbEventStore, cloudEventConverter,
                    new TestEvent("event-1"),
                    new TestEvent("event-2"),
                    new TestEvent("event-3"));
        }
    }

    static class RecordingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-start-at-position-subscriber", startAtDcbPosition = 0)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class RefusedPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-start-at-refused-position-subscriber", startAtDcbPosition = 1)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<io.cloudevents.CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
