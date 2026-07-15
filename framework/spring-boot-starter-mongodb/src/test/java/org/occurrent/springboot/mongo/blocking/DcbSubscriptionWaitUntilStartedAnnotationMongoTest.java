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
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.DcbSubscription.DcbStartPosition;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
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

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code startupMode = WAIT_UNTIL_STARTED} makes a {@link DcbSubscription} block application startup
 * until it is actually live, even for a {@code BEGINNING} replay, which defaults to {@code BACKGROUND} (non-blocking)
 * startup. A live event appended the instant the context is up is received with no startup race.
 * <p>
 * Finer {@code startupMode} timing (for example proving {@code BACKGROUND} does NOT block) is not asserted here: it
 * is not deterministically observable without sleeps, since "the context returned before catch-up finished" isn't a
 * fact a test can reliably measure. The replaying scenarios in the other annotation tests already exercise
 * {@code BACKGROUND} implicitly (it's the default for a replaying start) and prove eventual delivery.
 */
@DisplayName("DcbSubscription startupMode WAIT_UNTIL_STARTED blocks startup until live")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbSubscriptionWaitUntilStartedAnnotationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dcb-wait-until-started-test"
        }
)
@Import(DcbSubscriptionWaitUntilStartedAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbSubscriptionWaitUntilStartedAnnotationMongoTest {

    static final String TAG = "test:wait-until-started";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-wait-until-started-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private WaitUntilStartedSubscriber subscriber;

    @Test
    void a_live_event_appended_immediately_after_context_startup_is_received_with_no_race() {
        // By the time the context is up, WAIT_UNTIL_STARTED already forced the BEGINNING replay (of nothing, here)
        // and the live change stream to be fully attached, so this event, appended with no delay, is not missed.
        List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new TestEvent("live-1")))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents);

        await().atMost(ofSeconds(5)).untilAsserted(() ->
                assertThat(subscriber.received()).extracting(TestEvent::name).containsExactly("live-1"));
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

        @Bean
        WaitUntilStartedSubscriber waitUntilStartedSubscriber() {
            return new WaitUntilStartedSubscriber();
        }
    }

    static class WaitUntilStartedSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-wait-until-started", startAt = DcbStartPosition.BEGINNING, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
