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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Fills the reactive DCB start-position gap: {@code ReactiveDcbSubscriptionAnnotationMongoTest} only exercises
 * {@code startAt = BEGINNING}. This proves {@code DEFAULT} and {@code NOW} never replay, and that
 * {@code startAtDcbPosition} correctly resumes strictly after the given position, mirroring the blocking
 * {@code DcbSubscriptionDefaultAndNowStartPositionAnnotationMongoTest} and
 * {@code DcbSubscriptionStartAtPositionAnnotationMongoTest}.
 */
@DisplayName("Reactive DcbSubscription startAt matrix")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveDcbSubscriptionStartPositionAnnotationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-dcb-start-position-test"
        }
)
@Import(ReactiveDcbSubscriptionStartPositionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveDcbSubscriptionStartPositionAnnotationMongoTest {

    static final String TAG = "test:reactive-start-position";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-dcb-start-position-test");

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private DefaultPositionSubscriber defaultPositionSubscriber;

    @Autowired
    private NowPositionSubscriber nowPositionSubscriber;

    @Autowired
    private ExplicitPositionSubscriber explicitPositionSubscriber;

    @Test
    void default_and_now_never_replay_while_an_explicit_position_resumes_strictly_after_it() {
        // Neither DEFAULT nor NOW ever sees the pre-existing history.
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(defaultPositionSubscriber.invocationCount()).isZero();
            assertThat(defaultPositionSubscriber.received()).isEmpty();
        });
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(nowPositionSubscriber.invocationCount()).isZero();
            assertThat(nowPositionSubscriber.received()).isEmpty();
        });

        // startAtDcbPosition = 1 means: deliver from position 2 onward, i.e. skip "event-1".
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(explicitPositionSubscriber.received()).extracting(TestEvent::name).containsExactlyInAnyOrder("event-2", "event-3"));
        assertThat(explicitPositionSubscriber.received()).extracting(TestEvent::name).doesNotContain("event-1");

        append(new TestEvent("live-default"));
        append(new TestEvent("live-now"));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(defaultPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-default"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(nowPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-now"));
    }

    private void append(TestEvent event) {
        List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(event))
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(TAG)))
                .toList();
        dcbEventStore.append(cloudEvents).block();
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
    @EnableOccurrentReactive
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

        // Appends three distinguishable events before any subscriber starts. DCB positions are assigned
        // sequentially from 1 on a fresh store, so they get 1, 2, 3.
        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        DefaultPositionSubscriber defaultPositionSubscriber() {
            return new DefaultPositionSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        NowPositionSubscriber nowPositionSubscriber() {
            return new NowPositionSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        ExplicitPositionSubscriber explicitPositionSubscriber() {
            return new ExplicitPositionSubscriber();
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
            List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(Stream.of(
                            new TestEvent("event-1"), new TestEvent("event-2"), new TestEvent("event-3")))
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(TAG)))
                    .toList();
            dcbEventStore.append(cloudEvents).block();
        }
    }

    // --- subscribers ---

    static class DefaultPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @DcbSubscription(id = "reactive-dcb-sp-default")
        Mono<Void> onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-default")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class NowPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @DcbSubscription(id = "reactive-dcb-sp-now", startAt = DcbStartPosition.NOW)
        Mono<Void> onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-now")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class ExplicitPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "reactive-dcb-sp-explicit-position", startAtDcbPosition = 1)
        Mono<Void> onEvent(TestEvent event) {
            received.add(event);
            return Mono.empty();
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
