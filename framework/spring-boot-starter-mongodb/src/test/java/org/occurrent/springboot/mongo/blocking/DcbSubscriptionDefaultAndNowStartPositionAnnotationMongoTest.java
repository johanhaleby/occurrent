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

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Fills the gap the existing DCB annotation tests leave: {@code DcbSubscriptionAnnotationMongoTest} and
 * {@code DcbSubscriptionStartAtPositionAnnotationMongoTest} both prove replay ({@code BEGINNING} and
 * {@code startAtDcbPosition}), but nothing proves {@code DEFAULT} and {@code NOW} never replay pre-existing DCB
 * history, only deliver live events, mirroring the stream-side coverage.
 */
@DisplayName("DcbSubscription startAt: DEFAULT and NOW never replay")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class DcbSubscriptionDefaultAndNowStartPositionAnnotationMongoTest {

    static final String TAG = "test:default-now-start-position";
    private static final URI SOURCE = URI.create("urn:occurrent:dcb-default-now-start-position-test");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private DcbEventStore dcbEventStore;
    private CloudEventConverter<TestEvent> cloudEventConverter;
    private DefaultPositionSubscriber defaultPositionSubscriber;
    private NowPositionSubscriber nowPositionSubscriber;
    private ConfigurableApplicationContext context;

    @AfterEach
    void closeContext() {
        if (context != null) {
            context.close();
        }
    }

    // The start of a subscription at the present is read when the event store bean is created, so only what is written
    // before the context starts is history. A context with subscriptions off writes it and is closed again.
    @SuppressWarnings("unchecked")
    private void bootAfterTheHistoryIsWritten() {
        SpringApplication.run(DcbOnlyApplication.class, bootArgs("--occurrent.subscription.enabled=false", "--test.history=true")).close();
        context = SpringApplication.run(DcbOnlyApplication.class, bootArgs());
        dcbEventStore = context.getBean(DcbEventStore.class);
        cloudEventConverter = context.getBean(CloudEventConverter.class);
        defaultPositionSubscriber = context.getBean(DefaultPositionSubscriber.class);
        nowPositionSubscriber = context.getBean(NowPositionSubscriber.class);
    }

    private static String[] bootArgs(String... more) {
        return Stream.concat(Stream.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("dcb-default-now-start-position"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE), Stream.of(more)).toArray(String[]::new);
    }

    @Test
    void default_and_now_never_replay_pre_existing_dcb_history_only_live_events() {
        bootAfterTheHistoryIsWritten();

        // Neither subscriber's handler is ever invoked for the historic event, and consequently neither
        // ever sees it in received(), even after settling.
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(defaultPositionSubscriber.invocationCount()).isZero();
            assertThat(defaultPositionSubscriber.received()).isEmpty();
        });
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(nowPositionSubscriber.invocationCount()).isZero();
            assertThat(nowPositionSubscriber.received()).isEmpty();
        });

        append(new TestEvent("live-default"));
        append(new TestEvent("live-now"));

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(defaultPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-default"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(nowPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-now"));
    }

    private void append(TestEvent event) {
        List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- inner application and configuration classes ---

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

        // Appends history in the boot with subscriptions off, which is the one given test.history, so a
        // wrongly-replaying subscriber would be caught.
        @Bean
        @ConditionalOnProperty(name = "test.history", havingValue = "true")
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        DefaultPositionSubscriber defaultPositionSubscriber() {
            return new DefaultPositionSubscriber();
        }

        @Bean
        NowPositionSubscriber nowPositionSubscriber() {
            return new NowPositionSubscriber();
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
            List<io.cloudevents.CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new TestEvent("historic")))
                    .stream()
                    .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                    .toList();
            dcbEventStore.append(cloudEvents);
        }
    }

    // --- subscribers ---

    static class DefaultPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @DcbSubscription(id = "dcb-sp-default")
        void onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-default")) {
                received.add(event);
            }
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

        @DcbSubscription(id = "dcb-sp-now", startAt = StartPosition.NOW)
        void onEvent(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-now")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
