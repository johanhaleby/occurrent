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

package org.occurrent.springboot.mongo.blocking;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReads;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * With the stream and the DCB capability both enabled, a {@code @Primary} {@code DcbEventStore} of the application's
 * own that delegates to the MongoDB store is not an {@code EventStoreQueries}, so it cannot report where the present
 * is. A DCB subscription at {@code NOW} must still take its start from the MongoDB store that the subscription model
 * reads, so a DCB append made through the delegating store while the application starts is delivered to it.
 */
@DisplayName("A DCB subscription at NOW with a delegating primary DcbEventStore beside the MongoDB store")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class DelegatingDcbEventStoreStartBoundaryMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:delegating-dcb-start-boundary");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_dcb_append_through_the_delegating_store_from_a_post_construct_method_is_delivered_to_a_now_subscription() {
        try (ConfigurableApplicationContext context = SpringApplication.run(DelegatingStoreApplication.class,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("delegating-dcb-start-boundary"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream,dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE)) {
            NowSubscriber subscriber = context.getBean(NowSubscriber.class);

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(subscriber.received()).extracting(TestEvent::name).containsExactly("appended-at-startup"));
            assertThat(context.getBean(DelegatingDcbEventStore.class).appends).as("appends made through the delegating store").isEqualTo(1);
        }
    }

    // DcbCatchupSubscriptionModel refuses a DcbEventStore that doesn't implement DcbCatchupReads
    static class DelegatingDcbEventStore implements DcbEventStore, DcbCatchupReads {
        private final SpringMongoEventStore delegate;
        volatile int appends;

        DelegatingDcbEventStore(SpringMongoEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            return delegate.read(criteria, options);
        }

        @Override
        public DcbCatchupReadScope openDcbCatchupReadScope(@Nullable String notBefore) {
            return delegate.openDcbCatchupReadScope(notBefore);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            appends++;
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            appends++;
            return delegate.append(events, condition);
        }
    }

    static class StartupAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> converter;

        StartupAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            this.dcbEventStore = dcbEventStore;
            this.converter = converter;
        }

        @PostConstruct
        void append() {
            List<CloudEvent> events = converter.toCloudEvents(List.of(new TestEvent("appended-at-startup"))).stream()
                    .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse("test:delegating-store"))))
                    .toList();
            dcbEventStore.append(events);
        }
    }

    static class NowSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "delegatingStoreNow", startAt = StartPosition.NOW)
        void on(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrent
    static class DelegatingStoreApplication {

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
        @Primary
        DelegatingDcbEventStore delegatingDcbEventStore(SpringMongoEventStore mongoEventStore) {
            return new DelegatingDcbEventStore(mongoEventStore);
        }

        @Bean
        StartupAppender startupAppender(DelegatingDcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter) {
            return new StartupAppender(dcbEventStore, converter);
        }

        @Bean
        NowSubscriber nowSubscriber() {
            return new NowSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
