/*
 *
 *  Copyright 2024 Johan Haleby
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
import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
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
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that a {@code @Projection} factory method is registered reactively in DCB-only mode: it replays the DCB history
 * appended before startup, then folds live events into a store-agnostic in-memory read model (a {@link ViewStateRepository}).
 * The reactive counterpart of the blocking projection annotation test.
 */
@DisplayName("Reactive Projection annotation (DCB, async catch-up)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveProjectionAnnotationMongoTest.DcbOnlyApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-projection-annotation-test"
        }
)
@Import(ReactiveProjectionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveProjectionAnnotationMongoTest {

    static final String TAG = "dashboard:reactive-projection";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-projection-annotation-test");
    private static final String VIEW_ID = "dashboard";

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private ViewStateRepository<CountState, String> dashboardStore;

    @Test
    void replays_history_then_folds_live_events_into_the_read_model() {
        // Two Included events were appended before startup by HistoryAppender, so the BEGINNING projection replays them.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(dashboardStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(2)));

        // A live Included event is folded after the catch-up phase.
        append(new Included("live-1"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(dashboardStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(3)));

        // An Excluded event has no handler, so the fold leaves the count unchanged.
        append(new Excluded("excluded-1"));
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                assertThat(dashboardStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(3)));
    }

    private void append(TestEvent... events) {
        appendTagged(dcbEventStore, cloudEventConverter, events);
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents).block();
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
    @EnableOccurrentReactive
    static class DcbOnlyApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new tools.jackson.databind.ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // The read-model store. Being the only ViewStateRepository bean, the @Projection resolves to it by type.
        @Bean
        ViewStateRepository<CountState, String> dashboardStore() {
            ConcurrentHashMap<String, CountState> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        DashboardProjection dashboardProjection() {
            return new DashboardProjection();
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

    // Holds the @Projection factory method. The reactive registrar invokes it after the context is built.
    @Component
    static class DashboardProjection {

        @org.occurrent.annotation.Projection(id = "reactive-projection-dashboard", startAt = org.occurrent.annotation.StartPosition.BEGINNING)
        DcbProjection<CountState, TestEvent, String> countIncludedEvents() {
            Projection<CountState, TestEvent, String> projection = Projection.<CountState, TestEvent, String>builder(new CountState(0))
                    .id(event -> VIEW_ID)
                    .on(Included.class, (state, event) -> new CountState(state.count() + 1))
                    .build();
            return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse(TAG)));
        }
    }

    record CountState(int count) {
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
