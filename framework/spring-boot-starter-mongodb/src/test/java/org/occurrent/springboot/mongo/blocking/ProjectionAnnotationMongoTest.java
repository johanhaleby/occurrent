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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
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
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a {@link Projection @Projection} factory bean is registered as a DCB read model that catches up from
 * history on startup and materializes into a caller-provided (non-Mongo) {@link ViewStateRepository}. Docker-based, run
 * by the CI/integration step.
 */
@DisplayName("Projection annotation (DCB, catch-up, custom store)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ProjectionAnnotationMongoTest.DcbProjectionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:projection-annotation-test"
        }
)
@Import(ProjectionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationMongoTest {

    static final String TAG = "counter:orders";
    private static final URI SOURCE = URI.create("urn:occurrent:projection-annotation-test");

    @Autowired
    private DcbEventStore dcbEventStore;
    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private OrderCountStore orderCountStore;

    @Test
    void catches_up_from_history_then_materializes_live_events_into_the_custom_store() {
        // Two OrderPlaced events were appended before startup by HistoryAppender, so the BEGINNING projection replays them.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(2));

        append(new OrderPlaced("live"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(3));
    }

    private void append(TestEvent... events) {
        appendTagged(dcbEventStore, cloudEventConverter, events);
    }

    private static void appendTagged(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events)).stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents);
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
    static class DcbProjectionApplication {
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

        // The read model store, an in-memory ViewStateRepository, proving a projection can materialize into a non-Mongo store.
        @Bean
        OrderCountStore orderCountStore() {
            return new OrderCountStore();
        }

        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        // The @Projection factory. DependsOn the appender so the history is in place before catch-up replays it.
        @Bean
        @DependsOn("historyAppender")
        OrderCountProjection orderCountProjection() {
            return new OrderCountProjection();
        }
    }

    static class OrderCountProjection {
        @Projection(id = "order-count", startAt = org.occurrent.annotation.StartPosition.BEGINNING, storeName = "orderCountStore")
        DcbProjection<OrderCount, TestEvent, String> orderCount() {
            var projection = org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount(0))
                    .id(event -> "orders")
                    .on(OrderPlaced.class, (state, event) -> new OrderCount(state.count() + 1))
                    .build();
            return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse(TAG)));
        }
    }

    // In-memory ViewStateRepository, backing the projection's read model without MongoDB.
    static class OrderCountStore implements ViewStateRepository<OrderCount, String> {
        private final ConcurrentHashMap<String, OrderCount> store = new ConcurrentHashMap<>();

        @Override
        public java.util.Optional<OrderCount> findById(String id) {
            return java.util.Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, OrderCount state) {
            store.put(id, state);
        }

        int countFor(String id) {
            OrderCount current = store.get(id);
            return current == null ? 0 : current.count();
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
            appendTagged(dcbEventStore, cloudEventConverter, new OrderPlaced("historic-1"), new OrderPlaced("historic-2"));
        }
    }

    record OrderCount(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record OrderPlaced(String eventId, Date timestamp, String name) implements TestEvent {
        OrderPlaced(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
