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

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Source;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a {@link Projection @Projection} with {@link Source#PUSH} is fed by a {@link DomainEventFeed}:
 * it bootstraps its history from the event store on startup, then materializes live domain events fed through the feed,
 * with no CloudEvent conversion on the live path. Docker-based, run by the CI/integration step.
 */
@DisplayName("Projection annotation (domain-push source, bootstrap then live)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ProjectionAnnotationDomainPushSourceMongoTest.DomainPushProjectionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:domain-push-projection-annotation-test"
        }
)
@Import(ProjectionAnnotationDomainPushSourceMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationDomainPushSourceMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:domain-push-projection-annotation-test");

    @Autowired
    private EventStore eventStore;
    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private DomainEventFeed<TestEvent> ordersFeed;
    @Autowired
    private OrderCountStore orderCountStore;

    @Test
    void bootstraps_history_from_the_event_store_then_materializes_pushed_live_domain_events() {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(2));

        // A live domain event is fed directly (no CloudEvent), as an application listener would after decoding it.
        ordersFeed.accept(new OrderPlaced("live"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(3));
    }

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
    static class DomainPushProjectionApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
                    .build();
        }

        // The application-owned domain feed. It carries the domain-specific eventId; the framework's event store and
        // checkpoint storage supply the CloudEvent-layer bits for bootstrap.
        @Bean
        DomainEventFeed<TestEvent> ordersFeed(PositionOrderedReader reader, CloudEventConverter<TestEvent> converter, CheckpointStorage checkpointStorage) {
            return new DomainEventFeed<>(reader, converter, TestEvent::eventId, checkpointStorage);
        }

        @Bean
        OrderCountStore orderCountStore() {
            return new OrderCountStore();
        }

        @Bean
        HistoryAppender historyAppender(EventStore eventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(eventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        OrderCountProjection orderCountProjection() {
            return new OrderCountProjection();
        }
    }

    static class OrderCountProjection {
        @Projection(id = "domain-push-order-count", source = Source.PUSH, subscriptionModelName = "ordersFeed", storeName = "orderCountStore")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> orderCount() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount(0))
                    .id(event -> "orders")
                    .on(OrderPlaced.class, (state, event) -> new OrderCount(state.count() + 1))
                    .build();
        }
    }

    static class OrderCountStore implements ViewStateRepository<OrderCount, String> {
        private final ConcurrentHashMap<String, OrderCount> store = new ConcurrentHashMap<>();

        @Override
        public Optional<OrderCount> findById(String id) {
            return Optional.ofNullable(store.get(id));
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
        private final EventStore eventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        HistoryAppender(EventStore eventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.eventStore = eventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendHistory() {
            eventStore.write("orders", cloudEventConverter.toCloudEvents(List.of(new OrderPlaced("historic-1"), new OrderPlaced("historic-2"))));
        }
    }

    record OrderCount(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        String name();
    }

    record OrderPlaced(String eventId, String name) implements TestEvent {
        OrderPlaced(String name) {
            this(UUID.randomUUID().toString(), name);
        }
    }
}
