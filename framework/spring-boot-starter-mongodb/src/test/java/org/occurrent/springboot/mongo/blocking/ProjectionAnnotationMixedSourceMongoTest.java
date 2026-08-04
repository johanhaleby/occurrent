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
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that a push-fed {@link Projection @Projection} ({@link Source#PUSH}) and an event-store-fed one
 * ({@link Source#EVENT_STORE}, the default) coexist in the same application context and stay independent: the push
 * projection is driven only by events handed to its {@link PushSubscriptionModel}, the event-store projection only by
 * the change stream, and a delivery to one never leaks into the other. Docker-based, run by the CI/integration step.
 */
@DisplayName("Projection annotation (push and event-store sources side by side)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ProjectionAnnotationMixedSourceMongoTest.MixedSourceApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:mixed-source-projection-annotation-test"
        }
)
@Import(ProjectionAnnotationMixedSourceMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(90)
class ProjectionAnnotationMixedSourceMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:mixed-source-projection-annotation-test");
    private static final String STREAM = "orders";

    @Autowired
    private EventStore eventStore;
    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private PushSubscriptionModel pushModel;
    @Autowired
    private OrderCountStore pushCountStore;
    @Autowired
    private OrderCountStore eventStoreCountStore;

    @Test
    void push_and_event_store_projections_run_side_by_side_and_stay_independent() {
        // Two OrderPlaced events were written before startup. Both projections catch them up: the push projection by
        // replaying the store during its catch-up, the event-store projection by replaying the change stream from the
        // beginning.
        await().atMost(ofSeconds(45)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(pushCountStore.countFor(STREAM)).isEqualTo(2);
            assertThat(eventStoreCountStore.countFor(STREAM)).isEqualTo(2);
        });

        // An event pushed through the model (never written to the store) reaches only the push projection.
        pushModel.accept(cloudEventConverter.toCloudEvent(new OrderPlaced("pushed-live")));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(pushCountStore.countFor(STREAM)).isEqualTo(3));
        assertThat(eventStoreCountStore.countFor(STREAM)).isEqualTo(2);

        // An event written to the store (never pushed) reaches only the event-store projection, over the change stream.
        eventStore.write(STREAM, cloudEventConverter.toCloudEvents(List.of(new OrderPlaced("store-live"))));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(eventStoreCountStore.countFor(STREAM)).isEqualTo(3));
        assertThat(pushCountStore.countFor(STREAM)).isEqualTo(3);
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
    static class MixedSourceApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // The external push feed a broker listener would drive. Only the source = PUSH projection binds to it.
        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        OrderCountStore pushCountStore() {
            return new OrderCountStore();
        }

        @Bean
        OrderCountStore eventStoreCountStore() {
            return new OrderCountStore();
        }

        @Bean
        HistoryAppender historyAppender(EventStore eventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(eventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        MixedProjections mixedProjections() {
            return new MixedProjections();
        }
    }

    static class MixedProjections {
        @Projection(id = "mixed-push-order-count", source = Source.PUSH, subscriptionModelName = "pushModel", storeName = "pushCountStore")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> pushOrderCount() {
            return counter();
        }

        @Projection(id = "mixed-store-order-count", startAt = StartPosition.BEGINNING, storeName = "eventStoreCountStore")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> eventStoreOrderCount() {
            return counter();
        }

        private static org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount(0))
                    .id(event -> STREAM)
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
            eventStore.write(STREAM, cloudEventConverter.toCloudEvents(List.of(new OrderPlaced("historic-1"), new OrderPlaced("historic-2"))));
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
