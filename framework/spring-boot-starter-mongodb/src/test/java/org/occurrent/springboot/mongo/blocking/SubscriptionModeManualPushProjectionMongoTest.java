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
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.springboot.blocking.ManualStartPushSources;
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
 * Proves {@code occurrent.subscription.mode=manual} also withholds a {@link Projection @Projection} with
 * {@link Source#PUSH}, which bypasses the {@code SubscriptionModel} bean entirely and so is not withheld by that
 * bean being wrapped. Nothing runs the catch-up replay at boot, and {@link ManualStartPushSources#start(String)} is
 * what runs it, after which the projection also takes live events normally.
 */
@DisplayName("Subscription mode manual (push-source projection)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SubscriptionModeManualPushProjectionMongoTest.ManualPushProjectionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.subscription.mode=manual",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:manual-mode-push-projection-test"
        }
)
@Import(SubscriptionModeManualPushProjectionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class SubscriptionModeManualPushProjectionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:manual-mode-push-projection-test");
    private static final String PROJECTION_ID = "manual-push-order-count";

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private PushSubscriptionModel pushModel;
    @Autowired
    private OrderCountStore orderCountStore;
    @Autowired
    private ManualStartPushSources manualStartProjections;

    @Test
    void a_push_source_projection_runs_no_replay_until_started_then_catches_up_and_goes_live() {
        // Two OrderPlaced events were written before startup, so a running catch-up would already have counted them.
        assertThat(manualStartProjections.pendingIds()).contains(PROJECTION_ID);
        await().during(ofSeconds(2)).atMost(ofSeconds(10)).until(() -> orderCountStore.countFor("orders") == 0);

        manualStartProjections.start(PROJECTION_ID);

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(2));
        assertThat(manualStartProjections.pendingIds()).doesNotContain(PROJECTION_ID);

        pushModel.accept(cloudEventConverter.toCloudEvent(new OrderPlaced("live")));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.countFor("orders")).isEqualTo(3));

        // Starting it again is a no-op, not a second replay-then-push run.
        manualStartProjections.start(PROJECTION_ID);
        await().during(ofSeconds(2)).atMost(ofSeconds(10)).until(() -> orderCountStore.countFor("orders") == 3);
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
    static class ManualPushProjectionApplication {
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

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
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
        @Projection(id = PROJECTION_ID, source = Source.PUSH, subscriptionModelName = "pushModel", storeName = "orderCountStore")
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
