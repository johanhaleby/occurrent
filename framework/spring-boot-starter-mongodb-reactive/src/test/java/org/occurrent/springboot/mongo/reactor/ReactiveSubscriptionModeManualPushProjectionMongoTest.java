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

package org.occurrent.springboot.mongo.reactor;

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
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.springboot.reactor.ManualStartPushSources;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
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
import reactor.core.publisher.Flux;
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
 * A reactive {@code @Projection(source = PUSH)} is fed by a {@link PushSubscriptionModel} bean the application
 * supplies, so stopping Occurrent's own subscription model never reaches it. Proves that
 * {@code occurrent.subscription.mode=manual} withholds its catch-up anyway, and that
 * {@link ManualStartPushSources#startAll()} runs it.
 */
@DisplayName("Reactive subscription mode manual (push projection)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSubscriptionModeManualPushProjectionMongoTest.ManualPushProjectionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.subscription.mode=manual",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-manual-push-projection-test"
        }
)
@Import(ReactiveSubscriptionModeManualPushProjectionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSubscriptionModeManualPushProjectionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-manual-push-projection-test");
    private static final String PROJECTION_ID = "reactive-manual-push-order-count";
    private static final String VIEW_ID = "orders";

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private PushSubscriptionModel pushModel;
    @Autowired
    private ViewStateRepository<OrderCount, String> orderCountStore;
    @Autowired
    private ManualStartPushSources manualStartProjections;

    @Test
    void a_push_projection_stays_withheld_until_the_application_starts_it() {
        assertThat(manualStartProjections.pendingIds()).containsExactly(PROJECTION_ID);
        // Two OrderPlaced events were written before startup and nothing caught them up, so there is no read model.
        await().during(ofSeconds(2)).atMost(ofSeconds(10)).until(() -> orderCountStore.findById(VIEW_ID).isEmpty());

        assertThat(manualStartProjections.startAll().block(ofSeconds(30))).containsExactly(PROJECTION_ID);

        assertThat(manualStartProjections.pendingIds()).isEmpty();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(2)));

        // It is live now, so an event pushed through the model afterwards is materialized too.
        pushModel.accept(cloudEventConverter.toCloudEvent(new OrderPlaced("live"))).block();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(3)));
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
    @EnableOccurrentReactive
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
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        PushSubscriptionModel pushModel() {
            return new PushSubscriptionModel();
        }

        @Bean
        ViewStateRepository<OrderCount, String> orderCountStore() {
            ConcurrentHashMap<String, OrderCount> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
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

    @Component
    static class OrderCountProjection {
        @Projection(id = PROJECTION_ID, source = Source.PUSH, subscriptionModelName = "pushModel", storeName = "orderCountStore")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> orderCount() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount(0))
                    .id(event -> VIEW_ID)
                    .on(OrderPlaced.class, (state, event) -> new OrderCount(state.count() + 1))
                    .build();
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
            eventStore.write("orders", Flux.fromIterable(
                    cloudEventConverter.toCloudEvents(List.of(new OrderPlaced("historic-1"), new OrderPlaced("historic-2"))))).block();
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
