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
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
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
 * Domain-push variant where the {@link Projection @Projection}'s store is a {@link MaterializedView} rather than a
 * {@link ViewStateRepository}. This exercises the reactive bean-post-processor's MaterializedView branch: the view is
 * driven with a reactive fold while the projection supplies only the replay filter. Docker-based, run by the
 * CI/integration step.
 */
@DisplayName("Reactive Projection annotation (domain-push source into a MaterializedView, catch-up then live)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveProjectionAnnotationDomainPushViaMaterializedViewMongoTest.DomainPushMaterializedViewApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-domain-push-materializedview-annotation-test"
        }
)
@Import(ReactiveProjectionAnnotationDomainPushViaMaterializedViewMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveProjectionAnnotationDomainPushViaMaterializedViewMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-domain-push-materializedview-annotation-test");
    private static final String VIEW_ID = "orders";

    @Autowired
    private EventStore eventStore;
    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;
    @Autowired
    private DomainEventFeed<TestEvent> ordersFeed;
    @Autowired
    private ViewStateRepository<OrderCount, String> orderCountStore;

    @Test
    void catches_up_history_then_materializes_live_domain_events_through_a_MaterializedView_store() {
        // Two OrderPlaced events were written before startup, so the catch-up folds them through the MaterializedView.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(2)));

        ordersFeed.accept(new OrderPlaced("live")).block();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(orderCountStore.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(3)));
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
    @EnableOccurrentReactive
    static class DomainPushMaterializedViewApplication {
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
        DomainEventFeed<TestEvent> ordersFeed(PositionOrderedReader reader, CloudEventConverter<TestEvent> converter, CheckpointStorage checkpointStorage) {
            return new DomainEventFeed<>(reader, converter, TestEvent::eventId, checkpointStorage);
        }

        // The read model the assertions inspect. The MaterializedView below folds into it.
        @Bean
        ViewStateRepository<OrderCount, String> orderCountStore() {
            ConcurrentHashMap<String, OrderCount> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        // The @Projection's store is this MaterializedView (not the repository directly), so the catch-up drives it with
        // a reactive fold and uses the projection only for the replay filter.
        @Bean
        MaterializedView<TestEvent> orderCountView(ViewStateRepository<OrderCount, String> orderCountStore) {
            return event -> {
                if (event instanceof OrderPlaced) {
                    OrderCount current = orderCountStore.findById(VIEW_ID).orElse(new OrderCount(0));
                    orderCountStore.save(VIEW_ID, new OrderCount(current.count() + 1));
                }
            };
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
        // storeName points at the MaterializedView bean; the projection's fold is unused on this path (the view folds),
        // it only declares which events to replay.
        @Projection(id = "reactive-domain-push-mv-order-count", source = Source.PUSH, subscriptionModelName = "ordersFeed", storeName = "orderCountView")
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
