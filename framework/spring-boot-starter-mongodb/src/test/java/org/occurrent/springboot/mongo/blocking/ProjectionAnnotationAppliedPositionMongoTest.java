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
import org.occurrent.dsl.projection.AppliedPositionStorage;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@code @Projection(recordAppliedPosition = true)} end to end on the blocking stack: the registrar wraps
 * the resolved store with the recorder, resolves the zero-config {@link MongoAppliedPositionStorage}, and a client
 * can wait for the projection to reach a position it already knows about
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>).
 * Docker-based, run by the CI/integration step.
 */
@DisplayName("Projection annotation (recordAppliedPosition)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationAppliedPositionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:applied-position-test");

    @Test
    void a_client_can_wait_until_the_projection_has_applied_a_position_it_already_holds() {
        try (ConfigurableApplicationContext context = SpringApplication.run(new Class<?>[]{RecordingApplication.class}, new String[]{"--spring.main.web-application-type=none"})) {
            EventStore eventStore = context.getBean(EventStore.class);
            CloudEventConverter<OrderPlaced> converter = context.getBean(CloudEventConverter.class);
            AppliedPositionStorage storage = context.getBean(AppliedPositionStorage.class);

            List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(new OrderPlaced()));
            eventStore.write("order-1", cloudEvents);
            long writtenPosition = ((PositionOrderedReader) eventStore).currentPosition();

            boolean caughtUp = storage.waitUntilApplied("orders", writtenPosition, Duration.ofSeconds(30));

            assertThat(caughtUp).isTrue();
            assertThat(storage.appliedPosition("orders").orElseThrow()).isGreaterThanOrEqualTo(writtenPosition);
        }
    }

    @Test
    void recordAppliedPosition_combined_with_mode_SYNCHRONOUS_fails_context_startup() {
        assertThatThrownBy(() -> SpringApplication.run(new Class<?>[]{SynchronousRecordingApplication.class}, new String[]{"--spring.main.web-application-type=none"}))
                .hasMessageContaining("SYNCHRONOUS")
                .hasMessageContaining("recordAppliedPosition");
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
        }
    }

    @SpringBootApplication
    @Import(MongoDbContainerConfiguration.class)
    @EnableOccurrent
    static class RecordingApplication {
        @Bean
        CloudEventTypeMapper<OrderPlaced> orderPlacedCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<OrderPlaced> orderPlacedCloudEventConverter(CloudEventTypeMapper<OrderPlaced> typeMapper) {
            return new JacksonCloudEventConverter.Builder<OrderPlaced>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(OrderPlaced::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        OrdersProjection ordersProjection() {
            return new OrdersProjection();
        }
    }

    @SpringBootApplication
    @Import(MongoDbContainerConfiguration.class)
    @EnableOccurrent
    static class SynchronousRecordingApplication {
        @Bean
        CloudEventTypeMapper<OrderPlaced> orderPlacedCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<OrderPlaced> orderPlacedCloudEventConverter(CloudEventTypeMapper<OrderPlaced> typeMapper) {
            return new JacksonCloudEventConverter.Builder<OrderPlaced>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(OrderPlaced::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        InvalidOrdersProjection invalidOrdersProjection() {
            return new InvalidOrdersProjection();
        }
    }

    static class OrdersProjection {
        @Projection(id = "orders", startAt = org.occurrent.annotation.StartPosition.BEGINNING, recordAppliedPosition = true)
        org.occurrent.dsl.projection.Projection<OrderCount, OrderPlaced, String> orders() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, OrderPlaced, String>builder(new OrderCount(0))
                    .id(event -> "orders")
                    .on(OrderPlaced.class, (state, event) -> new OrderCount(state.count() + 1))
                    .build();
        }
    }

    static class InvalidOrdersProjection {
        @Projection(id = "orders-sync", mode = org.occurrent.annotation.Mode.SYNCHRONOUS, recordAppliedPosition = true)
        org.occurrent.dsl.projection.Projection<OrderCount, OrderPlaced, String> orders() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, OrderPlaced, String>builder(new OrderCount(0))
                    .id(event -> "orders")
                    .on(OrderPlaced.class, (state, event) -> new OrderCount(state.count() + 1))
                    .build();
        }
    }

    record OrderCount(int count) {
    }

    record OrderPlaced(String eventId, Date timestamp) {
        OrderPlaced() {
            this(UUID.randomUUID().toString(), new Date());
        }
    }
}
