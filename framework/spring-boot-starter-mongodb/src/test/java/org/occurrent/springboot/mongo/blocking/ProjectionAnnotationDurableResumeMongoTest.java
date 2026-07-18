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
import org.occurrent.application.service.blocking.ApplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the durable-resume claim across a full restart: a fresh context resumes an async {@link Projection
 * @Projection} from its stored checkpoint and processes only the events that arrived while it was down, not the whole
 * history again. Also exercises the agnostic {@code Projection} dispatch branch and the duplicate-id guard. Booted with
 * {@code SpringApplication.run} (not {@code @SpringBootTest}) so a second boot reprocesses the annotation like a real
 * restart.
 */
@DisplayName("Projection annotation (durable resume across restart)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ProjectionAnnotationDurableResumeMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        mongoDBContainer.withReuse(true);
        mongoDBContainer.setPortBindings(List.of("27017:27017"));
    }

    @Test
    void resumes_from_the_stored_checkpoint_and_processes_only_events_that_arrived_while_down() {
        String[] args = bootArgs("projection-durable-resume");

        ConfigurableApplicationContext ctx1 = SpringApplication.run(FirstBootApplication.class, args);
        try {
            MongoOperations mongo = ctx1.getBean(MongoOperations.class);
            // First boot: the BEGINNING projection replays the two historic events, so the Mongo-backed count reaches 2.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(count(mongo)).isEqualTo(2));
        } finally {
            ctx1.close();
        }

        ConfigurableApplicationContext ctx2 = SpringApplication.run(SecondBootApplication.class, args);
        try {
            MongoOperations mongo = ctx2.getBean(MongoOperations.class);
            // Second boot: resumeBehavior DEFAULT resumes from the durable checkpoint, so the fresh projection processes
            // only the one event appended while the application was down. The count goes from the persisted 2 to 3. A
            // wrong full replay would re-apply the two historic events and reach 5.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(count(mongo)).isEqualTo(3));
            await().during(ofSeconds(2)).atMost(ofSeconds(6)).untilAsserted(() ->
                    assertThat(count(mongo)).isEqualTo(3));
        } finally {
            ctx2.close();
        }
    }

    @Test
    void two_projections_with_the_same_id_fail_context_startup() {
        String[] args = bootArgs("projection-duplicate-id");
        assertThatThrownBy(() -> SpringApplication.run(DuplicateIdApplication.class, args).close())
                .hasStackTraceContaining("Duplicate subscription/projection id");
    }

    private static int count(MongoOperations mongo) {
        OrderCount current = mongo.findById("orders", OrderCount.class);
        return current == null ? 0 : current.count();
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.data.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:projection-durable-resume"))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    @SpringBootApplication
    @EnableOccurrent
    static class FirstBootApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService) {
            return new HistoryAppender(applicationService);
        }

        @Bean
        @DependsOn("historyAppender")
        OrderCountProjection orderCountProjection() {
            return new OrderCountProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SecondBootApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        OfflineAppender offlineAppender(ApplicationService<TestEvent> applicationService) {
            return new OfflineAppender(applicationService);
        }

        @Bean
        @DependsOn("offlineAppender")
        OrderCountProjection orderCountProjection() {
            return new OrderCountProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DuplicateIdApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        DuplicateIdProjections duplicateIdProjections() {
            return new DuplicateIdProjections();
        }
    }

    // The agnostic Projection dispatch branch (not DcbProjection), with the Mongo-default store.
    static class OrderCountProjection {
        @Projection(id = "order-count", capability = org.occurrent.annotation.Capability.AGNOSTIC, startAt = org.occurrent.annotation.StartPosition.BEGINNING, resumeBehavior = org.occurrent.annotation.ResumeBehavior.DEFAULT)
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> orderCount() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount("orders", 0))
                    .id(event -> "orders")
                    .on(OrderPlaced.class, (state, event) -> new OrderCount("orders", state.count() + 1))
                    .build();
        }
    }

    static class DuplicateIdProjections {
        @Projection(id = "same-id")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> first() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount("a", 0))
                    .id(event -> "a").on(OrderPlaced.class, (s, e) -> s).build();
        }

        @Projection(id = "same-id")
        org.occurrent.dsl.projection.Projection<OrderCount, TestEvent, String> second() {
            return org.occurrent.dsl.projection.Projection.<OrderCount, TestEvent, String>builder(new OrderCount("b", 0))
                    .id(event -> "b").on(OrderPlaced.class, (s, e) -> s).build();
        }
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;

        HistoryAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @jakarta.annotation.PostConstruct
        void appendHistory() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new OrderPlaced("historic-1")));
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new OrderPlaced("historic-2")));
        }
    }

    static class OfflineAppender {
        private final ApplicationService<TestEvent> applicationService;

        OfflineAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @jakarta.annotation.PostConstruct
        void appendWhileDown() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new OrderPlaced("while-down-1")));
        }
    }

    record OrderCount(@Id String id, int count) {
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
