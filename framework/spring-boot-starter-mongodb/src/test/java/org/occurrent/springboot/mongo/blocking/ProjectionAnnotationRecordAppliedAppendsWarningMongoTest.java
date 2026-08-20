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
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The end-to-end pairing this falsifier suite's own withheld post-reset tests depend on, from the real shipped
 * Spring Boot Mongo composition rather than {@code ProjectionAnnotationRecordAppliedAppendsWarningTest}'s
 * mocked {@code Subscribable} (issue #865 / PR #869): the default start position warns naming the projection, and
 * {@code startAt = StartPosition.BEGINNING}, the configuration
 * {@code ProjectionAnnotationRecordAppliedAppendsResetMongoTest} actually uses, does not. Not a re-test of the
 * wiring's truth table, that unit test module already covers every branch; this only pins that the real starter
 * reaches the same two outcomes.
 */
@DisplayName("Projection annotation (recordAppliedAppends, MongoDB, never-replays warning)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationRecordAppliedAppendsWarningMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // A manually attached logback appender does not survive SpringApplication.run(): Spring Boot's own logging
    // system reinitializes logback on every call (LoggingApplicationListener reacting to context-preparation
    // events), detaching whatever was attached beforehand, including an appender attached moments earlier in the
    // same test. Capturing System.out around the call sidesteps that entirely, since the console appender Spring
    // Boot (re)creates during this exact call binds to whatever System.out already is at that point.
    private String bootConsoleOutput(Class<?> applicationClass, String databaseName) {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try (ConfigurableApplicationContext ignored = SpringApplication.run(applicationClass, bootArgs(databaseName))) {
            return captured.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void the_default_start_position_warns_naming_the_projection_on_the_real_shipped_composition() {
        String output = bootConsoleOutput(DefaultStartApplication.class, "record-applied-appends-warning-default");

        assertThat(output)
                .contains("WARN")
                .contains("warning-default-counter")
                .contains("recordAppliedAppends = true")
                .contains("never replays");
    }

    @Test
    void start_at_beginning_does_not_warn_on_the_real_shipped_composition_because_a_rebuild_actually_replays() {
        String output = bootConsoleOutput(BeginningStartApplication.class, "record-applied-appends-warning-beginning");

        assertThat(output).doesNotContain("never replays");
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DefaultStartApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-warning-test"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        CounterStore counterStore() {
            return new CounterStore();
        }

        @Bean
        DefaultStartProjection defaultStartProjection() {
            return new DefaultStartProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class BeginningStartApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-warning-test"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        CounterStore counterStore() {
            return new CounterStore();
        }

        @Bean
        BeginningStartProjection beginningStartProjection() {
            return new BeginningStartProjection();
        }
    }

    static class DefaultStartProjection {
        @Projection(id = "warning-default-counter", recordAppliedAppends = true, storeName = "counterStore")
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> new Counter(state.count() + 1))
                    .build();
        }
    }

    static class BeginningStartProjection {
        @Projection(id = "warning-beginning-counter", recordAppliedAppends = true, storeName = "counterStore", startAt = StartPosition.BEGINNING)
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> new Counter(state.count() + 1))
                    .build();
        }
    }

    static class CounterStore implements ViewStateRepository<Counter, String> {
        private final ConcurrentHashMap<String, Counter> store = new ConcurrentHashMap<>();

        @Override
        public Optional<Counter> findById(String id) {
            return Optional.ofNullable(store.get(id));
        }

        @Override
        public void save(String id, Counter state) {
            store.put(id, state);
        }
    }

    record Counter(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Counted(String eventId, Date timestamp, String name) implements TestEvent {
        Counted(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
