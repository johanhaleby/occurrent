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
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@code @Projection(recordAppliedAppends = true)} end to end against a real MongoDB-backed
 * {@link AppliedAppendStore}
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>):
 * a live write is recorded and a caller can wait for it, and closing the application context stops the recording
 * poll's thread rather than leaking it. Booted with {@code SpringApplication.run} so the second test can close its
 * own context and inspect what threads survive it.
 */
@DisplayName("Projection annotation (recordAppliedAppends, MongoDB)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationRecordAppliedAppendsMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void records_a_live_write_so_a_caller_can_wait_for_it() {
        ConfigurableApplicationContext ctx = SpringApplication.run(RecordingProjectionApplication.class, bootArgs("record-applied-appends-happy-path"));
        try {
            EventStore eventStore = ctx.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = ctx.getBean(CloudEventConverter.class);
            AppliedAppendStore appliedAppendStore = ctx.getBean(AppliedAppendStore.class);

            WriteResult result = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("one"))));
            AppendId appendId = result.appendId().orElseThrow();

            assertThat(appliedAppendStore.waitUntilApplied("recording-counter", appendId, Duration.ofSeconds(20))).isTrue();
        } finally {
            ctx.close();
        }
    }

    @Test
    void closing_the_application_context_stops_the_recording_poll() {
        ConfigurableApplicationContext ctx = SpringApplication.run(RecordingProjectionApplication.class, bootArgs("record-applied-appends-close-stops-poll"));
        // Force the poll's scheduler to exist before closing, by driving one write through the projection first.
        EventStore eventStore = ctx.getBean(EventStore.class);
        CloudEventConverter<TestEvent> converter = ctx.getBean(CloudEventConverter.class);
        AppliedAppendStore appliedAppendStore = ctx.getBean(AppliedAppendStore.class);
        WriteResult result = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("one"))));
        appliedAppendStore.waitUntilApplied("recording-counter", result.appendId().orElseThrow(), Duration.ofSeconds(20));

        ctx.close();

        assertThat(countThreadsWithNamePrefix("occurrent-applied-append-poll")).isZero();
    }

    private static int countThreadsWithNamePrefix(String prefix) {
        return (int) Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith(prefix))
                .count();
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
    static class RecordingProjectionApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-test"))
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
        RecordingCounterProjection recordingCounterProjection() {
            return new RecordingCounterProjection();
        }
    }

    static class RecordingCounterProjection {
        @Projection(id = "recording-counter", recordAppliedAppends = true, storeName = "counterStore")
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
