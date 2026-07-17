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
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Component;
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
 * Proves the blocking {@code @Snapshot} registrar maintains a per-stream, resume-ready snapshot: each stream folds into
 * its own {@code Snapshot} document keyed by the stream id, carrying the folded state, the stream version it reached, and
 * the declared schema version. Also proves a snapshot id must be unique across subscriptions and snapshots.
 */
@DisplayName("Snapshot annotation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class SnapshotAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:snapshot-annotation-test");
    private static final int SCHEMA_VERSION = 3;

    private static ConfigurableApplicationContext run(Class<?> application, String databaseName) {
        return SpringApplication.run(new Class<?>[]{application, MongoContainerConfiguration.class}, new String[]{
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        });
    }

    @Test
    void maintains_a_resume_ready_snapshot_per_stream() {
        try (ConfigurableApplicationContext context = run(SnapshotApplication.class, "snapshot-maintained")) {
            @SuppressWarnings("unchecked")
            CloudEventConverter<CounterEvent> converter = context.getBean(CloudEventConverter.class);
            EventStore eventStore = context.getBean(EventStore.class);
            MongoOperations mongoOperations = context.getBean(MongoOperations.class);

            eventStore.write("counter-1", converter.toCloudEvents(List.of(new Incremented(1), new Incremented(2), new Incremented(3))));
            eventStore.write("counter-2", converter.toCloudEvents(List.of(new Incremented(10))));

            SnapshotStore<Counter> store = new SpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-counter");

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
                assertThat(store.findLatest("counter-1")).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(6);
                    assertThat(snapshot.version()).isEqualTo(3L);
                    assertThat(snapshot.schemaVersion()).isEqualTo(SCHEMA_VERSION);
                });
                assertThat(store.findLatest("counter-2")).hasValueSatisfying(snapshot -> {
                    assertThat(snapshot.state().total()).isEqualTo(10);
                    assertThat(snapshot.version()).isEqualTo(1L);
                });
            });
        }
    }

    @Test
    void rejects_a_snapshot_id_already_used_by_a_subscription() {
        assertThatThrownBy(() -> run(DuplicateIdApplication.class, "snapshot-duplicate-id"))
                .hasMessageContaining("Duplicate")
                .hasMessageContaining("counter");
    }

    @Configuration(proxyBeanMethods = false)
    static class MongoContainerConfiguration {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SnapshotApplication {
        @Bean
        CloudEventTypeMapper<CounterEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<CounterEvent> converter(CloudEventTypeMapper<CounterEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<CounterEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(CounterEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Component
        static class CounterSnapshot {
            @Snapshot(id = "counter")
            SnapshotView<Counter, CounterEvent> counterSnapshot() {
                return SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                        .schemaVersion(SCHEMA_VERSION)
                        .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                        .build();
            }
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class DuplicateIdApplication {
        @Bean
        CloudEventTypeMapper<CounterEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<CounterEvent> converter(CloudEventTypeMapper<CounterEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<CounterEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(CounterEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Component
        static class Clashing {
            @Subscription(id = "counter", eventTypes = Incremented.class)
            void onIncremented(CounterEvent event) {
            }

            @Snapshot(id = "counter")
            SnapshotView<Counter, CounterEvent> counterSnapshot() {
                return SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                        .on(Incremented.class, (state, event) -> new Counter(state.total() + event.amount()))
                        .build();
            }
        }
    }

    record Counter(int total) {
    }

    sealed interface CounterEvent {
        String eventId();

        Date timestamp();

        int amount();
    }

    record Incremented(String eventId, Date timestamp, int amount) implements CounterEvent {
        Incremented(int amount) {
            this(UUID.randomUUID().toString(), new Date(), amount);
        }
    }
}
