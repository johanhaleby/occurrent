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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.Snapshot;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
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
import static org.awaitility.Awaitility.await;

/**
 * Proves the reactive {@code @Snapshot} registrar maintains a per-stream, resume-ready snapshot: each stream folds into
 * its own snapshot document keyed by the stream id, carrying the folded state, the stream version it reached, and the
 * declared schema version, backed by the zero-config reactive MongoDB snapshot store.
 */
@DisplayName("Reactive Snapshot annotation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSnapshotAnnotationMongoTest.SnapshotApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-snapshot-test"
        }
)
@Import(ReactiveSnapshotAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSnapshotAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-snapshot-test");
    private static final int SCHEMA_VERSION = 4;

    @Autowired
    private ApplicationService<CounterEvent> applicationService;

    @Autowired
    private ReactiveMongoOperations mongoOperations;

    @Test
    void maintains_a_resume_ready_snapshot_per_stream() {
        applicationService.execute("counter-1", __ -> List.of(new Incremented(1), new Incremented(2), new Incremented(3))).block();
        applicationService.execute("counter-2", __ -> List.of(new Incremented(10))).block();

        ReactiveSnapshotStore<Counter> store = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Counter.class, "occurrent-snapshot-reactive-counter");

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(store.findLatest("counter-1").blockOptional()).hasValueSatisfying(snapshot -> {
                assertThat(snapshot.state().total()).isEqualTo(6);
                assertThat(snapshot.version()).isEqualTo(3L);
                assertThat(snapshot.schemaVersion()).isEqualTo(SCHEMA_VERSION);
            });
            assertThat(store.findLatest("counter-2").blockOptional()).hasValueSatisfying(snapshot -> {
                assertThat(snapshot.state().total()).isEqualTo(10);
                assertThat(snapshot.version()).isEqualTo(1L);
            });
        });
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
            @Snapshot(id = "reactive-counter")
            SnapshotView<Counter, CounterEvent> counterSnapshot() {
                return SnapshotView.<Counter, CounterEvent>builder(new Counter(0))
                        .schemaVersion(SCHEMA_VERSION)
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
