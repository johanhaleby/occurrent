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
 * The cross-instance half of {@code ProjectionAnnotationRecordAppliedAppendsMongoTest}
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 5): "the membership record is store-backed, not in-process... a wait can run on a different node than
 * the projection that did the recording." Two separate application contexts against the same MongoDB collection:
 * instance A runs the recording projection and does the write, instance B runs no projection at all and only calls
 * {@code waitUntilApplied} through its own, independently constructed {@link AppliedAppendStore} bean. If the
 * answer only lived in instance A's process, B's wait would time out no matter how long it waited; instead it must
 * see A's record because both stores are backed by the same Mongo collection.
 */
@DisplayName("Projection annotation (recordAppliedAppends, MongoDB, cross-instance)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationRecordAppliedAppendsCrossInstanceMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void a_wait_on_one_instance_observes_an_append_recorded_by_the_recording_projection_running_on_another() {
        String databaseName = "record-applied-appends-cross-instance";
        // try-with-resources rather than a single try/finally with two manual close() calls: if instance B's boot
        // throws, execution never reaches a finally guarding both, and instance A would leak its Mongo client and
        // subscription threads. Each resource here closes independently of whether an earlier one failed to open.
        try (ConfigurableApplicationContext instanceA = SpringApplication.run(RecordingProjectionApplication.class, bootArgs(databaseName, "instance-a"));
             ConfigurableApplicationContext instanceB = SpringApplication.run(ReaderOnlyApplication.class, bootArgs(databaseName, "instance-b"))) {
            EventStore eventStore = instanceA.getBean(EventStore.class);
            CloudEventConverter<TestEvent> converter = instanceA.getBean(CloudEventConverter.class);
            AppliedAppendStore storeOnA = instanceA.getBean(AppliedAppendStore.class);
            AppliedAppendStore storeOnB = instanceB.getBean(AppliedAppendStore.class);

            WriteResult result = eventStore.write(UUID.randomUUID().toString(), converter.toCloudEvents(List.of(new Counted("one"))));
            AppendId appendId = result.appendId().orElseThrow();

            // Instance A recorded it (sanity: the recording projection is actually running there).
            assertThat(storeOnA.waitUntilApplied("recording-counter", appendId, Duration.ofSeconds(20))).isTrue();

            // Instance B never ran the projection and never called recordApplied itself; its wait can only succeed
            // by reading the same underlying Mongo collection A's recorder wrote to.
            assertThat(storeOnB.waitUntilApplied("recording-counter", appendId, Duration.ofSeconds(20)))
                    .as("the membership record is store-backed, so a wait on a different node/process sees it too")
                    .isTrue();
        }
    }

    private static String[] bootArgs(String databaseName, String applicationName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--spring.application.name=" + applicationName,
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
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:record-applied-appends-cross-instance-test"))
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

    /**
     * Boots the Mongo starter's auto-configuration (and, with it, the zero-config {@link AppliedAppendStore} bean)
     * with no projection at all, so this instance never records anything itself.
     */
    @SpringBootApplication
    @EnableOccurrent
    static class ReaderOnlyApplication {
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
