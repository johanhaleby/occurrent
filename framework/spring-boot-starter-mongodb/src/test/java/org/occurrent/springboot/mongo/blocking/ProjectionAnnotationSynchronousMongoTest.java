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
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies a synchronous (read-your-writes) {@link Projection @Projection}: the read model is updated on the write path
 * inside {@code execute(...)}, and a throwing fold rolls the event write back when a transaction executor spans both.
 */
@DisplayName("Projection annotation (synchronous, read-your-writes)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ProjectionAnnotationSynchronousMongoTest.SynchronousProjectionApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:projection-synchronous-test"
        }
)
@Import(ProjectionAnnotationSynchronousMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ProjectionAnnotationSynchronousMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:projection-synchronous-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;
    @Autowired
    private EventStore eventStore;
    @Autowired
    private CounterStore counterStore;

    @Test
    void updates_the_read_model_synchronously_before_execute_returns() {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, __ -> List.of(new Counted("one"), new Counted("two")));
        // No await: a synchronous projection is materialized on the write thread before execute returns.
        assertThat(counterStore.countFor("counter")).isEqualTo(2);
        assertThat(eventStore.read(streamId).events().count()).isEqualTo(2);
    }

    @Test
    void a_throwing_fold_rolls_the_event_write_back() {
        String streamId = UUID.randomUUID().toString();
        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new Boom("boom"))))
                .hasStackTraceContaining("boom");
        // The transaction executor spans the write and the synchronous dispatch, so the throwing fold rolled the write back.
        assertThat(eventStore.read(streamId).events().count()).isZero();
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
    static class SynchronousProjectionApplication {
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
        CounterStore counterStore() {
            return new CounterStore();
        }

        @Bean
        CounterProjection counterProjection() {
            return new CounterProjection();
        }
    }

    static class CounterProjection {
        @Projection(id = "sync-counter", mode = org.occurrent.annotation.Mode.SYNCHRONOUS, storeName = "counterStore")
        org.occurrent.dsl.projection.Projection<Counter, TestEvent, String> counter() {
            return org.occurrent.dsl.projection.Projection.<Counter, TestEvent, String>builder(new Counter(0))
                    .id(event -> "counter")
                    .on(Counted.class, (state, event) -> new Counter(state.count() + 1))
                    .on(Boom.class, (state, event) -> {
                        throw new IllegalStateException("boom");
                    })
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

        int countFor(String id) {
            Counter current = store.get(id);
            return current == null ? 0 : current.count();
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

    record Boom(String eventId, Date timestamp, String name) implements TestEvent {
        Boom(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
