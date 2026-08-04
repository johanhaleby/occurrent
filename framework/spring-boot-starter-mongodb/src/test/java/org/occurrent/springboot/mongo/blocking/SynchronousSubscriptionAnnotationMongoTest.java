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

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the {@link SynchronousSubscription} annotation wiring end-to-end on MongoDB: a handler runs synchronously
 * on the writer's thread before {@code execute} returns; with the auto-configured {@code SpringTransactionExecutor}
 * the write and the handlers commit atomically, so a throwing handler rolls the write back; and a handler-side
 * {@code @Transactional(REQUIRES_NEW)} is honored, which is only possible if the handler is invoked through its Spring
 * proxy.
 */
@DisplayName("SynchronousSubscription annotation (stream mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SynchronousSubscriptionAnnotationMongoTest.SynchronousApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:synchronous-subscription-annotation-test"
        }
)
@Import(SynchronousSubscriptionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class SynchronousSubscriptionAnnotationMongoTest {

    static final String MARKER_COLLECTION = "sync-markers";
    private static final URI SOURCE = URI.create("urn:occurrent:synchronous-subscription-annotation-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private RecordingSubscriber recordingSubscriber;

    @Test
    void handler_runs_synchronously_before_execute_returns_and_commits_with_the_write() {
        String streamId = UUID.randomUUID().toString();
        Recorded event = new Recorded("recorded-1");

        applicationService.execute(streamId, __ -> List.of(event));

        // No awaitility: if the handler ran, it ran on this thread before execute returned.
        assertThat(recordingSubscriber.received()).extracting(TestEvent::name).contains("recorded-1");
        assertThat(eventStore.read(streamId).events().count()).isEqualTo(1);
    }

    @Test
    void a_throwing_synchronous_handler_rolls_the_event_write_back() {
        String streamId = UUID.randomUUID().toString();

        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new Boom("boom-1"))))
                .hasStackTraceContaining("boom");

        // The SpringTransactionExecutor spans the write and the handler, so the throwing handler rolled the write back.
        assertThat(eventStore.read(streamId).events().count()).isZero();
    }

    @Test
    void a_handler_side_requires_new_transaction_is_honored_and_survives_the_outer_rollback() {
        String streamId = UUID.randomUUID().toString();
        String markerId = UUID.randomUUID().toString();

        // One command writes two events in order: the Marker handler (REQUIRES_NEW) commits its marker in a new
        // transaction, then the Boom handler throws and rolls the outer transaction (the event write) back. Dispatch
        // order follows write order, so the marker is committed before the outer rollback.
        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new Marker(markerId), new Boom("boom-2"))))
                .hasStackTraceContaining("boom");

        // The event write was rolled back...
        assertThat(eventStore.read(streamId).events().count()).isZero();
        // ...but the marker written in the handler's REQUIRES_NEW transaction survived. This is only possible if the
        // handler was invoked through its Spring proxy, so the @Transactional(REQUIRES_NEW) advice actually ran.
        assertThat(mongoOperations.findById(markerId, Document.class, MARKER_COLLECTION)).isNotNull();
    }

    // --- inner application and configuration classes ---

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
    static class SynchronousApplication {

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
        RecordingSubscriber recordingSubscriber() {
            return new RecordingSubscriber();
        }

        @Bean
        ThrowingSubscriber throwingSubscriber() {
            return new ThrowingSubscriber();
        }

        @Bean
        MarkerWritingSubscriber markerWritingSubscriber(MongoOperations mongoOperations) {
            return new MarkerWritingSubscriber(mongoOperations);
        }
    }

    static class RecordingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "recording-synchronous-subscriber")
        void on(Recorded event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class ThrowingSubscriber {

        @SynchronousSubscription(id = "throwing-synchronous-subscriber")
        void on(Boom event) {
            throw new IllegalStateException("boom");
        }
    }

    static class MarkerWritingSubscriber {
        private final MongoOperations mongoOperations;

        MarkerWritingSubscriber(MongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
        }

        // REQUIRES_NEW so this write commits independently of the outer command transaction. It only takes effect if
        // this method is invoked through the Spring proxy, which is what the test asserts.
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        @SynchronousSubscription(id = "marker-writing-synchronous-subscriber")
        void on(Marker event) {
            mongoOperations.insert(new Document("_id", event.eventId()), MARKER_COLLECTION);
        }
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Recorded(String eventId, Date timestamp, String name) implements TestEvent {
        Recorded(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record Boom(String eventId, Date timestamp, String name) implements TestEvent {
        Boom(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record Marker(String eventId, Date timestamp, String name) implements TestEvent {
        Marker(String markerId) {
            this(markerId, new Date(), "marker");
        }
    }
}
