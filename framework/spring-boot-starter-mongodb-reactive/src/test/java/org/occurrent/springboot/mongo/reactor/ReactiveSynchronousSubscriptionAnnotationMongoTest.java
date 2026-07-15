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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
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
 * The reactive counterpart of {@code SynchronousSubscriptionAnnotationMongoTest}: proves the
 * {@link SynchronousSubscription} annotation wiring on the reactive stack. A handler runs synchronously in the write
 * chain before {@code execute} completes; with the auto-configured {@code SpringReactiveTransactionExecutor} the write
 * and the handlers commit atomically, so a handler whose {@code Mono} errors rolls the write back; and a handler-side
 * {@code @Transactional(REQUIRES_NEW)} is honored, which is only possible if the handler is invoked through its Spring
 * proxy.
 */
@DisplayName("Reactive SynchronousSubscription annotation (stream mode)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSynchronousSubscriptionAnnotationMongoTest.SynchronousApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-synchronous-subscription-annotation-test"
        }
)
@Import(ReactiveSynchronousSubscriptionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSynchronousSubscriptionAnnotationMongoTest {

    static final String MARKER_COLLECTION = "sync-markers";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-synchronous-subscription-annotation-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private ReactiveMongoOperations mongoOperations;

    @Autowired
    private RecordingSubscriber recordingSubscriber;

    @Test
    void handler_runs_synchronously_before_execute_completes_and_commits_with_the_write() {
        String streamId = UUID.randomUUID().toString();
        Recorded event = new Recorded("recorded-1");

        applicationService.execute(streamId, __ -> List.of(event)).block();

        // The handler was composed into the write chain, so by the time execute's Mono completed it had run.
        assertThat(recordingSubscriber.received()).extracting(TestEvent::name).contains("recorded-1");
        assertThat(streamEventCount(streamId)).isEqualTo(1);
    }

    @Test
    void a_handler_whose_mono_errors_rolls_the_event_write_back() {
        String streamId = UUID.randomUUID().toString();

        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new Boom("boom-1"))).block())
                .hasStackTraceContaining("boom");

        assertThat(streamEventCount(streamId)).isZero();
    }

    @Test
    void a_handler_side_requires_new_transaction_is_honored_and_survives_the_outer_rollback() {
        String streamId = UUID.randomUUID().toString();
        String markerId = UUID.randomUUID().toString();

        // The Marker handler (REQUIRES_NEW) commits its marker in a new transaction, then the Boom handler errors and
        // rolls the outer transaction (the event write) back. Dispatch order follows write order.
        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new Marker(markerId), new Boom("boom-2"))).block())
                .hasStackTraceContaining("boom");

        assertThat(streamEventCount(streamId)).isZero();
        // The marker survives because it was committed in the handler's REQUIRES_NEW transaction, which is only
        // possible if the handler was invoked through its Spring proxy.
        assertThat(mongoOperations.findById(markerId, Document.class, MARKER_COLLECTION).block()).isNotNull();
    }

    @Test
    void a_handler_side_required_transaction_joins_the_write_transaction_and_rolls_back_with_it() {
        String streamId = UUID.randomUUID().toString();
        String markerId = UUID.randomUUID().toString();

        // The JoiningMarker handler is @Transactional(REQUIRED), so its marker write joins the outer command
        // transaction instead of opening its own. The Boom handler then errors, rolling that single shared transaction
        // back. Contrast with the REQUIRES_NEW case above, whose marker survived.
        assertThatThrownBy(() -> applicationService.execute(streamId, __ -> List.of(new JoiningMarker(markerId), new Boom("boom-3"))).block())
                .hasStackTraceContaining("boom");

        // Both the event write and the handler's marker rolled back together, proving they shared one reactive
        // transaction propagated through the Reactor Context.
        assertThat(streamEventCount(streamId)).isZero();
        assertThat(mongoOperations.findById(markerId, Document.class, MARKER_COLLECTION).block()).isNull();
    }

    private long streamEventCount(String streamId) {
        Long count = eventStore.read(streamId).flatMapMany(EventStream::events).count().block();
        return count == null ? 0 : count;
    }

    // --- inner application and configuration classes ---

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
        MarkerWritingSubscriber markerWritingSubscriber(ReactiveMongoOperations mongoOperations) {
            return new MarkerWritingSubscriber(mongoOperations);
        }

        @Bean
        JoiningMarkerWritingSubscriber joiningMarkerWritingSubscriber(ReactiveMongoOperations mongoOperations) {
            return new JoiningMarkerWritingSubscriber(mongoOperations);
        }
    }

    static class RecordingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactive-recording-synchronous-subscriber")
        Mono<Void> on(Recorded event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class ThrowingSubscriber {

        @SynchronousSubscription(id = "reactive-throwing-synchronous-subscriber")
        Mono<Void> on(Boom event) {
            return Mono.error(new IllegalStateException("boom"));
        }
    }

    static class MarkerWritingSubscriber {
        private final ReactiveMongoOperations mongoOperations;

        MarkerWritingSubscriber(ReactiveMongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
        }

        // REQUIRES_NEW so this write commits independently of the outer command transaction. It only takes effect if
        // this method is invoked through the Spring proxy, which is what the test asserts.
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        @SynchronousSubscription(id = "reactive-marker-writing-synchronous-subscriber")
        Mono<Void> on(Marker event) {
            return mongoOperations.insert(new Document("_id", event.eventId()), MARKER_COLLECTION).then();
        }
    }

    static class JoiningMarkerWritingSubscriber {
        private final ReactiveMongoOperations mongoOperations;

        JoiningMarkerWritingSubscriber(ReactiveMongoOperations mongoOperations) {
            this.mongoOperations = mongoOperations;
        }

        // REQUIRED (the default) so this write JOINS the outer command transaction rather than starting its own. When
        // the command later rolls back, this marker must roll back with it, proving the handler ran in the same
        // reactive transaction as the write (propagated via the Reactor Context). Contrast with the REQUIRES_NEW
        // MarkerWritingSubscriber, whose marker survives the outer rollback.
        @Transactional(propagation = Propagation.REQUIRED)
        @SynchronousSubscription(id = "reactive-joining-marker-writing-synchronous-subscriber")
        Mono<Void> on(JoiningMarker event) {
            return mongoOperations.insert(new Document("_id", event.eventId()), MARKER_COLLECTION).then();
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

    record JoiningMarker(String eventId, Date timestamp, String name) implements TestEvent {
        JoiningMarker(String markerId) {
            this(markerId, new Date(), "joining-marker");
        }
    }
}
