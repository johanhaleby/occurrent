/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
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

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The reactive counterpart of {@code SubscriptionAnnotationTransactionalAdviceMongoTest}: proves that a handler-side
 * {@code @Transactional} on a {@link StreamSubscription}, {@link Subscription} or {@link DcbSubscription} method is
 * honored, one test per annotation. Each handler records whether the reactive
 * {@link TransactionSynchronizationManager#isActualTransactionActive()} answers true while it runs, which is only
 * possible if the handler was invoked through its Spring proxy rather than the raw bean the registrar's
 * {@code BeanPostProcessor} first sees.
 */
@DisplayName("Reactive subscription handler transactional advice")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSubscriptionAnnotationTransactionalAdviceMongoTest.AnnotationApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream,dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-subscription-transactional-advice-test"
        }
)
@Import(ReactiveSubscriptionAnnotationTransactionalAdviceMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSubscriptionAnnotationTransactionalAdviceMongoTest {

    static final String TAG = "test:transactional-advice";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-subscription-transactional-advice-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private StreamAnnotatedSubscriber streamAnnotatedSubscriber;

    @Autowired
    private AgnosticAnnotatedSubscriber agnosticAnnotatedSubscriber;

    @Autowired
    private DcbAnnotatedSubscriber dcbAnnotatedSubscriber;

    @Test
    void a_StreamSubscription_handler_runs_inside_the_transaction_its_own_Transactional_declares() {
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new StreamEvent("stream-1"))).block();

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(streamAnnotatedSubscriber.transactionWasActive()).isNotEmpty().containsOnly(true));
    }

    @Test
    void a_deprecated_Subscription_handler_runs_inside_the_transaction_its_own_Transactional_declares() {
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new AgnosticEvent("agnostic-1"))).block();

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(agnosticAnnotatedSubscriber.transactionWasActive()).isNotEmpty().containsOnly(true));
    }

    @Test
    void a_DcbSubscription_handler_runs_inside_the_transaction_its_own_Transactional_declares() {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(new DcbEvent("dcb-1")))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents).block();

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(dcbAnnotatedSubscriber.transactionWasActive()).isNotEmpty().containsOnly(true));
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
    @EnableOccurrentReactive
    static class AnnotationApplication {

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
        StreamAnnotatedSubscriber streamAnnotatedSubscriber() {
            return new StreamAnnotatedSubscriber();
        }

        @Bean
        AgnosticAnnotatedSubscriber agnosticAnnotatedSubscriber() {
            return new AgnosticAnnotatedSubscriber();
        }

        @Bean
        DcbAnnotatedSubscriber dcbAnnotatedSubscriber() {
            return new DcbAnnotatedSubscriber();
        }
    }

    static class StreamAnnotatedSubscriber {
        private final CopyOnWriteArrayList<Boolean> transactionWasActive = new CopyOnWriteArrayList<>();

        @Transactional
        @StreamSubscription(id = "reactive-stream-transactional-advice-subscriber")
        Mono<Void> on(StreamEvent event) {
            return TransactionSynchronizationManager.forCurrentTransaction()
                    .doOnNext(manager -> transactionWasActive.add(manager.isActualTransactionActive()))
                    .then();
        }

        List<Boolean> transactionWasActive() {
            return transactionWasActive;
        }
    }

    static class AgnosticAnnotatedSubscriber {
        private final CopyOnWriteArrayList<Boolean> transactionWasActive = new CopyOnWriteArrayList<>();

        @Transactional
        @SuppressWarnings("deprecation")
        @Subscription(id = "reactive-agnostic-transactional-advice-subscriber")
        Mono<Void> on(AgnosticEvent event) {
            return TransactionSynchronizationManager.forCurrentTransaction()
                    .doOnNext(manager -> transactionWasActive.add(manager.isActualTransactionActive()))
                    .then();
        }

        List<Boolean> transactionWasActive() {
            return transactionWasActive;
        }
    }

    static class DcbAnnotatedSubscriber {
        private final CopyOnWriteArrayList<Boolean> transactionWasActive = new CopyOnWriteArrayList<>();

        @Transactional
        @DcbSubscription(id = "reactive-dcb-transactional-advice-subscriber", eventTypes = DcbEvent.class)
        Mono<Void> on(DcbEvent event) {
            return TransactionSynchronizationManager.forCurrentTransaction()
                    .doOnNext(manager -> transactionWasActive.add(manager.isActualTransactionActive()))
                    .then();
        }

        List<Boolean> transactionWasActive() {
            return transactionWasActive;
        }
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record StreamEvent(String eventId, Date timestamp, String name) implements TestEvent {
        StreamEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record AgnosticEvent(String eventId, Date timestamp, String name) implements TestEvent {
        AgnosticEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }

    record DcbEvent(String eventId, Date timestamp, String name) implements TestEvent {
        DcbEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
