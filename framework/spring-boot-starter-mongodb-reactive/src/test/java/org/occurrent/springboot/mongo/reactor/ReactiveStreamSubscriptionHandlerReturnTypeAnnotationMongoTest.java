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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
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
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code OccurrentReactiveAnnotationBeanPostProcessor}'s {@code invokeMono} correctly adapts every
 * handler return shape it documents: a {@code void} method, a method returning {@code Mono<Void>}, and a method
 * returning a non-{@code Void} {@code Mono<T>} (chained with {@code .then()} so its completion still gates delivery
 * of the next event). None of these three shapes had annotation-level coverage before.
 */
@DisplayName("Reactive StreamSubscription handler return type adaptation")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveStreamSubscriptionHandlerReturnTypeAnnotationMongoTest.HandlerReturnTypeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-handler-return-type-test"
        }
)
@Import(ReactiveStreamSubscriptionHandlerReturnTypeAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
class ReactiveStreamSubscriptionHandlerReturnTypeAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-handler-return-type-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private VoidReturningSubscriber voidReturningSubscriber;

    @Autowired
    private MonoVoidReturningSubscriber monoVoidReturningSubscriber;

    @Autowired
    private MonoNonVoidReturningSubscriber monoNonVoidReturningSubscriber;

    @Test
    void void_mono_void_and_non_void_mono_handlers_all_deliver() {
        applicationService.execute(UUID.randomUUID().toString(), __ -> Stream.of(new TestEvent("void-event"))).block();
        applicationService.execute(UUID.randomUUID().toString(), __ -> Stream.of(new TestEvent("mono-void-event"))).block();
        applicationService.execute(UUID.randomUUID().toString(), __ -> Stream.of(new TestEvent("mono-non-void-event"))).block();

        await().atMost(ofSeconds(20)).untilAsserted(() ->
                assertThat(voidReturningSubscriber.received()).extracting(TestEvent::name).contains("void-event"));
        await().atMost(ofSeconds(20)).untilAsserted(() ->
                assertThat(monoVoidReturningSubscriber.received()).extracting(TestEvent::name).contains("mono-void-event"));
        await().atMost(ofSeconds(20)).untilAsserted(() ->
                assertThat(monoNonVoidReturningSubscriber.received()).extracting(TestEvent::name).contains("mono-non-void-event"));
    }

    // --- subscribers, one per handler return shape ---

    static class VoidReturningSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "reactive-handler-void")
        void on(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class MonoVoidReturningSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "reactive-handler-mono-void")
        Mono<Void> on(TestEvent event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class MonoNonVoidReturningSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // Returns Mono<String>, not Mono<Void>. invokeMono must chain .then() so completion still gates delivery.
        @StreamSubscription(id = "reactive-handler-mono-non-void")
        Mono<String> on(TestEvent event) {
            received.add(event);
            return Mono.just("handled:" + event.name());
        }

        List<TestEvent> received() {
            return received;
        }
    }

    // --- container configuration ---

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
    static class HandlerReturnTypeApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        VoidReturningSubscriber voidReturningSubscriber() {
            return new VoidReturningSubscriber();
        }

        @Bean
        MonoVoidReturningSubscriber monoVoidReturningSubscriber() {
            return new MonoVoidReturningSubscriber();
        }

        @Bean
        MonoNonVoidReturningSubscriber monoNonVoidReturningSubscriber() {
            return new MonoNonVoidReturningSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
