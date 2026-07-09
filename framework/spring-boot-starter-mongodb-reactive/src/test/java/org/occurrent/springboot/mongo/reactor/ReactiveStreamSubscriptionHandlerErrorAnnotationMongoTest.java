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
import org.junit.jupiter.api.Timeout;
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
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Characterizes what happens when a reactive {@link StreamSubscription} handler's returned {@code Mono} errors: the
 * error propagates through {@code concatMap} in {@code ReactorMongoSubscriptionModel} and terminates the whole
 * subscription's {@code Flux} permanently. There is no automatic restart for handler errors (only MongoDB-level
 * change-stream errors get retried). This is a real asymmetry with the blocking stack, which redelivers the failing
 * event forever instead of dying: recovering a reactive subscription after a handler error requires restarting the
 * application, not just fixing the transient cause.
 * <p>
 * The observable outcome this test asserts, a later event on the same subscription never arriving, is the same as
 * the blocking counterpart's test, but for a different underlying reason (a dead {@code Flux} here, an endless
 * redelivery loop there). Both are documented so the difference is visible, not discovered at runtime.
 */
@DisplayName("Reactive StreamSubscription handler error: an erroring Mono kills the subscription")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveStreamSubscriptionHandlerErrorAnnotationMongoTest.HandlerErrorApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-stream-handler-error-test"
        }
)
@Import(ReactiveStreamSubscriptionHandlerErrorAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveStreamSubscriptionHandlerErrorAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-stream-handler-error-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private AlwaysErroringSubscriber alwaysErroringSubscriber;

    @Test
    void an_erroring_handler_permanently_kills_the_subscription_so_a_later_event_never_arrives() {
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("poison"))).block();

        await().atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(alwaysErroringSubscriber.invocationCount()).isGreaterThan(0));

        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("should-not-arrive"))).block();

        // The Flux died on the poison event's error, so the later event is never delivered, unlike a MongoDB-level
        // change-stream error, which the model retries automatically.
        await().during(ofSeconds(5)).atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(alwaysErroringSubscriber.received()).extracting(TestEvent::name).doesNotContain("should-not-arrive"));
    }

    static class AlwaysErroringSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocations = new AtomicInteger();

        @StreamSubscription(id = "reactive-stream-handler-error-subscriber")
        Mono<Void> on(TestEvent event) {
            invocations.incrementAndGet();
            if (event.name().equals("poison")) {
                return Mono.error(new RuntimeException("Simulated permanent handler failure for " + event.name()));
            }
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocations.get();
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
    static class HandlerErrorApplication {

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
        AlwaysErroringSubscriber alwaysErroringSubscriber() {
            return new AlwaysErroringSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
