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

package org.occurrent.springboot.mongo.blocking;

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
import org.occurrent.application.service.blocking.ApplicationService;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Characterizes what happens when a blocking {@link StreamSubscription} handler always throws: the underlying
 * {@code SpringMongoSubscriptionModel} retries with backoff and, once retries are exhausted, restarts the change
 * stream listener in a new thread from the position before the failing event, so the same event is redelivered
 * forever rather than being skipped. This is a genuine correctness property (no silent data loss), but its
 * consequence is that a poison-pill event blocks all forward progress on that subscription: a later, otherwise
 * healthy event is never delivered either, since the redelivery loop never advances past the one that keeps failing.
 * <p>
 * This is not asserted to run out the full retry-and-restart cycle (unbounded by design); it only proves the
 * short-term blocking behavior within a bounded window, which is the operationally relevant fact for anyone relying
 * on this annotation.
 */
@DisplayName("StreamSubscription handler error: redelivery blocks forward progress")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = StreamSubscriptionHandlerErrorAnnotationMongoTest.HandlerErrorApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:stream-handler-error-test"
        }
)
@Import(StreamSubscriptionHandlerErrorAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class StreamSubscriptionHandlerErrorAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:stream-handler-error-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private AlwaysThrowingSubscriber alwaysThrowingSubscriber;

    @Test
    void a_permanently_failing_handler_blocks_delivery_of_a_later_event_on_the_same_subscription() {
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("poison")));
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("should-not-arrive")));

        // The poison event is retried (its handler is invoked at least once), but since it always throws it is never
        // recorded as successfully processed, and the later event never arrives either: the redelivery loop for the
        // poison event blocks the subscription from advancing past it.
        await().atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(alwaysThrowingSubscriber.invocationCount()).isGreaterThan(0));
        await().during(ofSeconds(5)).atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(alwaysThrowingSubscriber.received()).extracting(TestEvent::name).doesNotContain("should-not-arrive"));
    }

    static class AlwaysThrowingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocations = new AtomicInteger();

        @StreamSubscription(id = "stream-handler-error-subscriber")
        void on(TestEvent event) {
            invocations.incrementAndGet();
            if (event.name().equals("poison")) {
                throw new RuntimeException("Simulated permanent handler failure for " + event.name());
            }
            received.add(event);
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
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
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
        AlwaysThrowingSubscriber alwaysThrowingSubscriber() {
            return new AlwaysThrowingSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
