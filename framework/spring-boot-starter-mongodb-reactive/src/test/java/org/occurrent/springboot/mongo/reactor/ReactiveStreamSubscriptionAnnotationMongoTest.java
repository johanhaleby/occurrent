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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
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
 * Proves that both the {@link StreamSubscription} annotation and the deprecated {@link Subscription} alias wire a working
 * reactive stream subscription that receives appended events, processed by {@code OccurrentReactiveAnnotationBeanPostProcessor}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveStreamSubscriptionAnnotationMongoTest.AnnotationApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-stream-subscription-annotation-test"
        }
)
@Import(ReactiveStreamSubscriptionAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
class ReactiveStreamSubscriptionAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-stream-subscription-annotation-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private StreamAnnotatedSubscriber streamAnnotatedSubscriber;

    @Autowired
    private DeprecatedAnnotatedSubscriber deprecatedAnnotatedSubscriber;

    @Test
    void both_the_new_and_the_deprecated_annotation_receive_an_appended_event() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(), "name");
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(event)).block();

        await().atMost(ofSeconds(20)).untilAsserted(() -> {
            assertThat(streamAnnotatedSubscriber.received()).extracting(TestEvent::eventId).contains(event.eventId());
            assertThat(deprecatedAnnotatedSubscriber.received()).extracting(TestEvent::eventId).contains(event.eventId());
        });
    }

    static class StreamAnnotatedSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "reactiveStreamAnnotatedSubscriber")
        Mono<Void> on(TestEvent event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class DeprecatedAnnotatedSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SuppressWarnings("deprecation")
        @Subscription(id = "reactiveDeprecatedAnnotatedSubscriber")
        Mono<Void> on(TestEvent event) {
            received.add(event);
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }
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
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        StreamAnnotatedSubscriber streamAnnotatedSubscriber() {
            return new StreamAnnotatedSubscriber();
        }

        @Bean
        DeprecatedAnnotatedSubscriber deprecatedAnnotatedSubscriber() {
            return new DeprecatedAnnotatedSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
