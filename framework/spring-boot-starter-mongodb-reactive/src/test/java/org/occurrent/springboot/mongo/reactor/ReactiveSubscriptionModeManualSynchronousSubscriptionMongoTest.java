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
import org.occurrent.annotation.SynchronousSubscription;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive counterpart of {@code SubscriptionModeManualSynchronousSubscriptionMongoTest}: a synchronous handler
 * keeps running under {@code occurrent.subscription.mode=manual}. Nothing is resumed before the write, so the handler
 * can only have seen the event if the synchronous model was running from the start.
 */
@DisplayName("Reactive subscription mode manual (synchronous subscription)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSubscriptionModeManualSynchronousSubscriptionMongoTest.ManualModeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.subscription.mode=manual",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-subscription-mode-manual-synchronous-test"
        }
)
@Import(ReactiveSubscriptionModeManualSynchronousSubscriptionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSubscriptionModeManualSynchronousSubscriptionMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-subscription-mode-manual-synchronous-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private SynchronousSubscriber subscriber;

    @Test
    void a_write_before_the_application_starts_anything_reaches_a_synchronous_handler() {
        TestEvent event = new TestEvent(UUID.randomUUID().toString(), new Date(), "name");

        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(event)).block();

        // No awaitility: the handler is composed into the write chain, so it ran before execute's Mono completed.
        assertThat(subscriber.received()).extracting(TestEvent::eventId).containsExactly(event.eventId());
    }

    static class SynchronousSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @SynchronousSubscription(id = "reactiveManualModeSynchronousSubscriber")
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
    static class ManualModeApplication {

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
        SynchronousSubscriber synchronousSubscriber() {
            return new SynchronousSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
