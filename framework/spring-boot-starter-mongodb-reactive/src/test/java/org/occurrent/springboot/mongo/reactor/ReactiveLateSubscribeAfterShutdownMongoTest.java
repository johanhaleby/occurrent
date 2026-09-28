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
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.springboot.reactor.ComposedCatchupModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * A lazy bean built on a Reactor non-blocking thread subscribes on another thread, where nothing can throw to the
 * caller. On the model the starter builds by default, a durable model over a catch-up model over
 * {@code ReactorMongoSubscriptionModel}, a subscribe from the beginning after the model is shut down has to be refused
 * before the catch-up replays any history, so the subscription gives its id back instead of receiving history from a
 * model that is shut down.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveLateSubscribeAfterShutdownMongoTest.LateSubscribeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-late-subscribe-after-shutdown-test"
        }
)
@Import(ReactiveLateSubscribeAfterShutdownMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@DirtiesContext
@Timeout(60)
class ReactiveLateSubscribeAfterShutdownMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-late-subscribe-after-shutdown-test");

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private ReactorDurableSubscriptionModel subscriptionModel;

    @Autowired
    private ComposedCatchupModel composedCatchupModel;

    @Test
    void a_late_subscribe_from_the_beginning_on_the_starters_model_after_shutdown_replays_nothing_and_gives_its_id_back() {
        assertThat(composedCatchupModel.catchupModelFor(subscriptionModel)).containsInstanceOf(ReactorCatchupSubscriptionModel.class);
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("historic"))).block();
        subscriptionModel.shutdown();

        BeginningSubscriber first = (BeginningSubscriber) resolvedOnAParallelThread("firstBeginningSubscriber");

        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                assertThat(first.invocations).describedAs("events delivered after the model was shut down").hasValue(0));
        // A second bean claiming the same id builds only once the first has given it back
        await().atMost(ofSeconds(10)).pollInterval(ofMillis(200)).untilAsserted(() ->
                assertThatCode(() -> resolvedOnAParallelThread("secondBeginningSubscriber")).doesNotThrowAnyException());
    }

    private Object resolvedOnAParallelThread(String beanName) {
        return Mono.fromCallable(() -> context.getBean(beanName))
                .subscribeOn(Schedulers.parallel())
                .block(Duration.ofSeconds(10));
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
    static class LateSubscribeApplication {

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

        // Declared as Marker so the startup scan, which reads the declared type, sees no handler and leaves both
        // unbuilt. Both claim one id, which is how the test tells whether the first gave it back.
        @Bean
        @Lazy
        Marker firstBeginningSubscriber() {
            return new BeginningSubscriber();
        }

        @Bean
        @Lazy
        Marker secondBeginningSubscriber() {
            return new BeginningSubscriber();
        }
    }

    interface Marker {
    }

    static class BeginningSubscriber implements Marker {
        final AtomicInteger invocations = new AtomicInteger();

        @StreamSubscription(id = "reactive-late-beginning-after-shutdown", startAt = StartPosition.BEGINNING_OF_TIME)
        Mono<Void> on(TestEvent event) {
            invocations.incrementAndGet();
            return Mono.empty();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
