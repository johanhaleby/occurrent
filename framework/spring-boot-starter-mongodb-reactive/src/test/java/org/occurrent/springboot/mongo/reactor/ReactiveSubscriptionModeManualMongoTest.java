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
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Proves {@code occurrent.subscription.mode=manual} on the reactive stack: the subscription beans exist and every
 * annotated subscription is registered, but nothing runs until the application resumes it.
 * <p>
 * The subscriber deliberately asks for {@link StartupMode#WAIT_UNTIL_STARTED}, which under {@code manual} would block
 * the context forever if the registrars did not skip the wait, since a paused reactive subscription's
 * {@code waitUntilStarted()} never completes. That is what this test costs if a gate is ever dropped: the wait happens
 * while Spring builds the context, before the test method runs, so {@link Timeout} does not cover it and the build
 * hangs instead of reporting a failure. Verified by removing one gate and watching a run that normally takes about a
 * minute pass ten without finishing.
 * <p>
 * One test method rather than three, because resuming is a one-way move and Spring caches the context for the whole
 * class. Split up, whichever method ran first would decide what the others saw.
 */
@DisplayName("Reactive subscription mode manual")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = ReactiveSubscriptionModeManualMongoTest.ManualModeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream",
                "occurrent.subscription.mode=manual",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-subscription-mode-manual-test"
        }
)
@Import(ReactiveSubscriptionModeManualMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class ReactiveSubscriptionModeManualMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-subscription-mode-manual-test");
    private static final String SUBSCRIPTION_ID = "reactiveManualModeSubscriber";

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private SubscriptionModelLifeCycle subscriptionModel;

    @Autowired
    private ManualModeSubscriber subscriber;

    @Test
    void a_subscription_stays_paused_until_the_application_resumes_it() {
        assertAll(
                () -> assertThat(subscriptionModel.isRunning()).isFalse(),
                () -> assertThat(subscriptionModel.isPaused(SUBSCRIPTION_ID)).isTrue(),
                () -> assertThat(subscriptionModel.isRunning(SUBSCRIPTION_ID)).isFalse()
        );

        TestEvent whileWaiting = event();
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(whileWaiting)).block();
        await().during(ofSeconds(2)).atMost(ofSeconds(10)).until(() -> subscriber.received().isEmpty());

        subscriptionModel.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted().block(ofSeconds(10));

        TestEvent afterResuming = event();
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(afterResuming)).block();
        // Both of them: waiting withholds events rather than losing them, because the position was pinned when the
        // subscription was registered rather than when it was started.
        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(subscriber.received()).extracting(TestEvent::eventId)
                        .contains(whileWaiting.eventId(), afterResuming.eventId()));
    }

    private static TestEvent event() {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), "name");
    }

    static class ManualModeSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = SUBSCRIPTION_ID, startupMode = StartupMode.WAIT_UNTIL_STARTED)
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
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
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
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        ManualModeSubscriber manualModeSubscriber() {
            return new ManualModeSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
