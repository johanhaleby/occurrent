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

import jakarta.annotation.PostConstruct;
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
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.testcontainers.junit.jupiter.Container;
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

/**
 * Pins {@code occurrent.subscription.mode=manual} on the reactive stack across a restart, which is the case where a
 * subscription no longer goes straight to live delivery. A first boot consumes an event and stores a checkpoint, so
 * the second boot resumes from it and takes the catch-up path instead. Catch-up replays out of the event store rather
 * than the change stream, so it is the one route that could deliver events to a subscription nobody has started.
 * <p>
 * Booted with {@code SpringApplication.run} rather than {@code @SpringBootTest}, so the second boot registers the
 * annotation again the way a real restart does.
 */
@DisplayName("Reactive subscription mode manual (across restart)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveSubscriptionModeManualRestartMongoTest {

    private static final String SUBSCRIPTION_ID = "reactiveManualModeRestartSubscriber";
    private static final URI SOURCE = URI.create("urn:occurrent:reactive-subscription-mode-manual-restart");

    private static final CopyOnWriteArrayList<TestEvent> FIRST_BOOT_RECEIVED = new CopyOnWriteArrayList<>();
    private static final CopyOnWriteArrayList<TestEvent> SECOND_BOOT_RECEIVED = new CopyOnWriteArrayList<>();

    @Container
    static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        mongoDBContainer.withReuse(true);
        // Pinned because the replica set advertises its own address, so the driver leaves the mapped port as soon as
        // it discovers the set. The other restart tests in this module pin it for the same reason.
        mongoDBContainer.setPortBindings(List.of("27017:27017"));
    }

    @Test
    void a_stored_checkpoint_does_not_let_a_manual_subscription_replay_before_it_is_resumed() {
        TestEvent beforeRestart = event();
        runFirstBoot(beforeRestart);

        ConfigurableApplicationContext context = SpringApplication.run(ManualBootApplication.class, bootArgs("manual"));
        try {
            SubscriptionModelLifeCycle subscriptionModel = context.getBean(SubscriptionModelLifeCycle.class);
            assertThat(subscriptionModel.isPaused(SUBSCRIPTION_ID)).isTrue();

            // The event was appended before the subscription registered, so a catch-up replay would deliver it here.
            await().during(ofSeconds(5)).atMost(ofSeconds(15)).until(SECOND_BOOT_RECEIVED::isEmpty);

            subscriptionModel.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted().block(ofSeconds(30));

            // Resuming from the stored checkpoint still delivers it, so pausing withholds events rather than losing them.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(SECOND_BOOT_RECEIVED).extracting(TestEvent::eventId).contains(OfflineAppender.APPENDED_WHILE_DOWN));
        } finally {
            context.close();
        }
    }

    private static void runFirstBoot(TestEvent beforeRestart) {
        ConfigurableApplicationContext context = SpringApplication.run(AutoBootApplication.class, bootArgs("auto"));
        try {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> List.of(beforeRestart)).block();
            // Receiving it is what writes the checkpoint the second boot resumes from.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(FIRST_BOOT_RECEIVED).extracting(TestEvent::eventId).contains(beforeRestart.eventId()));
        } finally {
            context.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<TestEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    private static TestEvent event() {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), "name");
    }

    private static String[] bootArgs(String mode) {
        return new String[]{
                "--spring.data.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("reactive-subscription-mode-manual-restart"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.subscription.mode=" + mode,
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE
        };
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    static class FirstBootSubscriber {
        @StreamSubscription(id = SUBSCRIPTION_ID)
        Mono<Void> on(TestEvent event) {
            FIRST_BOOT_RECEIVED.add(event);
            return Mono.empty();
        }
    }

    static class SecondBootSubscriber {
        @StreamSubscription(id = SUBSCRIPTION_ID)
        Mono<Void> on(TestEvent event) {
            SECOND_BOOT_RECEIVED.add(event);
            return Mono.empty();
        }
    }

    /**
     * Appends an event before the subscription registers, standing in for one written while the application was down.
     */
    static class OfflineAppender {
        static final String APPENDED_WHILE_DOWN = "reactive-appended-while-down";

        private final ApplicationService<TestEvent> applicationService;

        OfflineAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendWhileDown() {
            applicationService.execute(UUID.randomUUID().toString(),
                    __ -> List.of(new TestEvent(APPENDED_WHILE_DOWN, new Date(), "name"))).block();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class AutoBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        FirstBootSubscriber firstBootSubscriber() {
            return new FirstBootSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class ManualBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        OfflineAppender offlineAppender(ApplicationService<TestEvent> applicationService) {
            return new OfflineAppender(applicationService);
        }

        @Bean
        @DependsOn("offlineAppender")
        SecondBootSubscriber secondBootSubscriber() {
            return new SecondBootSubscriber();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
    }
}
