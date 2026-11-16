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

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayName;
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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code DEFAULT} and {@code NOW} on the reactive {@link StreamSubscription} never replay history written
 * through the same MongoClient more than a second before they subscribe, only deliver live events, exactly as {@code OccurrentReactiveMongoAutoConfigurationWiringTest}'s fail-loud
 * tests document as the supported alternative to a time-based start (which the reactive stack rejects outright, since
 * it has no stream catch-up model).
 */
@DisplayName("Reactive StreamSubscription startAt: DEFAULT and NOW never replay history older than a second")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveStreamSubscriptionStartPositionAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-stream-start-position-test");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void default_and_now_never_replay_history_older_than_a_second_only_live_events() throws InterruptedException {
        // The start of a subscription at the present is read when the event store bean is created, so only what is
        // written before the context starts is history. A context with subscriptions off writes it and is closed again.
        SpringApplication.run(StartPositionApplication.class, bootArgs("--occurrent.subscription.enabled=false", "--test.history=historic")).close();
        // A live start opens at the start of the current second, so the history must be a second older to be excluded
        Thread.sleep(2100);

        try (ConfigurableApplicationContext context = SpringApplication.run(StartPositionApplication.class, bootArgs())) {
            @SuppressWarnings("unchecked")
            ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
            DefaultPositionSubscriber defaultPositionSubscriber = context.getBean(DefaultPositionSubscriber.class);
            NowPositionSubscriber nowPositionSubscriber = context.getBean(NowPositionSubscriber.class);

            // Neither subscriber's handler is ever invoked for the historic event, and consequently neither
            // sees it in received() either, even after settling.
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
                assertThat(defaultPositionSubscriber.invocationCount()).isZero();
                assertThat(defaultPositionSubscriber.received()).isEmpty();
            });
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
                assertThat(nowPositionSubscriber.invocationCount()).isZero();
                assertThat(nowPositionSubscriber.received()).isEmpty();
            });

            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("live-default"))).block();
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("live-now"))).block();

            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-default"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(nowPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-now"));
        }
    }

    private static String[] bootArgs(String... more) {
        return Stream.concat(Stream.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("reactive-stream-start-position"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE), Stream.of(more)).toArray(String[]::new);
    }

    // --- inner application and configuration classes ---

    @SpringBootApplication
    @EnableOccurrentReactive
    static class StartPositionApplication {

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

        // Appends the history named by test.history, which only the boot with subscriptions off is given
        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService, @Value("${test.history:}") List<String> history) {
            return new HistoryAppender(applicationService, history);
        }

        @Bean
        DefaultPositionSubscriber defaultPositionSubscriber() {
            return new DefaultPositionSubscriber();
        }

        @Bean
        NowPositionSubscriber nowPositionSubscriber() {
            return new NowPositionSubscriber();
        }
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;
        private final List<String> history;

        HistoryAppender(ApplicationService<TestEvent> applicationService, List<String> history) {
            this.applicationService = applicationService;
            this.history = history;
        }

        @PostConstruct
        void appendHistory() {
            for (String name : history) {
                applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block();
            }
        }
    }

    // --- subscribers ---

    static class DefaultPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @StreamSubscription(id = "reactive-sp-default")
        Mono<Void> on(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-default")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class NowPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @StreamSubscription(id = "reactive-sp-now", startAt = StartPosition.NOW)
        Mono<Void> on(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-now")) {
                received.add(event);
            }
            return Mono.empty();
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
