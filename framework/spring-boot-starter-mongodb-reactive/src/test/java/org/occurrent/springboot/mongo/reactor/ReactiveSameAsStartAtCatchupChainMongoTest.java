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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code SAME_AS_START_AT} resolves null for the durable layer, which then hands the catch-up the same start, so the
 * catch-up replays and goes live while nothing is stored for the subscription.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveSameAsStartAtCatchupChainMongoTest {

    private static final String SUBSCRIPTION = "reactive-same-as-start-at-chain";

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void the_durable_layer_passes_a_null_answer_through_so_the_catchup_replays_and_nothing_is_stored() {
        // Given
        String[] args = bootArgs("reactive-same-as-start-at-chain");
        SpringApplication.run(HistoryApplication.class, args).close();
        // A live start opens at the start of the current second, so the history must be a second older to need a replay
        await().pollDelay(ofMillis(2100)).atMost(ofSeconds(5)).until(() -> true);
        ConfigurableApplicationContext ctx = SpringApplication.run(ChainApplication.class, args);
        try {
            Subscriber subscriber = ctx.getBean(Subscriber.class);
            CheckpointStorage storage = ctx.getBean(CheckpointStorage.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(subscriber.received()).as("what the replay delivered").containsExactly("historic-1", "historic-2"));

            // When
            ctx.getBean(ApplicationService.class).execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("live-1"))).block();

            // Then
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(subscriber.received()).as("what the subscription delivered").containsExactly("historic-1", "historic-2", "live-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(SUBSCRIPTION).blockOptional()).as("the checkpoint after a live event").isEmpty());
        } finally {
            ctx.close();
        }
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    // Appends the history in a boot of its own, before the subscribing boot reads where its live stream starts, so
    // only a replay delivers it
    @SpringBootApplication
    @EnableOccurrentReactive
    static class HistoryApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return converter(typeMapper);
        }

        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService) {
            return new HistoryAppender(applicationService);
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class ChainApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return converter(typeMapper);
        }

        @Bean
        Subscriber subscriber() {
            return new Subscriber();
        }
    }

    private static CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:reactive-same-as-start-at-chain"))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;

        HistoryAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendHistory() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("historic-1"))).block();
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("historic-2"))).block();
        }
    }

    static class Subscriber {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @Subscription(id = SUBSCRIPTION, startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event) {
            received.add(event.name());
            return Mono.empty();
        }

        List<String> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
