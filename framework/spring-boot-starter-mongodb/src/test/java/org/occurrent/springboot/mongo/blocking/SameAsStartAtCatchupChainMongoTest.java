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

package org.occurrent.springboot.mongo.blocking;

import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.testcontainers.junit.jupiter.Container;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code SAME_AS_START_AT} resolves null for the competing consumer and the durable layer, so the catch-up replays
 * without the live delegate. Its live start reaches only the innermost Mongo model, and nothing stays stored for the
 * subscription, neither at the handover nor for a live event.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class SameAsStartAtCatchupChainMongoTest {

    private static final String SUBSCRIPTION = "same-as-start-at-chain";

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void a_same_as_start_at_subscription_replays_goes_live_and_stores_no_checkpoint() {
        // Given
        ConfigurableApplicationContext ctx = SpringApplication.run(ChainApplication.class, bootArgs("same-as-start-at-chain"));
        try {
            Subscriber subscriber = ctx.getBean(Subscriber.class);
            CheckpointStorage storage = ctx.getBean(CheckpointStorage.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(subscriber.received()).as("what the replay delivered").containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(SUBSCRIPTION)).as("the checkpoint after the handover").isNull());

            // When
            ctx.getBean(ApplicationService.class).execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("live-1")));

            // Then
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(subscriber.received()).as("what the subscription delivered").containsExactly("historic-1", "historic-2", "live-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(SUBSCRIPTION)).as("the checkpoint after a live event").isNull());
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

    @SpringBootApplication
    @EnableOccurrent
    static class ChainApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:same-as-start-at-chain"))
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // Appends the history before the subscriber starts, so the subscription replays it
        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService) {
            return new HistoryAppender(applicationService);
        }

        @Bean
        @DependsOn("historyAppender")
        Subscriber subscriber() {
            return new Subscriber();
        }
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;

        HistoryAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendHistory() {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("historic-1")));
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent("historic-2")));
        }
    }

    static class Subscriber {
        private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = SUBSCRIPTION, startAt = StartPosition.BEGINNING_OF_TIME, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event) {
            received.add(event.name());
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
