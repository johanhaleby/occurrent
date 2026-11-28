/*
 *
 *  Copyright 2024 Johan Haleby
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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the capability-agnostic {@code Projection} dispatch branch on the reactive stack: a non-DCB {@code Projection}
 * (capability AGNOSTIC) on a STREAM store replays history from BEGINNING over the unified global position, then folds
 * live events into an in-memory {@link ViewStateRepository}.
 */
@DisplayName("Reactive Projection annotation (agnostic, async catch-up)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveAgnosticProjectionAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-agnostic-projection-test");
    private static final String VIEW_ID = "names";

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    @SuppressWarnings("unchecked")
    void replays_stream_history_then_folds_live_events() {
        // The history is written by a context with subscriptions off, since the application service refuses a write while the application starts
        SpringApplication.run(StreamApplication.class, bootArgs("--occurrent.subscription.enabled=false", "--test.history=true")).close();

        try (ConfigurableApplicationContext context = SpringApplication.run(StreamApplication.class, bootArgs())) {
            ViewStateRepository<CountState, String> store = context.getBean(ViewStateRepository.class);
            ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(store.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(2)));

            applicationService.execute("stream-live", __ -> List.of(new Registered("live-1"))).block();
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(store.findById(VIEW_ID)).hasValueSatisfying(state -> assertThat(state.count()).isEqualTo(3)));
        }
    }

    private static String[] bootArgs(String... more) {
        return Stream.concat(Stream.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("reactive-agnostic-projection"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE), Stream.of(more)).toArray(String[]::new);
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class StreamApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        ViewStateRepository<CountState, String> store() {
            ConcurrentHashMap<String, CountState> map = new ConcurrentHashMap<>();
            return ViewStateRepository.create(map::get, map::put);
        }

        // Writes two stream events when test.history is set, so they exist before the projection registers and catches up.
        @Bean
        @ConditionalOnProperty(name = "test.history", havingValue = "true")
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService) {
            return new HistoryAppender(applicationService);
        }

        @Bean
        NamesProjection namesProjection() {
            return new NamesProjection();
        }
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;

        HistoryAppender(ApplicationService<TestEvent> applicationService) {
            this.applicationService = applicationService;
        }

        @PostConstruct
        void appendHistory() {
            applicationService.execute("stream-history", __ -> List.of(new Registered("historic-1"))).block();
            applicationService.execute("stream-history-2", __ -> List.of(new Registered("historic-2"))).block();
        }
    }

    @Component
    static class NamesProjection {
        @org.occurrent.annotation.Projection(id = "reactive-agnostic-names", startAt = org.occurrent.annotation.StartPosition.BEGINNING, capability = org.occurrent.annotation.Capability.AGNOSTIC)
        Projection<CountState, TestEvent, String> countRegistered() {
            return Projection.<CountState, TestEvent, String>builder(new CountState(0))
                    .id(event -> VIEW_ID)
                    .on(Registered.class, (state, event) -> new CountState(state.count() + 1))
                    .build();
        }
    }

    record CountState(int count) {
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Registered(String eventId, Date timestamp, String name) implements TestEvent {
        Registered(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
