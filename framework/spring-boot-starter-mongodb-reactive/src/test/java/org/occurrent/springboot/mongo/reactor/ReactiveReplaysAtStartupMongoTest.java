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
import org.occurrent.springboot.common.internal.StartupGap;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves when the replays of {@code startupMode = WAIT_UNTIL_STARTED} subscriptions run. They start once the startup
 * scan has registered every handler and opened the application service for writes, in the order the handlers
 * registered, and each one has delivered all the history it replays before the next one starts and before the
 * application goes on starting, which an {@code ApplicationRunner} observes.
 * <p>
 * The handlers pace themselves, so a replay that started while another was running, or that did not finish before the
 * runner ran, shows up in the order of what they recorded.
 */
@DisplayName("Reactive Replays at startup")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveReplaysAtStartupMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:reactive-replays-at-startup");
    private static final List<String> HISTORY = List.of("s1", "s2", "s3");
    private static final List<String> EVERY_REPLAY_IN_ORDER = List.of("one:s1", "one:s2", "one:s3", "two:s1", "two:s2", "two:s3");

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void a_wait_until_started_replay_has_delivered_all_stored_history_before_an_application_runner_runs() {
        String database = "history-before-the-runner";
        seed(database);

        try (ConfigurableApplicationContext context = boot(ReplaysApplication.class, database)) {
            assertThat(context.getBean(Seen.class).byTheRunner.get())
                    .as("[what the replays had delivered when the application runner ran]")
                    .containsExactlyElementsOf(EVERY_REPLAY_IN_ORDER);
        }
    }

    @Test
    void replays_start_after_the_startup_scan_in_registration_order_each_finishing_before_the_next_starts() {
        String database = "replay-order";
        seed(database);

        try (ConfigurableApplicationContext context = boot(ReplaysApplication.class, database)) {
            assertThat(context.getBean(Log.class).entries)
                    .as("[deliveries in the order they happened, a delivery before the application service admitted writes is marked]")
                    .containsExactlyElementsOf(EVERY_REPLAY_IN_ORDER);
        }
    }

    private static void seed(String database) {
        try (ConfigurableApplicationContext context = boot(SeedApplication.class, database)) {
            applicationServiceOf(context).execute(UUID.randomUUID().toString(), __ -> HISTORY.stream().<TestEvent>map(Seeded::new).toList()).block();
        }
    }

    @SuppressWarnings("unchecked")
    private static ApplicationService<TestEvent> applicationServiceOf(ConfigurableApplicationContext context) {
        return context.getBean(ApplicationService.class);
    }

    private static ConfigurableApplicationContext boot(Class<?> application, String database) {
        return SpringApplication.run(application,
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE);
    }

    static class Log {
        final CopyOnWriteArrayList<String> entries = new CopyOnWriteArrayList<>();
    }

    static class Seen {
        final AtomicReference<List<String>> byTheRunner = new AtomicReference<>();
    }

    // A delivery that happens while the startup scan still runs is marked, since the application service refuses writes
    // then
    static class Replayer {
        final Log log;
        private final String name;
        private final ObjectProvider<StartupGap> startupGaps;

        Replayer(String name, Log log, ObjectProvider<StartupGap> startupGaps) {
            this.name = name;
            this.log = log;
            this.startupGaps = startupGaps;
        }

        String entry(Seeded event) {
            boolean admitsWrites = startupGaps.orderedStream().allMatch(StartupGap::admitsWrite);
            return name + ":" + event.name() + (admitsWrites ? "" : " (before the application service admitted writes)");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventConverters {

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
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class SeedApplication {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @EnableOccurrentReactive
    @Import(EventConverters.class)
    static class ReplaysApplication {

        @Bean
        Log log() {
            return new Log();
        }

        @Bean
        Seen seen() {
            return new Seen();
        }

        @Bean
        ApplicationRunner recordWhatTheReplaysHaveDelivered(Log log, Seen seen) {
            return arguments -> seen.byTheRunner.set(List.copyOf(log.entries));
        }

        @Bean
        ReplayOne replayOne(Log log, ObjectProvider<StartupGap> startupGaps) {
            return new ReplayOne(log, startupGaps);
        }

        @Bean
        ReplayTwo replayTwo(Log log, ObjectProvider<StartupGap> startupGaps) {
            return new ReplayTwo(log, startupGaps);
        }
    }

    static class ReplayOne extends Replayer {
        ReplayOne(Log log, ObjectProvider<StartupGap> startupGaps) {
            super("one", log, startupGaps);
        }

        @StreamSubscription(id = "reactiveReplayOne", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            return Mono.delay(Duration.ofMillis(40)).then(Mono.fromRunnable(() -> log.entries.add(entry(event))));
        }
    }

    static class ReplayTwo extends Replayer {
        ReplayTwo(Log log, ObjectProvider<StartupGap> startupGaps) {
            super("two", log, startupGaps);
        }

        @StreamSubscription(id = "reactiveReplayTwo", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME, startupMode = StartupMode.WAIT_UNTIL_STARTED)
        Mono<Void> on(Seeded event) {
            return Mono.delay(Duration.ofMillis(40)).then(Mono.fromRunnable(() -> log.entries.add(entry(event))));
        }
    }

    sealed interface TestEvent {
        String eventId();

        Date timestamp();

        String name();
    }

    record Seeded(String eventId, Date timestamp, String name) implements TestEvent {
        Seeded(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
