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

package org.occurrent.springboot.mongo.blocking;

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
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the {@link StreamSubscription#startAt()} matrix: {@code DEFAULT} and {@code NOW} never replay history,
 * {@code BEGINNING_OF_TIME} replays all of it, and {@code startAtISO8601}/{@code startAtTimeEpochMillis} correctly
 * parse and drive the same replay path as {@code BEGINNING_OF_TIME}.
 * <p>
 * The ISO8601 and epoch cases both use a hardcoded date far in the past (well before any history in this test), so
 * their observable outcome is "replay everything", the same as {@code BEGINNING_OF_TIME}. A case demonstrating a
 * start time strictly between two historic events is not included: {@code startAtISO8601} is a compile-time
 * annotation constant, so the annotated method's fixed value cannot be computed from the test's own runtime clock or
 * from the timing of its own appends. That precise a scenario is already covered at the DSL level (not the
 * annotation level) by the existing {@code DcbDualModeCatchupAutoConfigurationMongoTest}-style tests. What this test
 * adds is proof that the ISO8601/epoch annotation attributes actually parse and reach the same replay mechanism as
 * the enum position, which the existing suite did not previously exercise at all.
 */
@DisplayName("StreamSubscription startAt matrix")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class StreamSubscriptionStartPositionAnnotationMongoTest {

    private static final URI SOURCE = URI.create("urn:occurrent:stream-start-position-test");
    // 2000-01-01T00:00:00Z, well before any event this test appends.
    private static final long FAR_PAST_EPOCH_MILLIS = 946684800000L;

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void default_and_now_never_replay_history_while_beginning_of_time_iso8601_and_epoch_all_replay_it() {
        // The start of a subscription at the present is read when the event store bean is created, so only what is
        // written before the context starts is history. A context with subscriptions off writes it and is closed again.
        SpringApplication.run(StartPositionApplication.class, bootArgs("--occurrent.subscription.enabled=false", "--test.history=historic-beginning,historic-iso,historic-epoch")).close();

        try (ConfigurableApplicationContext context = SpringApplication.run(StartPositionApplication.class, bootArgs())) {
            assertSubscribersSeeHistoryOnlyWhenTheyReplayIt(context);
        }
    }

    private static String[] bootArgs(String... more) {
        return Stream.concat(Stream.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl("stream-start-position"),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=" + SOURCE), Stream.of(more)).toArray(String[]::new);
    }

    @SuppressWarnings("unchecked")
    private static void assertSubscribersSeeHistoryOnlyWhenTheyReplayIt(ConfigurableApplicationContext context) {
        ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
        DefaultPositionSubscriber defaultPositionSubscriber = context.getBean(DefaultPositionSubscriber.class);
        NowPositionSubscriber nowPositionSubscriber = context.getBean(NowPositionSubscriber.class);
        BeginningOfTimeSubscriber beginningOfTimeSubscriber = context.getBean(BeginningOfTimeSubscriber.class);
        Iso8601Subscriber iso8601Subscriber = context.getBean(Iso8601Subscriber.class);
        EpochMillisSubscriber epochMillisSubscriber = context.getBean(EpochMillisSubscriber.class);

        // None of the live-only subscribers ever see the pre-existing history, even after settling.
        // The invocation-count checks prove the handler was never called at all, not merely that
        // whatever it was called with failed the name filter below.
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(defaultPositionSubscriber.invocationCount()).isZero();
            assertThat(defaultPositionSubscriber.received()).isEmpty();
        });
        await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(nowPositionSubscriber.invocationCount()).isZero();
            assertThat(nowPositionSubscriber.received()).isEmpty();
        });

        // The three replaying subscribers each see their own pre-existing historic event.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(beginningOfTimeSubscriber.received()).extracting(TestEvent::name).containsExactly("historic-beginning"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(iso8601Subscriber.received()).extracting(TestEvent::name).containsExactly("historic-iso"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(epochMillisSubscriber.received()).extracting(TestEvent::name).containsExactly("historic-epoch"));

        // A fresh event per scenario, appended now that the context is fully up, reaches every subscriber.
        for (String name : List.of("live-default", "live-now", "live-beginning", "live-iso", "live-epoch")) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name)));
        }

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(defaultPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-default"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(nowPositionSubscriber.received()).extracting(TestEvent::name).containsExactly("live-now"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(beginningOfTimeSubscriber.received()).extracting(TestEvent::name).containsExactly("historic-beginning", "live-beginning"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(iso8601Subscriber.received()).extracting(TestEvent::name).containsExactly("historic-iso", "live-iso"));
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(epochMillisSubscriber.received()).extracting(TestEvent::name).containsExactly("historic-epoch", "live-epoch"));
    }

    // --- inner application and configuration classes ---

    @SpringBootApplication
    @EnableOccurrent
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

        // Appends the events of test.history, which only the context that writes before the application starts is given
        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService, @Value("${test.history:}") List<String> history) {
            return new HistoryAppender(applicationService, history);
        }

        @Bean
        @DependsOn("historyAppender")
        DefaultPositionSubscriber defaultPositionSubscriber() {
            return new DefaultPositionSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        NowPositionSubscriber nowPositionSubscriber() {
            return new NowPositionSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        BeginningOfTimeSubscriber beginningOfTimeSubscriber() {
            return new BeginningOfTimeSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        Iso8601Subscriber iso8601Subscriber() {
            return new Iso8601Subscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        EpochMillisSubscriber epochMillisSubscriber() {
            return new EpochMillisSubscriber();
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
            history.forEach(name -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))));
        }
    }

    // --- subscribers (one per scenario, since the annotation attributes are compile-time constants) ---

    static class DefaultPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger invocationCount = new AtomicInteger();

        @StreamSubscription(id = "sp-default")
        void on(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-default")) {
                received.add(event);
            }
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

        @StreamSubscription(id = "sp-now", startAt = StartPosition.NOW)
        void on(TestEvent event) {
            invocationCount.incrementAndGet();
            if (event.name().equals("live-now")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }

        int invocationCount() {
            return invocationCount.get();
        }
    }

    static class BeginningOfTimeSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "sp-beginning", startAt = StartPosition.BEGINNING_OF_TIME)
        void on(TestEvent event) {
            if (event.name().equals("historic-beginning") || event.name().equals("live-beginning")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class Iso8601Subscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "sp-iso", startAtISO8601 = "2000-01-01T00:00:00Z")
        void on(TestEvent event) {
            if (event.name().equals("historic-iso") || event.name().equals("live-iso")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class EpochMillisSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @StreamSubscription(id = "sp-epoch", startAtTimeEpochMillis = FAR_PAST_EPOCH_MILLIS)
        void on(TestEvent event) {
            if (event.name().equals("historic-epoch") || event.name().equals("live-epoch")) {
                received.add(event);
            }
        }

        List<TestEvent> received() {
            return received;
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
