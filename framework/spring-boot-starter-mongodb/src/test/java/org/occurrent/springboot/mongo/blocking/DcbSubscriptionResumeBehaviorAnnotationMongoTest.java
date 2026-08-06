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
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
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
 * Proves that {@code resumeBehavior} on {@link DcbSubscription} behaves differently across an actual application
 * restart, not just a pause. The annotation processor computes the {@code DcbStartAt} once, when the subscriber bean
 * is post-processed at startup, and that computation is a {@code DcbStartAt.dynamic(...)} supplier evaluated exactly
 * once inside the subscription model's {@code subscribe(...)} call. Pausing and resuming the same running model
 * (stop/start) never re-invokes that supplier, so {@code resumeBehavior} can only be observed by closing the Spring
 * context and booting a fresh one against the same durable MongoDB backing data, exactly like a real process restart.
 * <p>
 * Both boots share one MongoDB replica set but use isolated collections per scenario (see {@code bootArgs}), so the
 * three scenarios below can run against the same container without interfering with each other despite the fixed,
 * compile-time-constant annotation attributes forcing one subscriber class per scenario.
 */
@DisplayName("DcbSubscription resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class DcbSubscriptionResumeBehaviorAnnotationMongoTest {

    // Booted directly with SpringApplication.run (not @SpringBootTest), so there is no @ServiceConnection to resolve
    // the container for the context. The url is passed as a boot argument instead, and getReplicaSetUrl() reports the
    // mapped host port, so nothing has to be pinned.
    @Container
    static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @Test
    void default_resume_behavior_resumes_from_the_stored_position_after_restart_while_same_as_start_at_replays_from_the_beginning_again() {
        String[] args = bootArgs("dcb-resume-behavior");

        ConfigurableApplicationContext ctx1 = SpringApplication.run(FirstBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber1 = ctx1.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtBeginningSubscriber sameAsStartAtSubscriber1 = ctx1.getBean(SameAsStartAtBeginningSubscriber.class);
            SameAsStartAtPositionSubscriber positionSubscriber1 = ctx1.getBean(SameAsStartAtPositionSubscriber.class);

            // First boot: every scenario replays the same pre-existing history, each per its own start position.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(positionSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-2"));
        } finally {
            ctx1.close();
        }

        // The application is "down": a new event is appended while nothing is subscribed.
        ConfigurableApplicationContext ctx2 = SpringApplication.run(SecondBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber2 = ctx2.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtBeginningSubscriber sameAsStartAtSubscriber2 = ctx2.getBean(SameAsStartAtBeginningSubscriber.class);
            SameAsStartAtPositionSubscriber positionSubscriber2 = ctx2.getBean(SameAsStartAtPositionSubscriber.class);

            // DEFAULT: the fresh subscriber instance resumes from the durably stored position, it never sees the
            // historic events again, only the one event appended while the application was down.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));

            // SAME_AS_START_AT (BEGINNING): the fresh subscriber instance replays the entire history again, from
            // the beginning, exactly as it did on the first boot, plus the new event.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber2.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "while-down-1"));

            // SAME_AS_START_AT (explicit startAtDcbPosition): replays again from that same position, not from
            // stored progress, so it sees the same tail of history plus the new event.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(positionSubscriber2.received()).extracting(TestEvent::name).containsExactly("historic-2", "while-down-1"));
        } finally {
            ctx2.close();
        }
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    // --- application boot classes ---

    @SpringBootApplication
    @EnableOccurrent
    static class FirstBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the two historic events before any subscriber starts, so BEGINNING-based subscriptions replay them.
        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        SameAsStartAtBeginningSubscriber sameAsStartAtBeginningSubscriber() {
            return new SameAsStartAtBeginningSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        SameAsStartAtPositionSubscriber sameAsStartAtPositionSubscriber() {
            return new SameAsStartAtPositionSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SecondBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the "while the application was down" event before any subscriber (re)starts.
        @Bean
        OfflineAppender offlineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new OfflineAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("offlineAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        SameAsStartAtBeginningSubscriber sameAsStartAtBeginningSubscriber() {
            return new SameAsStartAtBeginningSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        SameAsStartAtPositionSubscriber sameAsStartAtPositionSubscriber() {
            return new SameAsStartAtPositionSubscriber();
        }
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:dcb-resume-behavior"))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    // --- appenders ---

    static class HistoryAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        HistoryAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendHistory() {
            append(dcbEventStore, cloudEventConverter, new TestEvent("historic-1"), new TestEvent("historic-2"));
        }
    }

    static class OfflineAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        OfflineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendWhileDown() {
            append(dcbEventStore, cloudEventConverter, new TestEvent("while-down-1"));
        }
    }

    private static void append(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<io.cloudevents.CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse("test:resume-behavior"))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- subscribers (one per scenario, since the annotation attributes are compile-time constants) ---

    static class DefaultResumeSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-resume-default", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.DEFAULT)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class SameAsStartAtBeginningSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-resume-same-as-start-at-beginning", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class SameAsStartAtPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // startAtDcbPosition = 1 means: deliver from position 2 onward, i.e. skip "historic-1".
        @DcbSubscription(id = "dcb-resume-same-as-start-at-position", startAtDcbPosition = 1, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void onEvent(TestEvent event) {
            received.add(event);
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
