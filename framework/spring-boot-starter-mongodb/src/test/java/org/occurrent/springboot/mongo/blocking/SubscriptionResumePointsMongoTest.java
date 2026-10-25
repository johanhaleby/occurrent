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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.common.SubscriptionResumePoints;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Where an annotated stream-store subscription starts when the application declares a {@link SubscriptionResumePoints}
 * bean, whatever start position the annotation gives. The checkpoints the bean answers are the ones a replaying
 * subscription delivered with its events in an earlier process, which is how an application gets hold of one.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(240)
class SubscriptionResumePointsMongoTest {

    private static final String RESUMED_NOW = "resumed-now";
    private static final String RESUMED_DEFAULT = "resumed-default";
    private static final String RESUMED_BEGINNING = "resumed-beginning";
    private static final String RESUMED_SAME_AS_START_AT = "resumed-same-as-start-at";
    private static final String CONTROL = "control";

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    enum Annotated {
        NOW("now", RESUMED_NOW, List.of()),
        DEFAULT("default", RESUMED_DEFAULT, List.of()),
        BEGINNING("beginning", RESUMED_BEGINNING, List.of("history-1", "history-2", "history-3"));

        final String subscriber;
        final String subscriptionId;
        final List<String> deliveredFromItsOwnAttributes;

        Annotated(String subscriber, String subscriptionId, List<String> deliveredFromItsOwnAttributes) {
            this.subscriber = subscriber;
            this.subscriptionId = subscriptionId;
            this.deliveredFromItsOwnAttributes = deliveredFromItsOwnAttributes;
        }
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void an_answered_checkpoint_starts_the_subscription_after_the_event_it_was_delivered_with_whatever_start_the_annotation_gives(Annotated annotated) {
        // Given
        String database = newDatabase();
        Map<String, String> checkpoints = aReplayDeliversTheCheckpointsOf(database, "history-1", "history-2", "history-3");

        // When
        ConfigurableApplicationContext ctx = start(database, annotated.subscriber, "", checkpoints.get("history-1"));
        try {
            Recording recording = ctx.getBean(Recording.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the replay from the answer delivered").containsExactly("history-2", "history-3"));
            append(ctx, "live-1");

            // Then
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the subscription delivered").containsExactly("history-2", "history-3", "live-1"));
        } finally {
            ctx.close();
        }
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void a_bean_that_answers_null_leaves_the_subscription_to_its_own_start_attributes(Annotated annotated) {
        // Given
        String database = newDatabase();
        aReplayDeliversTheCheckpointsOf(database, "history-1", "history-2", "history-3");

        // When
        ConfigurableApplicationContext ctx = start(database, annotated.subscriber, "", null);
        try {
            Recording recording = ctx.getBean(Recording.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the start attributes deliver").containsExactlyElementsOf(annotated.deliveredFromItsOwnAttributes));
            append(ctx, "live-1");

            // Then
            List<String> expected = new ArrayList<>(annotated.deliveredFromItsOwnAttributes);
            expected.add("live-1");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the subscription delivered").containsExactlyElementsOf(expected));
        } finally {
            ctx.close();
        }
    }

    @ParameterizedTest
    @EnumSource(Annotated.class)
    void without_the_bean_the_subscription_starts_from_its_own_start_attributes(Annotated annotated) {
        // Given
        String database = newDatabase();
        aReplayDeliversTheCheckpointsOf(database, "history-1", "history-2", "history-3");

        // When
        ConfigurableApplicationContext ctx = startWithoutTheBean(database, annotated.subscriber, "");
        try {
            Recording recording = ctx.getBean(Recording.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the start attributes deliver").containsExactlyElementsOf(annotated.deliveredFromItsOwnAttributes));
            append(ctx, "live-1");

            // Then
            List<String> expected = new ArrayList<>(annotated.deliveredFromItsOwnAttributes);
            expected.add("live-1");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the subscription delivered").containsExactlyElementsOf(expected));
        } finally {
            ctx.close();
        }
    }

    @Test
    void an_answered_checkpoint_wins_over_the_checkpoint_already_stored_in_checkpoint_storage_for_the_subscription() {
        // Given
        String database = newDatabase();
        Map<String, String> checkpoints = aReplayDeliversTheCheckpointsOf(database, "history-1", "history-2", "history-3");
        ConfigurableApplicationContext first = startWithoutTheBean(database, Annotated.DEFAULT.subscriber, "");
        try {
            append(first, "live-1");
            append(first, "live-2");
            Recording firstRecording = first.getBean(Recording.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(firstRecording.names()).as("what the first process delivered").containsExactly("live-1", "live-2"));
            CheckpointStorage storage = first.getBean(CheckpointStorage.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(RESUMED_DEFAULT)).as("the checkpoint the first process stored for the subscription").isNotNull());
        } finally {
            first.close();
        }

        // When
        ConfigurableApplicationContext second = start(database, Annotated.DEFAULT.subscriber, "live-3", checkpoints.get("history-1"));
        try {
            Recording recording = second.getBean(Recording.class);

            // Then
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the second process delivered, every event after the answered checkpoint once")
                            .containsExactly("history-2", "history-3", "live-1", "live-2", "live-3"));
        } finally {
            second.close();
        }
    }

    @Test
    void with_same_as_start_at_an_answer_changes_only_where_the_replay_starts_and_the_subscription_stays_out_of_checkpoint_storage_and_competition() {
        // Given
        String database = newDatabase();
        Map<String, String> checkpoints = aReplayDeliversTheCheckpointsOf(database, "history-1", "history-2", "history-3");

        // When
        ConfigurableApplicationContext ctx = start(database, "same-as-start-at," + CONTROL, "", checkpoints.get("history-1"));
        try {
            Recording recording = ctx.getBean(Recording.class);
            CheckpointStorage storage = ctx.getBean(CheckpointStorage.class);
            GrantedCompetingConsumers granted = ctx.getBean(GrantedCompetingConsumers.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.namesOf(RESUMED_SAME_AS_START_AT)).as("what the replay from the answer delivered").containsExactly("history-2", "history-3"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(granted.subscriptions()).as("the subscriptions that compete, the control among them so that an empty answer means something").contains(CONTROL));
            append(ctx, "live-1");

            // Then
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.namesOf(RESUMED_SAME_AS_START_AT)).as("what the subscription delivered").containsExactly("history-2", "history-3", "live-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(RESUMED_SAME_AS_START_AT)).as("the checkpoint stored for the subscription").isNull());
            assertThat(granted.subscriptions()).as("the subscriptions that compete").doesNotContain(RESUMED_SAME_AS_START_AT);
        } finally {
            ctx.close();
        }
    }

    // A process that replays a history in which every event hands over the checkpoint it was delivered with
    private static Map<String, String> aReplayDeliversTheCheckpointsOf(String database, String... names) {
        ConfigurableApplicationContext ctx = startWithoutTheBean(database, "recorder", String.join(",", names));
        try {
            Recording recording = ctx.getBean(Recording.class);
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(recording.names()).as("what the replay delivered").containsExactly(names));
            Map<String, String> checkpoints = recording.checkpoints();
            assertThat(checkpoints).as("the checkpoint each replayed event was delivered with").containsOnlyKeys(names);
            assertThat(checkpoints.values()).as("the checkpoints").doesNotContain("");
            return checkpoints;
        } finally {
            ctx.close();
        }
    }

    private static ConfigurableApplicationContext start(String database, String subscribers, String history, String resumePoint) {
        String[] arguments = resumePoint == null
                ? new String[]{"--test.resume-points=true"}
                : new String[]{"--test.resume-points=true", "--test.resume-point=" + resumePoint};
        return run(database, subscribers, history, arguments);
    }

    private static ConfigurableApplicationContext startWithoutTheBean(String database, String subscribers, String history) {
        return run(database, subscribers, history);
    }

    private static ConfigurableApplicationContext run(String database, String subscribers, String history, String... more) {
        List<String> arguments = new ArrayList<>(List.of(
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(database),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + database,
                "--test.subscribers=" + subscribers,
                "--test.history=" + history));
        arguments.addAll(List.of(more));
        return SpringApplication.run(ResumeApplication.class, arguments.toArray(String[]::new));
    }

    private static String newDatabase() {
        return "resume-points-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void append(ConfigurableApplicationContext ctx, String name) {
        ApplicationService<TestEvent> applicationService = ctx.getBean(ApplicationService.class);
        applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name)));
    }

    @SpringBootApplication
    @EnableOccurrent
    static class ResumeApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:resume-points"))
                    .typeMapper(typeMapper)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        // Answers the checkpoint given on the command line for every resumed subscription, and nothing for any other
        @Bean
        @ConditionalOnProperty(name = "test.resume-points", havingValue = "true")
        SubscriptionResumePoints resumePoints(Environment environment) {
            return subscriptionId -> subscriptionId.startsWith("resumed") ? environment.getProperty("test.resume-point") : null;
        }

        @Bean
        GrantedCompetingConsumers grantedCompetingConsumers() {
            return new GrantedCompetingConsumers();
        }

        @Bean
        Recording recording() {
            return new Recording();
        }

        // Appends the history before any subscriber starts, so that the subscriptions replay it
        @Bean
        HistoryAppender historyAppender(ApplicationService<TestEvent> applicationService, Environment environment) {
            return new HistoryAppender(applicationService, environment.getProperty("test.history", ""));
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('recorder')")
        RecorderSubscriber recorderSubscriber(Recording recording) {
            return new RecorderSubscriber(recording);
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('now')")
        NowSubscriber nowSubscriber(Recording recording) {
            return new NowSubscriber(recording);
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('default')")
        DefaultSubscriber defaultSubscriber(Recording recording) {
            return new DefaultSubscriber(recording);
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('beginning')")
        BeginningSubscriber beginningSubscriber(Recording recording) {
            return new BeginningSubscriber(recording);
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('same-as-start-at')")
        SameAsStartAtSubscriber sameAsStartAtSubscriber(Recording recording) {
            return new SameAsStartAtSubscriber(recording);
        }

        @Bean
        @DependsOn("historyAppender")
        @ConditionalOnExpression("T(java.util.Arrays).asList('${test.subscribers}'.split(',')).contains('control')")
        ControlSubscriber controlSubscriber(Recording recording) {
            return new ControlSubscriber(recording);
        }
    }

    static class HistoryAppender {
        private final ApplicationService<TestEvent> applicationService;
        private final String history;

        HistoryAppender(ApplicationService<TestEvent> applicationService, String history) {
            this.applicationService = applicationService;
            this.history = history;
        }

        @PostConstruct
        void appendHistory() {
            if (!history.isBlank()) {
                for (String name : history.split(",")) {
                    applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name)));
                }
            }
        }
    }

    static class GrantedCompetingConsumers implements CompetingConsumerListener {
        private final CopyOnWriteArrayList<String> subscriptions = new CopyOnWriteArrayList<>();

        @Override
        public void onConsumeGranted(String subscriptionId, String subscriberId) {
            subscriptions.add(subscriptionId);
        }

        List<String> subscriptions() {
            return subscriptions;
        }
    }

    static class Recording {
        private final CopyOnWriteArrayList<String> names = new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<String[]> bySubscription = new CopyOnWriteArrayList<>();
        private final Map<String, String> checkpoints = new ConcurrentHashMap<>();

        void record(String subscriptionId, TestEvent event, EventMetadata metadata) {
            names.add(event.name());
            bySubscription.add(new String[]{subscriptionId, event.name()});
            checkpoints.put(event.name(), metadata.getCheckpoint().orElse(""));
        }

        List<String> names() {
            return names;
        }

        List<String> namesOf(String subscriptionId) {
            return bySubscription.stream().filter(entry -> entry[0].equals(subscriptionId)).map(entry -> entry[1]).toList();
        }

        Map<String, String> checkpoints() {
            return Map.copyOf(checkpoints);
        }
    }

    static class RecorderSubscriber {
        private final Recording recording;

        RecorderSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = "recorder", startAt = StartPosition.BEGINNING)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record("recorder", event, metadata);
        }
    }

    static class NowSubscriber {
        private final Recording recording;

        NowSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = RESUMED_NOW, startAt = StartPosition.NOW)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record(RESUMED_NOW, event, metadata);
        }
    }

    static class DefaultSubscriber {
        private final Recording recording;

        DefaultSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = RESUMED_DEFAULT, startAt = StartPosition.DEFAULT)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record(RESUMED_DEFAULT, event, metadata);
        }
    }

    static class BeginningSubscriber {
        private final Recording recording;

        BeginningSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = RESUMED_BEGINNING, startAt = StartPosition.BEGINNING)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record(RESUMED_BEGINNING, event, metadata);
        }
    }

    static class SameAsStartAtSubscriber {
        private final Recording recording;

        SameAsStartAtSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = RESUMED_SAME_AS_START_AT, startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record(RESUMED_SAME_AS_START_AT, event, metadata);
        }
    }

    static class ControlSubscriber {
        private final Recording recording;

        ControlSubscriber(Recording recording) {
            this.recording = recording;
        }

        @Subscription(id = CONTROL, startAt = StartPosition.NOW)
        void on(TestEvent event, EventMetadata metadata) {
            recording.record(CONTROL, event, metadata);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
