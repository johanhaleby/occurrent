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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code startAt} combined with {@code resumeBehavior} on the reactive {@link StreamSubscription} behaves
 * as documented across an actual application restart: the context is closed and a fresh one is booted against the
 * same MongoDB data, with events written while nothing is subscribed. Mirrors the blocking
 * {@code StreamSubscriptionResumeBehaviorAnnotationMongoTest}.
 * <p>
 * Each boot writes the events named by {@code test.before-subscribe} before any subscription registers and, from an
 * {@link ApplicationRunner}, the events named by {@code test.after-start}. The last of those is a marker: it is
 * written after everything else, so once a subscriber has received it, whatever it was going to deliver for the
 * events before it has arrived and the whole delivered list can be asserted.
 */
@DisplayName("Reactive StreamSubscription startAt and resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveStreamSubscriptionResumeBehaviorAnnotationMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void beginning_of_time_with_default_resume_behavior_replays_history_once_and_resumes_from_the_stored_position_after_restart() {
        restart("reactive-resume-bot-default", BeginningOfTimeDefault.class, FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("historic-1", "historic-2", "live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void beginning_of_time_with_same_as_start_at_replays_the_whole_history_again_after_restart() {
        restart("reactive-resume-bot-same", BeginningOfTimeSameAsStartAt.class, FirstBootEnds.WHEN_THE_MARKER_IS_DELIVERED,
                List.of("historic-1", "historic-2", "live-1"),
                List.of("historic-1", "historic-2", "live-1", "while-down-1", "live-2"),
                Checkpoints.NONE_STORED);
    }

    @Test
    void now_with_default_resume_behavior_ignores_history_on_first_boot_and_receives_events_written_while_down_after_restart() {
        restart("reactive-resume-now-default", NowDefault.class, FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void now_with_default_resume_behavior_receives_events_written_while_down_after_a_first_boot_that_delivered_nothing() {
        restart("reactive-resume-now-default-nothing-delivered", NowDefaultNothingDelivered.class, FirstBootEnds.WHEN_THE_INITIAL_CHECKPOINT_IS_STORED,
                List.of(),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void now_with_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        restart("reactive-resume-now-same", NowSameAsStartAt.class, FirstBootEnds.WHEN_THE_MARKER_IS_DELIVERED,
                List.of("live-1"),
                List.of("live-2"));
    }

    @Test
    void plain_defaults_ignore_history_on_first_boot_and_resume_from_the_stored_position_after_restart() {
        restart("reactive-resume-plain-defaults", PlainDefaults.class, FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    // Whether a boot also asserts that the subscription has no checkpoint stored. Only a start that is dynamic for the
    // durable layer, BEGINNING_OF_TIME with SAME_AS_START_AT, stores none. A NOW start does store its first position.
    private enum Checkpoints {
        NOT_ASSERTED,
        NONE_STORED
    }

    // What has to have happened before the first boot is closed. A subscription that resumes from a stored position
    // resumes from whatever was stored when the context closed, and the position of an event is stored after its
    // handler returned, so closing on the delivery alone races the store.
    private enum FirstBootEnds {
        // The marker reached the handler, which is all a subscription that does not resume from a stored position needs.
        WHEN_THE_MARKER_IS_DELIVERED,
        // The marker reached the handler and its checkpoint is what the checkpoint storage holds.
        WHEN_THE_MARKER_IS_CHECKPOINTED,
        // No event is written after the subscription starts, so there is no marker. The initial checkpoint is what the
        // checkpoint storage holds, which is later than the subscription being registered as running.
        WHEN_THE_INITIAL_CHECKPOINT_IS_STORED
    }

    // The subscription id doubles as the name of the database, so each scenario starts from an empty one.
    private static <S extends RecordingSubscriber> void restart(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                                                List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        restart(subscriptionId, subscriberType, firstBootEnds, expectedFirstBoot, expectedSecondBoot, Checkpoints.NOT_ASSERTED);
    }

    private static <S extends RecordingSubscriber> void restart(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                                                List<String> expectedFirstBoot, List<String> expectedSecondBoot,
                                                                Checkpoints checkpoints) {
        String firstBootAfterStart = firstBootEnds == FirstBootEnds.WHEN_THE_INITIAL_CHECKPOINT_IS_STORED ? "" : "live-1";
        ConfigurableApplicationContext firstBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(subscriptionId, "historic-1,historic-2", firstBootAfterStart));
        try {
            S subscriber = firstBoot.getBean(subscriberType);
            switch (firstBootEnds) {
                case WHEN_THE_MARKER_IS_DELIVERED -> awaitDelivered(subscriber, "live-1");
                case WHEN_THE_MARKER_IS_CHECKPOINTED -> {
                    awaitDelivered(subscriber, "live-1");
                    awaitCheckpointed(firstBoot, subscriptionId, subscriber, "live-1");
                }
                case WHEN_THE_INITIAL_CHECKPOINT_IS_STORED -> awaitInitialCheckpoint(firstBoot, subscriptionId);
            }
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedFirstBoot);
            if (checkpoints == Checkpoints.NONE_STORED) {
                assertNoCheckpointStored(firstBoot, subscriptionId, "live-1");
            }
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(subscriptionId, "while-down-1", "live-2"));
        try {
            S subscriber = secondBoot.getBean(subscriberType);
            awaitDelivered(subscriber, "live-2");
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedSecondBoot);
            if (checkpoints == Checkpoints.NONE_STORED) {
                assertNoCheckpointStored(secondBoot, subscriptionId, "live-2");
            }
        } finally {
            secondBoot.close();
        }
    }

    private static void awaitDelivered(RecordingSubscriber subscriber, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(subscriber.names()).contains(marker));
    }

    // The durable model registers the subscription as running before it writes the first position, so running does not
    // mean the position a restart resumes from is stored.
    private static void awaitInitialCheckpoint(ConfigurableApplicationContext context, String subscriptionId) {
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(storage.read(subscriptionId).blockOptional()).isPresent());
    }

    // The checkpoint is stored after the handler returned, so the marker being delivered does not mean it is stored.
    private static void awaitCheckpointed(ConfigurableApplicationContext context, String subscriptionId, RecordingSubscriber subscriber, String marker) {
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        String markerCheckpoint = subscriber.checkpointOf(marker);
        assertThat(markerCheckpoint).describedAs("the checkpoint %s was delivered with", marker).isNotNull();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(storage.read(subscriptionId).blockOptional().map(Checkpoint::asString)).contains(markerCheckpoint));
    }

    // A negative assertion passes just as well when nothing has been stored yet, so it has to be made after a store would
    // have happened. Every subscriber bean runs in every context against the same database, and the control one,
    // BEGINNING_OF_TIME with DEFAULT, receives the same marker and does store the position of it. Once that checkpoint
    // is there the checkpoint storage has demonstrably been written for this marker, and the absence is not a missed race.
    private static void assertNoCheckpointStored(ConfigurableApplicationContext context, String subscriptionId, String marker) {
        BeginningOfTimeDefault control = context.getBean(BeginningOfTimeDefault.class);
        awaitDelivered(control, marker);
        awaitCheckpointed(context, "reactive-resume-bot-default", control, marker);
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        assertThat(storage.read(subscriptionId).blockOptional())
                .describedAs("the checkpoint stored for %s, a start that is dynamic for the durable layer stores none", subscriptionId)
                .isEmpty();
    }

    // Booted with SpringApplication.run rather than @SpringBootTest, so there is no @ServiceConnection and the url is
    // passed as an argument.
    private static String[] bootArgs(String databaseName, String beforeSubscribe, String afterStart) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName,
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart
        };
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class RestartApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:reactive-stream-resume-behavior"))
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(applicationService, beforeSubscribe, afterStart);
        }

        @Bean
        BeginningOfTimeDefault beginningOfTimeDefault() {
            return new BeginningOfTimeDefault();
        }

        @Bean
        BeginningOfTimeSameAsStartAt beginningOfTimeSameAsStartAt() {
            return new BeginningOfTimeSameAsStartAt();
        }

        @Bean
        NowDefault nowDefault() {
            return new NowDefault();
        }

        @Bean
        NowDefaultNothingDelivered nowDefaultNothingDelivered() {
            return new NowDefaultNothingDelivered();
        }

        @Bean
        NowSameAsStartAt nowSameAsStartAt() {
            return new NowSameAsStartAt();
        }

        @Bean
        PlainDefaults plainDefaults() {
            return new PlainDefaults();
        }
    }

    static class EventWriter implements ApplicationRunner {
        private final ApplicationService<TestEvent> applicationService;
        private final List<String> beforeSubscribe;
        private final List<String> afterStart;

        EventWriter(ApplicationService<TestEvent> applicationService, String beforeSubscribe, String afterStart) {
            this.applicationService = applicationService;
            this.beforeSubscribe = List.of(beforeSubscribe.split(","));
            this.afterStart = afterStart.isEmpty() ? List.of() : List.of(afterStart.split(","));
        }

        // Subscriptions register once every singleton exists, so these are written before any of them starts. A
        // subscription starting at the present can still receive what the same MongoClient wrote up to a second
        // earlier, so the pause keeps them out of reach of the NOW subscribers.
        @PostConstruct
        void writeBeforeSubscribe() throws InterruptedException {
            beforeSubscribe.forEach(this::write);
            Thread.sleep(1500);
        }

        @Override
        public void run(ApplicationArguments args) {
            afterStart.forEach(this::write);
        }

        private void write(String name) {
            applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block();
        }
    }

    abstract static class RecordingSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final ConcurrentHashMap<String, String> checkpoints = new ConcurrentHashMap<>();

        Mono<Void> record(TestEvent event, EventMetadata metadata) {
            metadata.getCheckpoint().ifPresent(checkpoint -> checkpoints.put(event.name(), checkpoint));
            received.add(event);
            return Mono.empty();
        }

        @Nullable String checkpointOf(String name) {
            return checkpoints.get(name);
        }

        List<String> names() {
            return received.stream().map(TestEvent::name).toList();
        }
    }

    static class BeginningOfTimeDefault extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-bot-default", startAt = StartPosition.BEGINNING_OF_TIME, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class BeginningOfTimeSameAsStartAt extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-bot-same", startAt = StartPosition.BEGINNING_OF_TIME, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class NowDefault extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class NowDefaultNothingDelivered extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-now-default-nothing-delivered", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class NowSameAsStartAt extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-now-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class PlainDefaults extends RecordingSubscriber {
        @StreamSubscription(id = "reactive-resume-plain-defaults")
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
