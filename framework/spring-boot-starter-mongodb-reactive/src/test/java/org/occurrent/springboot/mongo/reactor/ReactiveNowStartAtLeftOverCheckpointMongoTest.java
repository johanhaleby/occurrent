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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.EventWriter;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.RecordingSubscriber;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.TestEvent;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.awaitDelivered;

/**
 * Proves what a reactive subscription that starts at {@code NOW} does when an earlier {@code BEGINNING} start of the same
 * subscription id left a catch-up checkpoint in storage. The first boot replays {@code historic-1} and {@code historic-2},
 * then the test plants the position form catch-up checkpoint a {@code BEGINNING} catch-up stores while it replays, and the
 * second boot starts the subscription again, as {@code NOW}.
 */
@DisplayName("Reactive startAt NOW and a catch-up checkpoint left in storage by an earlier BEGINNING start")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveNowStartAtLeftOverCheckpointMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final ApplicationRestart APPLICATION = new ApplicationRestart(mongoDBContainer, RestartApplication.class, "stream");

    private static final String LEFT_OVER_ID = "reactive-now-left-over";

    @Test
    void now_with_default_resume_behavior_replays_from_the_catch_up_checkpoint_an_earlier_beginning_start_left_in_storage() {
        List<String> handledBySecondBoot = restartWithLeftOverCheckpoint("reactive-now-left-over-now", "now");

        assertReplayedFromTheCheckpoint(handledBySecondBoot);
    }

    @Test
    void default_start_position_replays_from_the_catch_up_checkpoint_an_earlier_beginning_start_left_in_storage() {
        List<String> handledBySecondBoot = restartWithLeftOverCheckpoint("reactive-now-left-over-default", "default");

        assertReplayedFromTheCheckpoint(handledBySecondBoot);
    }

    // Pausing and resuming is not a restart. The subscription is NOW with SAME_AS_START_AT and a catch-up checkpoint is
    // planted while it is paused. A resume does not start over from storage, so historic-2 is not replayed from the
    // planted checkpoint before live-2.
    @Test
    void now_with_same_as_start_at_does_not_replay_from_a_catch_up_checkpoint_planted_while_paused_when_it_is_resumed() {
        try (ConfigurableApplicationContext context = SpringApplication.run(RestartApplication.class,
                bootArgs("reactive-now-left-over-paused", "now-same-as-start-at", "historic-1,historic-2", "live-1"))) {
            LeftOverNowSameAsStartAt handler = context.getBean(LeftOverNowSameAsStartAt.class);
            awaitDelivered(handler, "live-1");
            Checkpoint live = new StringBasedCheckpoint(handler.checkpointOf("live-1"));
            CheckpointStorage storage = context.getBean(CheckpointStorage.class);
            SubscriptionModelLifeCycle subscriptionModel = context.getBean(SubscriptionModelLifeCycle.class);

            subscriptionModel.pauseSubscription(LEFT_OVER_ID);
            await().atMost(ofSeconds(10)).pollInterval(ofMillis(50)).until(() -> subscriptionModel.isPaused(LEFT_OVER_ID));
            storage.save(LEFT_OVER_ID, GlobalCheckpoint.of(1, live, 0, 2)).block();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(LEFT_OVER_ID).block())).as("catch-up checkpoint").isTrue();
            subscriptionModel.resumeSubscription(LEFT_OVER_ID).waitUntilStarted().block(ofSeconds(30));
            @SuppressWarnings("unchecked")
            ApplicationService<TestEvent> applicationService = context.getBean(ApplicationService.class);
            applicationService.execute("live-2", __ -> List.of(new TestEvent("live-2"))).block();
            awaitDelivered(handler, "live-2");

            assertThat(handler.names()).containsExactly("live-1", "live-2");
        }
    }

    // The checkpoint says historic-1 was handled, so the replay starts at historic-2. The live start precedes the event
    // written while down, so the replay reaches it and the live feed can deliver it too, which is why no count is asserted.
    private static void assertReplayedFromTheCheckpoint(List<String> handledBySecondBoot) {
        assertThat(handledBySecondBoot).containsSubsequence("historic-2", "while-down-1", "live-2");
        assertThat(handledBySecondBoot).filteredOn(name -> name.startsWith("historic")).containsExactly("historic-2");
    }

    private static List<String> restartWithLeftOverCheckpoint(String databaseName, String secondBootStartsAt) {
        try (ConfigurableApplicationContext firstBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(databaseName, "beginning", "historic-1,historic-2", ""))) {
            awaitDelivered(firstBoot.getBean(LeftOverBeginning.class), "historic-2");
            CheckpointStorage storage = firstBoot.getBean(CheckpointStorage.class);
            // The catch-up stores a checkpoint of its own while it replays, so wait for the one that carries the handover.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(storage.read(LEFT_OVER_ID).blockOptional().map(Checkpoint::asString)).hasValueSatisfying(value -> assertThat(value).startsWith("position:2;")));
            Checkpoint stored = storage.read(LEFT_OVER_ID).block();
            Checkpoint live = GlobalCheckpoint.isGlobalCheckpoint(stored) ? GlobalCheckpoint.parse(stored).liveFrom().orElseThrow() : stored;
            storage.save(LEFT_OVER_ID, GlobalCheckpoint.of(1, live, 0, 2)).block();
            Checkpoint planted = storage.read(LEFT_OVER_ID).block();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(planted) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(planted)).as("catch-up checkpoint").isTrue();
        }
        try (ConfigurableApplicationContext secondBoot = SpringApplication.run(RestartApplication.class,
                bootArgs(databaseName, secondBootStartsAt, "while-down-1", "live-2"))) {
            Class<? extends RecordingSubscriber> handlerType = secondBootStartsAt.equals("now") ? LeftOverNow.class : LeftOverDefault.class;
            RecordingSubscriber handler = secondBoot.getBean(handlerType);
            awaitDelivered(handler, "live-2");
            return handler.names();
        }
    }

    private static String[] bootArgs(String databaseName, String startsAt, String beforeSubscribe, String afterStart) {
        return Stream.concat(Stream.of(
                        "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                        "--spring.main.web-application-type=none",
                        "--occurrent.event-store.capabilities=stream",
                        "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName,
                        "--test.before-subscribe=" + beforeSubscribe,
                        "--test.after-start=" + afterStart),
                Stream.of("--test.left-over=" + startsAt)).toArray(String[]::new);
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
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-now-left-over");
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(name -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block(), beforeSubscribe, afterStart);
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "beginning")
        LeftOverBeginning leftOverBeginning() {
            return new LeftOverBeginning();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "now")
        LeftOverNow leftOverNow() {
            return new LeftOverNow();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "now-same-as-start-at")
        LeftOverNowSameAsStartAt leftOverNowSameAsStartAt() {
            return new LeftOverNowSameAsStartAt();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "default")
        LeftOverDefault leftOverDefault() {
            return new LeftOverDefault();
        }
    }

    static class LeftOverBeginning extends RecordingSubscriber {
        @Subscription(id = LEFT_OVER_ID, startAt = StartPosition.BEGINNING)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class LeftOverNow extends RecordingSubscriber {
        @Subscription(id = LEFT_OVER_ID, startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class LeftOverNowSameAsStartAt extends RecordingSubscriber {
        @Subscription(id = LEFT_OVER_ID, startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class LeftOverDefault extends RecordingSubscriber {
        @Subscription(id = LEFT_OVER_ID)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }
}
