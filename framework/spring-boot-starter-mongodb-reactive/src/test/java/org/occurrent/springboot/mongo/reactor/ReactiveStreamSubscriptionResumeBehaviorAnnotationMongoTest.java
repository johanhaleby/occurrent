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
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.BootCheck;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.EventWriter;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.FirstBootEnds;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.RecordingSubscriber;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.Recorder;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.TestEvent;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.awaitCheckpointed;
import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.awaitDelivered;

/**
 * Proves that {@code startAt} combined with {@code resumeBehavior} on the reactive {@link StreamSubscription} behaves
 * as documented across an actual application restart: the context is closed and a fresh one is booted against the
 * same MongoDB data, with events written while nothing is subscribed. Mirrors the blocking
 * {@code StreamSubscriptionResumeBehaviorAnnotationMongoTest}.
 * <p>
 * The boots are run by {@link ApplicationRestart}, which also explains the events each of them writes.
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
        restart("reactive-resume-now-same", NowSameAsStartAt.class, FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED,
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

    // The subscription id doubles as the name of the database, so each scenario starts from an empty one.
    private static final ApplicationRestart APPLICATION = new ApplicationRestart(mongoDBContainer, RestartApplication.class, "stream");

    private static <S extends Recorder> void restart(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                                     List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        APPLICATION.run(subscriptionId, subscriberType, firstBootEnds, expectedFirstBoot, expectedSecondBoot);
    }

    private static <S extends Recorder> void restart(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                                     List<String> expectedFirstBoot, List<String> expectedSecondBoot,
                                                     Checkpoints checkpoints) {
        BootCheck bootCheck = checkpoints == Checkpoints.NONE_STORED
                ? (context, marker) -> assertNoCheckpointStored(context, subscriptionId, marker)
                : ApplicationRestart.NOTHING;
        APPLICATION.run(subscriptionId, subscriberType, firstBootEnds, expectedFirstBoot, expectedSecondBoot, bootCheck);
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
        // A wrong write can land after the sibling's, so the absence has to hold for a while and not only at one read.
        await().during(ofSeconds(1)).atMost(ofSeconds(5)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(storage.read(subscriptionId).blockOptional())
                        .describedAs("the checkpoint stored for %s, a start that is dynamic for the durable layer stores none", subscriptionId)
                        .isEmpty());
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
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-stream-resume-behavior");
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(name -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block(), beforeSubscribe, afterStart);
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
}
