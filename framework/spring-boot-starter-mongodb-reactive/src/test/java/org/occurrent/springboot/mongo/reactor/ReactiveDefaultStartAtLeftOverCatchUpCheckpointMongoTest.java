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
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.EventWriter;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.Recorder;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.RecordingSubscriber;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.TestEvent;
import org.occurrent.springboot.reactor.ComposedCatchupModel;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.ObjectProvider;
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

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The reactive control for the blocking {@code DefaultStartAtLeftOverCatchUpCheckpointMongoTest}. A reactive
 * {@code @Subscription} with {@code startAt} unset resolves the stored checkpoint to a plain {@code StartAt.checkpoint}
 * that the catch-up layer replays from, so what was written while the application was down is delivered.
 * <p>
 * The first boot runs the id at {@code BEGINNING}. The replay delivers {@code historic-1} and {@code historic-2}, and
 * {@code live-1} is written once the replay is over, so it is delivered live and the checkpoint stored with it is a
 * MongoDB resume token. A catch-up checkpoint with that token as its live start is written in place of the stored
 * one. The second boot runs the same id with {@code startAt} unset, with {@code while-down-1} written before it
 * subscribes and {@code live-2} after it started.
 */
@DisplayName("Reactive DEFAULT start position with a catch-up checkpoint in storage")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveDefaultStartAtLeftOverCatchUpCheckpointMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final String ID = "reactive-default-left-over";
    private static final String DATABASE = "reactive-default-left-over";

    @Test
    void subscription_without_start_at_is_delivered_what_was_written_while_it_was_down_when_a_catch_up_checkpoint_is_stored() {
        try (ConfigurableApplicationContext firstBoot = SpringApplication.run(RestartApplication.class,
                bootArgs("historic-1,historic-2", "live-1", "beginning"))) {
            Recorder subscriber = firstBoot.getBean(SubscriptionBeginning.class);
            ApplicationRestart.awaitDelivered(subscriber, "live-1");
            ApplicationRestart.awaitCheckpointed(firstBoot, ID, subscriber, "live-1");
            CheckpointStorage storage = firstBoot.getBean(CheckpointStorage.class);
            Checkpoint live = storage.read(ID).block();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(live)).as("live-1 is delivered live, so its checkpoint is a resume token").isFalse();
            // historic-2 is global position 2
            storage.save(ID, GlobalCheckpoint.of(2, live, 0, 2)).block();
            Checkpoint stored = storage.read(ID).block();
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(stored) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(stored)).as("catch-up checkpoint").isTrue();
        }

        try (ConfigurableApplicationContext secondBoot = SpringApplication.run(RestartApplication.class,
                bootArgs("while-down-1", "live-2", "default"))) {
            Recorder subscriber = secondBoot.getBean(SubscriptionDefault.class);
            ApplicationRestart.awaitDelivered(subscriber, "live-2");

            // The live start the first boot stored precedes live-1, so live-1 may be delivered again and is not asserted.
            assertThat(subscriber.names()).filteredOn(name -> !name.equals("live-1"))
                    .containsExactly("while-down-1", "live-2");
        }
    }

    private static String[] bootArgs(String beforeSubscribe, String afterStart, String leftOver) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(DATABASE),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=stream",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + DATABASE,
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart,
                "--test.left-over=" + leftOver
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
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-default-left-over");
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart,
                                ObjectProvider<SubscriptionBeginning> beginning,
                                ObjectProvider<ComposedCatchupModel> composedCatchupModel,
                                ObjectProvider<FluxSubscriptionModel> subscriptionModel) {
            // An event written while the replay still runs can be delivered by the replay, which hands it over with a
            // GlobalCheckpoint. live-1 is written once the replay is over so that it arrives live.
            Runnable replayIsOver = () -> {
                SubscriptionBeginning replaying = beginning.getIfAvailable();
                if (replaying == null) {
                    return;
                }
                String lastHistory = List.of(beforeSubscribe.split(",")).getLast();
                ReplayAwareSubscriptions replayAware = composedCatchupModel.getObject().catchupModelFor((SubscriptionModelCapability) subscriptionModel.getObject()).orElseThrow();
                await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).untilAsserted(() -> {
                    assertThat(replaying.names()).contains(lastHistory);
                    assertThat(replayAware.isCatchingUp(ID)).isFalse();
                });
            };
            return new EventWriter(name -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block(), beforeSubscribe, afterStart, replayIsOver);
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "beginning")
        SubscriptionBeginning subscriptionBeginning() {
            return new SubscriptionBeginning();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "default")
        SubscriptionDefault subscriptionDefault() {
            return new SubscriptionDefault();
        }
    }

    static class SubscriptionBeginning extends RecordingSubscriber {
        @Subscription(id = ID, startAt = StartPosition.BEGINNING)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class SubscriptionDefault extends RecordingSubscriber {
        @Subscription(id = ID)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }
}
