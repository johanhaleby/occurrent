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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A live model that opens asynchronously can lose the live start a DCB replay handed over. The catch-up replays again
 * instead of leaving the subscription to the live model, which skips what was appended in between or forgets it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupLostLiveStartTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    static Stream<Arguments> everyCombination() {
        return Stream.of(true, false).flatMap(restart ->
                Stream.of(true, false).flatMap(storing ->
                        Stream.of(true, false).map(async -> Arguments.of(restart, storing, async))));
    }

    @ParameterizedTest(name = "restartOnHistoryLost={0}, storing={1}, opensAsynchronously={2}")
    @MethodSource("everyCombination")
    @Timeout(60)
    void a_live_start_the_live_model_loses_as_it_opens_replays_again_and_delivers_what_was_appended_in_between(boolean restartOnHistoryLost, boolean storing, boolean opensAsynchronously) {
        List<NameDefined> history = List.of(nameDefined("e1"), nameDefined("e2"), nameDefined("e3"));
        history.forEach(event -> appendTagged("name:1", event));
        NameDefined appendedInTheGap = nameDefined("appendedInTheGap");
        LiveStartLosingSubscriptionModel liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, restartOnHistoryLost, !opensAsynchronously, () -> appendTagged("name:1", appendedInTheGap));
        CopyOnWriteArrayList<Checkpoint> stored = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                stored.add(checkpoint);
                return super.save(id, checkpoint, condition);
            }
        };
        CatchupSubscriptionModelConfig config = storing
                ? new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))
                : new CatchupSubscriptionModelConfig(100);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
        DcbCatchupSubscriptionModel subscription = new DcbCatchupSubscriptionModel(liveModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")), config);
        subscription.listenForCatchup("subscription", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("catchupStarted");
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
            }
        });
        try {
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

            assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).as("started").isTrue();
            assertThat(received).as("the history, and then all of it again together with what was appended in between")
                    .containsExactlyElementsOf(Stream.concat(history.stream(), Stream.concat(history.stream(), Stream.of(appendedInTheGap))).toList());
            assertThat(liveModel.lostLiveStarts).as("the live starts the live model lost").hasSize(1);
            assertThat(liveModel.openedAt).as("the live starts the live model opened").hasSize(1).doesNotContainAnyElementsOf(liveModel.lostLiveStarts);
            assertAll(
                    () -> assertThat(liveModel.readLiveStarts).as("the live starts read").containsExactly(liveModel.lostLiveStarts.get(0), liveModel.openedAt.get(0)),
                    () -> assertThat(liveModel.restartedFromThePresent).as("restarts from the present").isEmpty(),
                    () -> assertThat(signals).as("the catch-up starts and has read its history once for each replay")
                            .containsExactly("catchupStarted", "historyRead", "catchupStarted", "historyRead"),
                    () -> assertThat(subscription.isRunning("subscription")).as("running").isTrue(),
                    () -> assertThat(subscription.isPaused("subscription")).as("paused").isFalse()
            );
            if (storing) {
                assertAll(
                        () -> assertThat(storage.read("subscription")).as("the stored position is the live start that was handed over last").isEqualTo(liveModel.openedAt.get(0)),
                        () -> assertThat(stored).as("every position stored").noneMatch(checkpoint -> checkpoint.asString().startsWith("present"))
                );
            }
            NameDefined afterHandOver = nameDefined("afterHandOver");
            appendTagged("name:1", afterHandOver);
            await().untilAsserted(() -> assertThat(received).as("received").endsWith(appendedInTheGap, afterHandOver));
        } finally {
            subscription.shutdown();
        }
    }

    private NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private Consumer<CloudEvent> toDomainEvents(List<DomainEvent> target) {
        return cloudEvent -> target.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse(tag))))
                .toList();
        eventStore.append(cloudEvents);
    }
}
