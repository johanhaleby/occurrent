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
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CatchupListener;
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
 * A live model that opens asynchronously can lose the live start a replay handed over. The catch-up replays again
 * instead of leaving the subscription to the live model, which skips what was written in between or forgets it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLostLiveStartTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    static Stream<Arguments> everyCombination() {
        return Stream.of(true, false).flatMap(restart ->
                Stream.of(true, false).flatMap(positional ->
                        Stream.of(true, false).flatMap(storing ->
                                Stream.of(true, false).map(async ->
                                        Arguments.of(restart, positional, storing, async)))));
    }

    @ParameterizedTest(name = "restartOnHistoryLost={0}, positional={1}, storing={2}, opensAsynchronously={3}")
    @MethodSource("everyCombination")
    @Timeout(60)
    void a_live_start_the_live_model_loses_as_it_opens_replays_again_and_delivers_what_was_written_in_between(boolean restartOnHistoryLost, boolean positional, boolean storing, boolean opensAsynchronously) {
        InMemoryEventStore eventStore = positional ? new InMemoryEventStore(inMemorySubscriptionModel) : new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isEqualTo(positional);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        NameDefined writtenInTheGap = nameDefined("writtenInTheGap");
        LiveStartLosingSubscriptionModel liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, restartOnHistoryLost, !opensAsynchronously, () -> write(eventStore, writtenInTheGap));
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
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore, config);
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
            Subscription started = subscription.subscribe("subscription", positional ? StartAt.checkpoint(GlobalCheckpoint.of(0)) : StartAtTime.beginningOfTime(), toDomainEvents(received));

            assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).as("started").isTrue();
            assertThat(received).as("the history, and then all of it again together with what was written in between")
                    .containsExactlyElementsOf(Stream.concat(history.stream(), Stream.concat(history.stream(), Stream.of(writtenInTheGap))).toList());
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
            write(eventStore, afterHandOver);
            await().untilAsserted(() -> assertThat(received).as("received").endsWith(writtenInTheGap, afterHandOver));
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

    private void write(InMemoryEventStore eventStore, DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
