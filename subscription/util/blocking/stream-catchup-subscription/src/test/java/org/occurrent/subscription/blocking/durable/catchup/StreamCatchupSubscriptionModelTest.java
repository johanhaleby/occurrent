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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.retry.AttemptNotMade;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * In-memory unit tests for {@link StreamCatchupSubscriptionModel}, used directly rather than through the
 * {@code CatchupSubscriptionModel} dispatcher. These prove the extracted class works standalone (this module has no
 * {@code eventstore-api-dcb} dependency of its own; see the module's {@code pom.xml} and
 * {@code mvn dependency:tree}), covering both the legacy time-ordered catch-up and the position-ordered catch-up.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupSubscriptionModelTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CheckpointAwareSubscriptionModel subscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        subscriptionModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    // The contract a recording projection is told, rather than one it reads per delivery. The start arrives before
    // anything this catch-up delivers, the boundary arrives after the history that was already there and before the
    // events written since the catch-up started, and both name the same catch-up.
    @Test
    void tells_a_listener_when_a_catch_up_starts_and_when_its_history_has_been_read() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        write(eventStore, nameDefined("history"));

        CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Object> episodes = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        boolean sendsThem = subscription.listenForCatchup("subscription", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("started");
                episodes.add(episode);
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
                episodes.add(episode);
            }
        });
        assertThat(sendsThem).isTrue();

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            signals.add("delivered");
            // Written from inside the history read, so it cannot be part of it and what delivers it is the rest of
            // the catch-up.
            if (signals.stream().filter("delivered"::equals).count() == 1) {
                write(eventStore, nameDefined("writtenDuringTheHistoryRead"));
            }
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(signals).containsExactly("started", "delivered", "historyRead", "delivered"));
        assertThat(episodes).hasSize(2);
        assertThat(episodes.get(0)).isSameAs(episodes.get(1));
    }

    // The same two signals on the time path, which is a different method with its own send. A store that writes no
    // position replays by time for every start, and a store that writes one still replays by time for a specific
    // wall-clock start, so this send is live in production and the position-path test above never reaches it.
    @Test
    void tells_a_listener_when_a_catch_up_starts_and_when_its_history_has_been_read_on_the_time_path() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();
        write(eventStore, nameDefined("history"));

        CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Object> episodes = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        boolean sendsThem = subscription.listenForCatchup("subscription", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                signals.add("started");
                episodes.add(episode);
            }

            @Override
            public void historyRead(Object episode) {
                signals.add("historyRead");
                episodes.add(episode);
            }
        });
        assertThat(sendsThem).isTrue();

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            signals.add("delivered");
            // Written from inside the history read, so it cannot be part of it and what delivers it is the rest of
            // the catch-up.
            if (signals.stream().filter("delivered"::equals).count() == 1) {
                write(eventStore, nameDefined("writtenDuringTheHistoryRead"));
            }
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(signals).containsExactly("started", "delivered", "historyRead", "delivered"));
        assertThat(episodes).hasSize(2);
        assertThat(episodes.get(0)).isSameAs(episodes.get(1));
    }

    @Test
    void replays_historic_events_by_time_when_the_store_does_not_write_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();

        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        write(eventStore, event1);
        write(eventStore, event2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        AtomicBoolean replayedOnVirtualThread = new AtomicBoolean(false);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            replayedOnVirtualThread.set(Thread.currentThread().isVirtual());
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
        }).waitUntilStarted();

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(event1, event2);
            assertThat(replayedOnVirtualThread).isTrue();
        });
    }

    @Test
    void replays_historic_events_by_position_when_the_store_writes_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        assertThat(eventStore.writesPosition()).isTrue();

        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        write(eventStore, event1);
        write(eventStore, event2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(event1, event2));
    }

    static Stream<Named<Function<InMemoryEventStore, EventStoreQueries>>> storesThatWriteNoPosition() {
        Function<InMemoryEventStore, EventStoreQueries> writesNoPosition = store -> store;
        Function<InMemoryEventStore, EventStoreQueries> isNoPositionOrderedReader = QueriesOnlyEventStore::new;
        return Stream.of(Named.of("a position ordered reader that writes no position", writesNoPosition), Named.of("a store that is no position ordered reader", isNoPositionOrderedReader));
    }

    static Stream<Arguments> positionsAboveZeroOnStoresThatWriteNoPosition() {
        List<Checkpoint> positions = List.of(GlobalCheckpoint.of(1), GlobalCheckpoint.of(2, new StringBasedCheckpoint("some live start"), 0, 4));
        return storesThatWriteNoPosition().flatMap(store -> positions.stream().map(position -> Arguments.of(store, Named.of(position.asString(), position))));
    }

    @ParameterizedTest
    @MethodSource("storesThatWriteNoPosition")
    void a_start_at_global_position_0_replays_the_whole_history_by_time_and_then_goes_live_when_the_store_does_not_write_position(Function<InMemoryEventStore, EventStoreQueries> storeAsTheModelSeesIt) {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, storeAsTheModelSeesIt.apply(eventStore), new CatchupSubscriptionModelConfig(100));

        // When
        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        // Then
        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.liveStarts).as("the wrapped model goes live from the live start read before the replay, and is never opened at the position")
                .containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
    }

    @ParameterizedTest
    @MethodSource("positionsAboveZeroOnStoresThatWriteNoPosition")
    void a_start_after_a_global_position_above_0_is_refused_by_subscribe_and_starts_nothing_when_the_store_does_not_write_position(Function<InMemoryEventStore, EventStoreQueries> storeAsTheModelSeesIt, Checkpoint position) {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        writeAt(eventStore, "event1", at(1));
        writeAt(eventStore, "event2", at(2));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, storeAsTheModelSeesIt.apply(eventStore),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When
            Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.checkpoint(position), toDomainEvents(received)));

            // Then the call itself is refused, before a Subscription exists to wait on
            assertThat(failure).isInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("after a global position, since the event store doesn't write one");
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNull();
            assertThat(model.liveStarts).as("handed over").isEmpty();
            assertThat(model.isRunning("subscription")).as("subscribed in the wrapped model").isFalse();
        } finally {
            subscription.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("storesThatWriteNoPosition")
    void a_dynamic_start_that_resolves_to_global_position_0_replays_the_whole_history_by_time_stores_the_live_start_and_goes_live_when_the_store_does_not_write_position(
            Function<InMemoryEventStore, EventStoreQueries> storeAsTheModelSeesIt) {
        // Given the start Spring Boot builds for StartPosition.BEGINNING with ResumeBehavior.DEFAULT
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        assertThat(storage.exists("subscription")).as("nothing stored yet").isFalse();
        StartAt resumeFromTheStoredPosition = StartAt.dynamic(ctx -> storage.exists("subscription") ? StartAt.subscriptionModelDefault() : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, storeAsTheModelSeesIt.apply(eventStore),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When
            subscription.subscribe("subscription", resumeFromTheStoredPosition, toDomainEvents(received)).waitUntilStarted();

            // Then the whole history arrives in order
            await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
            // and the subscription is live once the wrapped model has been handed over to
            await().untilAsserted(() -> assertThat(model.liveStarts).hasSize(1));
            DomainEvent live = writeAt(eventStore, "live", at(100));
            await().untilAsserted(() -> assertThat(received).containsExactly(history.get(0), history.get(1), history.get(2), live));
            assertThat(model.liveStarts).as("the wrapped model goes live from the live start read before the replay, and is never opened at the position")
                    .containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
            assertThat(storage.read("subscription")).as("the stored checkpoint, which a restart resumes from").isNotNull().extracting(Checkpoint::asString).isEqualTo(liveStartReadNow.asString());
        } finally {
            subscription.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("storesThatWriteNoPosition")
    void a_dynamic_start_that_the_wrapped_model_answers_null_for_replays_the_whole_history_by_time_and_goes_live_without_storing_a_checkpoint_when_the_store_does_not_write_position(
            Function<InMemoryEventStore, EventStoreQueries> storeAsTheModelSeesIt) {
        // Given the start Spring Boot builds for StartPosition.BEGINNING with ResumeBehavior.SAME_AS_START_AT, where the model it wraps stores nothing
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        assertThat(eventStore.writesPosition()).isFalse();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        assertThat(storage.exists("subscription")).as("nothing stored yet").isFalse();
        StartAt replayAgainAtEveryStart = StartAt.dynamic(ctx -> CheckpointAwareInMemorySubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType())
                ? null : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, storeAsTheModelSeesIt.apply(eventStore),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When
            subscription.subscribe("subscription", replayAgainAtEveryStart, toDomainEvents(received)).waitUntilStarted();

            // Then the whole history arrives in order
            await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
            // and the subscription is live once the wrapped model has been handed over to
            await().untilAsserted(() -> assertThat(model.liveStarts).hasSize(1));
            DomainEvent live = writeAt(eventStore, "live", at(100));
            await().untilAsserted(() -> assertThat(received).containsExactly(history.get(0), history.get(1), history.get(2), live));
            assertThat(model.liveStarts).as("the wrapped model goes live from the live start read before the replay, and is never opened at the position")
                    .containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
            assertThat(saved).as("stored during the catch-up").isNotEmpty();
            assertThat(storage.exists("subscription")).as("no checkpoint left stored after going live").isFalse();
        } finally {
            subscription.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("storesThatWriteNoPosition")
    void a_default_start_is_refused_by_subscribe_and_starts_nothing_when_the_stored_position_is_above_0_and_the_store_does_not_write_position(Function<InMemoryEventStore, EventStoreQueries> storeAsTheModelSeesIt) {
        // Given a position above 0 with a live start beside it, stored under the subscription id
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        writeAt(eventStore, "event1", at(1));
        writeAt(eventStore, "event2", at(2));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        Checkpoint stored = GlobalCheckpoint.of(2, new StringBasedCheckpoint("some live start"), 0, 4);
        storage.save("subscription", stored);
        saved.clear();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, storeAsTheModelSeesIt.apply(eventStore),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When
            Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)));

            // Then the call itself is refused, before a Subscription exists to wait on
            assertThat(failure).isInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("catch-up subscription subscription after a global position, since the event store doesn't write one")
                    .hasMessageContaining("delete that checkpoint first");
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNotNull().extracting(Checkpoint::asString).isEqualTo(stored.asString());
            assertThat(model.liveStarts).as("handed over").isEmpty();
            assertThat(model.isRunning("subscription")).as("subscribed in the wrapped model").isFalse();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void every_n_counts_on_across_the_windows_of_one_catch_up_by_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        for (int i = 0; i < 6; i++) {
            write(eventStore, nameDefined("event" + i));
        }
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(3)).dcbCatchupPositionWindowSize(2));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).hasSize(6));
        // The stored checkpoints also hold the live start, so only their positions are compared
        assertThat(saved.stream().map(StringBasedCheckpoint::new).filter(GlobalCheckpoint::isGlobalCheckpoint).map(GlobalCheckpoint::positionOf))
                .as("every third event of six, read in windows of two").contains(3L, 6L);
    }

    @Test
    void each_event_a_replay_by_position_delivers_carries_the_checkpoint_stored_right_after_it() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);

        // When
        List<String> log = replayThreeEventsLoggingEachDeliveryAndSave(eventStore, StartAt.checkpoint(GlobalCheckpoint.of(0)));

        // Then
        assertThat(entriesRightAfterEachDelivery(log)).as("what is stored right after each replayed event").containsExactlyElementsOf(deliveriesAsSaves(log));
        assertThat(deliveredCheckpoints(log).stream().map(StringBasedCheckpoint::new).map(GlobalCheckpoint::positionOf))
                .as("the position each delivered checkpoint holds").containsExactly(1L, 2L, 3L);
    }

    @Test
    void each_event_a_replay_by_time_delivers_carries_the_checkpoint_stored_right_after_it() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();

        // When
        List<String> log = replayThreeEventsLoggingEachDeliveryAndSave(eventStore, StartAtTime.beginningOfTime());

        // Then
        assertThat(entriesRightAfterEachDelivery(log)).as("what is stored right after each replayed event").containsExactlyElementsOf(deliveriesAsSaves(log));
        assertThat(deliveredCheckpoints(log)).as("the checkpoints the replayed events were delivered with").doesNotHaveDuplicates();
    }

    @Test
    void every_n_counts_on_from_the_replay_into_the_events_written_while_it_ran() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("history"));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(2)));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            received.add(cloudEvent);
            // Written from inside the replay, so the catch-up reads it in a window of its own after the replay
            if (received.size() == 1) {
                write(eventStore, nameDefined("writtenDuringTheReplay"));
            }
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).hasSize(2));
        assertThat(catchupTimeCheckpointsIn(saved)).extracting(CatchupTimeCheckpoint::time).as("the second event, read in a window after the replay")
                .contains(TimeBasedCheckpoint.from(received.get(1).getTime()).asString());
    }

    @Test
    void beginning_of_time_maps_to_position_zero_when_the_store_writes_position() {
        PositionOnlyInMemoryEventStore eventStore = new PositionOnlyInMemoryEventStore(inMemorySubscriptionModel);
        assertThat(eventStore.writesPosition()).isTrue();

        NameDefined event1 = nameDefined("event1");
        NameDefined event2 = nameDefined("event2");
        write(eventStore, event1);
        write(eventStore, event2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(event1, event2));
        assertThat(eventStore.lastPositionRange).isNotNull();
        assertThat(eventStore.lastPositionRange.afterPosition()).hasValue(0L);
    }

    @Test
    void live_only_subscription_delegates_to_the_wrapped_model_when_start_is_now() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.now(), toDomainEvents(received)).waitUntilStarted();

        NameDefined live = nameDefined("live");
        write(eventStore, live);

        await().untilAsserted(() -> assertThat(received).containsExactly(live));
    }

    /**
     * The assertion ADR 94 promised the TCK would make and could not: the subscription-model TCK depends on
     * {@code occurrent-subscription-api-blocking} and nothing else on purpose, and driving this needs a catch-up model,
     * which lives here. {@code CheckpointAwareSubscriptionModel.globalCheckpoint()} documents null as an unresolvable
     * problem, and this is what that costs: such a model is still a working live model, but it cannot sit behind
     * catch-up, because the handover at the end of a replay has no position to start live delivery from.
     * <p>
     * What must never happen is the quiet version, where the handover falls back to "now" and every event committed
     * while history replayed is dropped with nothing said. The positive control is
     * {@link #replays_historic_events_by_position_when_the_store_writes_position()}, which is this test with a model
     * that answers.
     */
    @Test
    void a_model_that_reports_no_checkpoint_cannot_sit_behind_catchup() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        write(eventStore, nameDefined("event1"));
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CheckpointAwareSubscriptionModel reportsNoCheckpoint = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, null);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(reportsNoCheckpoint, eventStore, new CatchupSubscriptionModelConfig(100));

        Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

        // A start that failed and will not be retried throws rather than answering false, since false is reserved for a
        // subscription nothing has started yet but still could, and this one never will. Five seconds is a bound on a
        // replay that fails on its first call into the model, not a wait for anything, so it is only ever paid in
        // full by a test that was going to fail anyway.
        assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the reason has to reach whoever reads the log, or an operator sees a subscription that simply "
                        + "never started")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no resume token");
        assertThat(received)
                .as("and nothing may be replayed, because delivering the history and then silently never going live is "
                        + "worse than refusing: the read model would look up to date and stop moving")
                .isEmpty();
        assertThat(subscription.isCatchingUp("subscription"))
                .as("a failed catch-up must not stay visible as catching up forever, the same guarantee a completed "
                        + "or cancelled one already gets")
                .isFalse();
        assertThat(subscription.isRunning("subscription"))
                .as("nothing is running for a replay that failed and was never handed over")
                .isFalse();
    }

    /**
     * Before this class stopped leaking the running-catch-up marker on failure, pausing a subscription whose replay
     * had already failed still matched the "replay in flight" branch in {@code pauseSubscription}, which only
     * records the request for {@code applyPendingPauseIfAny} to apply once the replay hands over. A replay that
     * failed never hands over, so the pause was silently swallowed. It must now take the other branch and reach the
     * delegate directly, which knows nothing about a subscription whose catch-up never got that far.
     */
    @Test
    void pausing_a_subscription_whose_catchup_already_failed_reaches_the_delegate_instead_of_being_swallowed() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        write(eventStore, nameDefined("event1"));
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CheckpointAwareSubscriptionModel reportsNoCheckpoint = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, null);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(reportsNoCheckpoint, eventStore, new CatchupSubscriptionModelConfig(100));
        Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));
        assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(5)));

        assertThatThrownBy(() -> subscription.pauseSubscription("subscription"))
                .as("the delegate never saw this subscription, since the catch-up failed before handing over to it")
                .isInstanceOf(UnknownSubscriptionException.class);
    }

    /**
     * A pause requested while the replay is still in flight only records itself in
     * {@code pauseRequestedDuringCatchup}, for {@code applyPendingPauseIfAny} to apply once the replay hands over
     * to the delegate. A replay that fails instead of handing over must still clear that record, or the pause
     * outlives the catch-up it was requested for and {@code isPaused} keeps answering {@code true} forever.
     */
    @Test
    void a_pause_requested_while_the_replay_is_in_flight_does_not_survive_the_replay_then_failing() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        CheckpointAwareSubscriptionModel reportsNoCheckpoint = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, null);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(reportsNoCheckpoint, eventStore, new CatchupSubscriptionModelConfig(100));
        String subscriptionId = "subscription";

        Subscription started = subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> subscription.pauseSubscription(subscriptionId));

        assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(5))).isInstanceOf(IllegalStateException.class);
        assertThat(subscription.isPaused(subscriptionId))
                .as("the pause was requested for a replay that never handed over to apply it, and must not survive "
                        + "the replay failing")
                .isFalse();
    }

    @Test
    void catchup_is_marked_running_before_the_virtual_thread_starts() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        AtomicBoolean runningMarkedBeforeCatchupRuns = new AtomicBoolean(false);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
                return super.startCatchupAsync(subscriptionId, lastStored -> {
                    runningMarkedBeforeCatchupRuns.set(runningCatchupSubscriptions.containsKey(subscriptionId));
                    return catchup.replayFrom(lastStored);
                }, holdPaused);
            }
        };

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received)).waitUntilStarted();

        assertThat(runningMarkedBeforeCatchupRuns).isTrue();
    }

    /**
     * The reason {@code isCatchingUp} exists as a signal distinct from {@code isRunning}: a caller (for example a
     * saga's timer poller) that gates on liveness needs to know when the replay has actually handed over, since
     * {@code isRunning(id)} is true for the entire replay and cannot answer that on its own.
     */
    @Test
    void a_subscription_reports_catching_up_while_its_replay_is_still_in_flight_and_stops_once_it_hands_over() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        Subscription started = subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });

        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            assertAll(
                    () -> assertThat(subscription.isCatchingUp(subscriptionId)).isTrue(),
                    () -> assertThat(subscription.isRunning(subscriptionId)).isTrue()
            );
        } finally {
            releaseReplay.countDown();
        }

        assertThat(started.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
        assertThat(subscription.isCatchingUp(subscriptionId)).isFalse();
    }

    @Test
    void an_id_the_model_has_never_seen_is_not_catching_up() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        assertThat(subscription.isCatchingUp("never-subscribed")).isFalse();
    }

    /**
     * Before per-attempt identity (issue #737, finding 4), the running-catch-up marker was keyed only by
     * {@code subscriptionId}, with no way to tell a cancelled attempt's own entry from a later attempt's. A
     * cancelled replay whose virtual thread had not yet noticed would finish after a resubscribe for the same id had
     * already started a second replay, and blindly remove that second replay's marker instead of its own: the
     * cancelled attempt would then hand itself over to the delegate as if it were still current, while the actually
     * current attempt found its own marker gone and reported itself cancelled instead.
     */
    @Test
    void a_cancelled_replays_late_completion_does_not_disturb_a_later_attempts_bookkeeping_for_the_same_id() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        String subscriptionId = "subscription";

        CountDownLatch firstReplayReached = new CountDownLatch(1);
        CountDownLatch releaseFirstReplay = new CountDownLatch(1);
        Subscription first = subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            firstReplayReached.countDown();
            awaitLatch(releaseFirstReplay);
        });
        assertThat(firstReplayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancel while the first attempt is still blocked inside its replay, then resubscribe the same id before
        // that blocked thread has had any chance to notice the cancellation.
        subscription.cancelSubscription(subscriptionId);

        CountDownLatch secondReplayReached = new CountDownLatch(1);
        CountDownLatch releaseSecondReplay = new CountDownLatch(1);
        Subscription second = subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            secondReplayReached.countDown();
            awaitLatch(releaseSecondReplay);
        });
        assertThat(secondReplayReached.await(500, TimeUnit.MILLISECONDS)).as("the second attempt entered the action while the cancelled call is still in it").isFalse();

        // Release the stale first attempt, which the second attempt waits for, and then the second is in flight, mid-replay, unresolved.
        releaseFirstReplay.countDown();
        assertThat(secondReplayReached.await(5, TimeUnit.SECONDS)).as("the second attempt enters the action once the cancelled call has returned").isTrue();
        assertThat(first.waitUntilStarted(Duration.ofSeconds(5)))
                .as("a cancelled attempt must not resurrect itself as the live subscription once its stale thread "
                        + "finally runs")
                .isFalse();

        assertThat(subscription.isCatchingUp(subscriptionId))
                .as("the second attempt is still legitimately in flight; its own marker must have survived the "
                        + "first attempt's stale cleanup")
                .isTrue();

        releaseSecondReplay.countDown();
        assertThat(second.waitUntilStarted(Duration.ofSeconds(5)))
                .as("the subscription actually in use must hand over to live delivery, not be told it was "
                        + "cancelled by someone else's stale attempt")
                .isTrue();
    }

    /**
     * Issue #1517. A cancel and then a stop each wait up to ACTION_WAIT for an action still running, and both give up
     * with a WARN. The stop drops the attempt the cancel ended, and a replay that runs later must still wait for the
     * action to return, so its first call to the action never runs alongside the earlier one.
     */
    @Test
    @Timeout(30)
    void a_replay_started_after_a_cancel_and_a_stop_waits_for_the_cancelled_replays_action_that_outlasts_both_waits() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        String subscriptionId = "subscription";

        CountDownLatch firstActionReached = new CountDownLatch(1);
        CountDownLatch releaseFirstAction = new CountDownLatch(1);
        CountDownLatch secondActionReached = new CountDownLatch(1);
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);
        try {
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
                firstActionReached.countDown();
                awaitLatch(releaseFirstAction);
            });
            assertThat(firstActionReached.await(5, TimeUnit.SECONDS)).isTrue();

            // Both give up on the blocked action after their wait, so the first call is still in it afterwards
            subscription.cancelSubscription(subscriptionId);
            subscription.stop();
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> secondActionReached.countDown());
            subscription.start(true);

            assertThat(stillRanWarnings(logged)).as("the warnings logged by the cancel and the stop, that both gave up waiting").satisfiesExactly(
                    message -> assertThat(message).startsWith("A cancel returned while the action of subscription subscription still ran"),
                    message -> assertThat(message).startsWith("A stop returned while the action of subscription subscription still ran"));
            assertThat(earlierAttemptsDoneOfCurrentAttempt(subscription, subscriptionId))
                    .as("the second replay is registered and still waits for the first call to the action, that a cancel and a stop gave up waiting for, to return")
                    .isNotNull()
                    .isNotDone();
            assertThat(secondActionReached.getCount()).as("the second replay has not entered the action while the first call to it is still in it").isEqualTo(1);
            releaseFirstAction.countDown();
            assertThat(secondActionReached.await(5, TimeUnit.SECONDS)).as("the second replay enters the action once the first call has returned").isTrue();
        } finally {
            releaseFirstAction.countDown();
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    /**
     * Copilot review on PR #823 (issue #737). A stale in-flight event's checkpoint save survived being cancelled,
     * since the identity check treated "nobody registered" as always safe to persist, not distinguishing a
     * cancellation (which also deletes the stored checkpoint, expecting it gone) from a graceful shutdown (which
     * deletes nothing and does want the last position kept).
     */
    @Test
    void a_stale_events_checkpoint_save_does_not_recreate_a_position_cancelSubscription_just_deleted() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancels while the event's action is still blocked, deleting the (so far nonexistent) stored checkpoint.
        subscription.cancelSubscription(subscriptionId);
        releaseReplay.countDown();

        // The stale attempt's action already returned and tried to persist by now if it was going to; give it a
        // moment, then assert the cancellation's deletion is what stands, not a resurrected position.
        Thread.sleep(200);
        assertThat(storage.read(subscriptionId))
                .as("cancelSubscription deleted this id's position and expects it to stay gone, not be recreated "
                        + "by the stale attempt's own in-flight event")
                .isNull();
    }

    /**
     * Copilot review on PR #823 (issue #737). Replacing a cancelled attempt's map entry with a shared sentinel,
     * instead of flagging the attempt object itself, meant nothing ever removed that entry for an id nobody
     * resubscribed, growing the map without bound over the model's lifetime.
     */
    @Test
    void cancelling_a_never_reused_subscription_id_does_not_leave_its_entry_registered_forever() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        String subscriptionId = "a-subscription-id-never-reused-again";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        subscription.cancelSubscription(subscriptionId);
        releaseReplay.countDown();

        Field currentAttemptField = AbstractCatchupSubscriptionModel.class.getDeclaredField("currentAttempt");
        currentAttemptField.setAccessible(true);
        await().untilAsserted(() -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> currentAttempt = (Map<String, Object>) currentAttemptField.get(subscription);
            assertThat(currentAttempt)
                    .as("the cancelled attempt's own cleanup removes its entry once its replay thread notices, "
                            + "instead of leaving a tombstone behind for an id that is never resubscribed")
                    .doesNotContainKey(subscriptionId);
        });
    }

    /**
     * Issue #827 (deferred from PR 823's per-attempt identity fix, #737). That fix made the ownership decision
     * itself atomic (endReplayIfStillCurrent), but nothing held ownership across what follows it. A cancellation
     * landing after this attempt decided it was still current, but before the delegate subscribe below actually
     * ran, found nothing left in the map to flag, and the delegate did not know the id yet either, so the
     * cancellation was silently lost and the delegate went live anyway. Closing that gap needs a lock spanning the
     * whole handover, not just the decision.
     */
    @Test
    void a_cancellation_racing_the_final_delegate_subscribe_is_not_lost() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";
        CountDownLatch reachedFinishing = new CountDownLatch(1);
        CountDownLatch releaseFinishing = new CountDownLatch(1);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected boolean endReplayIfStillCurrent(String subscriptionId) {
                boolean stillCurrent = super.endReplayIfStillCurrent(subscriptionId);
                reachedFinishing.countDown();
                awaitLatch(releaseFinishing);
                return stillCurrent;
            }
        };

        subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
        });
        assertThat(reachedFinishing.await(5, TimeUnit.SECONDS)).isTrue();

        // Races the cancellation against the still-in-flight delegate subscribe, on its own thread since it must
        // not deadlock this test on locked (post-fix) code, where it blocks until the handover below completes.
        Thread cancel = new Thread(() -> subscription.cancelSubscription(subscriptionId));
        cancel.start();

        // Unlocked (pre-fix) code lets this cancellation run its fast no-op path (nothing is left to flag, the
        // delegate does not know the id yet) well within this window; locked (post-fix) code blocks it here until
        // releaseFinishing below, so this reliably times out there instead.
        cancel.join(500);

        releaseFinishing.countDown();
        cancel.join(Duration.ofSeconds(5).toMillis());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(subscription.isRunning(subscriptionId))
                        .as("a cancellation racing the handover must still end the subscription, not be silently "
                                + "lost to a delegate subscribe that had already committed to going live")
                        .isFalse());
    }

    /**
     * A pause asked while a subscribe's replay runs is applied by the handover, which holds the handover lock from the end
     * of the replay through the live subscribe. A resume that comes after the replay ended but before the live model
     * has the subscription therefore waits for that lock, instead of finding no live subscription to resume and
     * throwing after the pause returned ok.
     */
    @Test
    void a_resume_that_comes_between_the_end_of_a_replay_and_the_live_subscribe_waits_for_the_handover_and_does_not_throw_after_the_pause_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";
        CountDownLatch inTheAction = new CountDownLatch(1);
        CountDownLatch leaveTheAction = new CountDownLatch(1);
        CountDownLatch replayEnded = new CountDownLatch(1);
        CountDownLatch releaseTheHandover = new CountDownLatch(1);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected boolean endReplayIfStillCurrent(String subscriptionId) {
                boolean stillCurrent = super.endReplayIfStillCurrent(subscriptionId);
                replayEnded.countDown();
                awaitLatch(releaseTheHandover);
                return stillCurrent;
            }
        };
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        AtomicReference<Throwable> resumeThrew = new AtomicReference<>();
        try {
            // Given a pause asked while the replay of the subscribe runs, and a replay that has ended but holds the handover
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
                inTheAction.countDown();
                awaitLatch(leaveTheAction);
            });
            assertThat(inTheAction.await(5, TimeUnit.SECONDS)).as("the replay reached the action").isTrue();
            subscription.pauseSubscription(subscriptionId);
            leaveTheAction.countDown();
            assertThat(replayEnded.await(5, TimeUnit.SECONDS)).as("the replay ended and the handover is held").isTrue();

            // When a resume is made from another thread
            Thread resume = new Thread(() -> {
                resumingThread.set(Thread.currentThread());
                resumeThrew.set(catchThrowable(() -> subscription.resumeSubscription(subscriptionId)));
            });
            resume.start();

            // Then it waits for the handover lock while the handover is held
            await().atMost(Duration.ofSeconds(5)).until(() -> resumingThread.get() != null && Arrays.stream(resumingThread.get().getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("lockHandover")));
            assertThat(resume.isAlive()).as("the resume is still waiting while the handover is held").isTrue();

            // And once the handover goes on it returns without throwing, and the subscription runs
            releaseTheHandover.countDown();
            resume.join(Duration.ofSeconds(5).toMillis());
            assertAll(
                    () -> assertThat(resume.isAlive()).as("the resume returned").isFalse(),
                    () -> assertThat(resumeThrew.get()).as("what the resume threw").isNull(),
                    () -> await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(subscription.isRunning(subscriptionId)).as("the subscription runs").isTrue()),
                    () -> assertThat(subscription.isPaused(subscriptionId)).as("the subscription is paused").isFalse());
        } finally {
            leaveTheAction.countDown();
            releaseTheHandover.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issue #827, the checkpoint-delete manifestation of the same gap (deferred from PR 823, the time-based path's
     * "delegate must not run" cleanup at what was then line 307). A finishing attempt's temporary checkpoint delete
     * runs after it already relinquished ownership of the id. Unguarded, a fresh attempt for the same id can
     * register and save its own position in that gap, and the stale delete then removes the fresh attempt's
     * position instead of its own. Closing the gap needs holding the same lock across the delete and gating a
     * fresh attempt's registration on it too, so the fresh attempt cannot even start until the stale delete (if
     * any) is already done.
     */
    @Test
    void a_finishing_attempts_late_checkpoint_delete_does_not_clobber_a_fresh_attempts_saved_position() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";

        CountDownLatch freshSaved = new CountDownLatch(1);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                Checkpoint saved = super.save(id, checkpoint, condition);
                freshSaved.countDown();
                return saved;
            }
        };
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage));

        CountDownLatch reachedFinishing = new CountDownLatch(1);
        CountDownLatch releaseFinishing = new CountDownLatch(1);
        // Only the FIRST call (the stale attempt's) pauses here; the fresh attempt started below reaches this same
        // overridden method too, for its own identity decision, and must not be blocked by it as well, or nothing
        // would ever distinguish "the fresh attempt got ahead" from "both attempts are simply parked together".
        AtomicBoolean firstCallToEndReplayIfStillCurrent = new AtomicBoolean(true);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config) {
            @Override
            protected boolean endReplayIfStillCurrent(String subscriptionId) {
                boolean stillCurrent = super.endReplayIfStillCurrent(subscriptionId);
                if (firstCallToEndReplayIfStillCurrent.compareAndSet(true, false)) {
                    reachedFinishing.countDown();
                    awaitLatch(releaseFinishing);
                }
                return stillCurrent;
            }
        };

        // Resolves to real history for the catch-up's own replay, but to null for the delegate, so the finishing
        // tail's "delegate must not run" branch deletes the temporary catch-up position once replay is done.
        StartAt neverDelegates = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(CheckpointAwareInMemorySubscriptionModel.class) ? null : StartAtTime.beginningOfTime());

        subscription.subscribe(subscriptionId, neverDelegates, cloudEvent -> {
        });
        assertThat(reachedFinishing.await(5, TimeUnit.SECONDS)).isTrue();

        // A fresh, normally-delegating attempt for the same id, started on its own thread while the first is
        // blocked right after deciding it is still current but before its own delete runs. It cancels the first
        // subscription before it subscribes again, since a subscribe for an id that is in use is refused.
        AtomicReference<Throwable> freshFailure = new AtomicReference<>();
        Thread freshAttempt = new Thread(() -> {
            try {
                subscription.cancelSubscription(subscriptionId);
                subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
                }).waitUntilStarted();
            } catch (Throwable e) {
                freshFailure.set(e);
            }
        });
        freshAttempt.start();

        // Unlocked (pre-fix) code lets this fresh attempt register and save well within this window; locked
        // (post-fix) code cannot even register until releaseFinishing below, so this reliably times out there
        // instead, which is exactly why the real assertion is the one after both attempts have finished, not this
        // wait.
        freshSaved.await(500, TimeUnit.MILLISECONDS);

        releaseFinishing.countDown();
        freshAttempt.join(Duration.ofSeconds(5).toMillis());

        assertThat(freshFailure.get()).as("what the fresh attempt's cancel and subscribe threw").isNull();
        assertThat(freshSaved.await(5, TimeUnit.SECONDS))
                .as("the fresh attempt must have saved its own position by the time both attempts have finished")
                .isTrue();
        assertThat(storage.read(subscriptionId))
                .as("a finishing attempt's late, now-stale checkpoint delete must not remove a fresh attempt's own "
                        + "saved position for the same id")
                .isNotNull();
    }

    /**
     * Same race as {@link #a_finishing_attempts_late_checkpoint_delete_does_not_clobber_a_fresh_attempts_saved_position()},
     * for the position-ordered catch-up path instead of the time-ordered one. A separate finishing tail in
     * {@code StreamCatchupSubscriptionModel} has its own checkpoint-delete call, deferred from PR 823 alongside the
     * time-based one at what was then line 409 of the same file.
     */
    @Test
    void a_finishing_attempts_late_checkpoint_delete_does_not_clobber_a_fresh_attempts_saved_position_in_position_mode() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        assertThat(eventStore.writesPosition()).isTrue();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";

        CountDownLatch freshSaved = new CountDownLatch(1);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                Checkpoint saved = super.save(id, checkpoint, condition);
                freshSaved.countDown();
                return saved;
            }
        };
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage));

        CountDownLatch reachedFinishing = new CountDownLatch(1);
        CountDownLatch releaseFinishing = new CountDownLatch(1);
        AtomicBoolean firstCallToEndReplayIfStillCurrent = new AtomicBoolean(true);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config) {
            @Override
            protected boolean endReplayIfStillCurrent(String subscriptionId) {
                boolean stillCurrent = super.endReplayIfStillCurrent(subscriptionId);
                if (firstCallToEndReplayIfStillCurrent.compareAndSet(true, false)) {
                    reachedFinishing.countDown();
                    awaitLatch(releaseFinishing);
                }
                return stillCurrent;
            }
        };

        // Beginning-of-time maps to position 0 on a position-writing store (streamPositionCatchup), still resolving
        // to null for the delegate so the finishing tail's "delegate must not run" branch deletes the temporary
        // catch-up position once replay is done.
        StartAt neverDelegates = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(CheckpointAwareInMemorySubscriptionModel.class) ? null : StartAtTime.beginningOfTime());

        subscription.subscribe(subscriptionId, neverDelegates, cloudEvent -> {
        });
        assertThat(reachedFinishing.await(5, TimeUnit.SECONDS)).isTrue();

        AtomicReference<Throwable> freshFailure = new AtomicReference<>();
        Thread freshAttempt = new Thread(() -> {
            try {
                subscription.cancelSubscription(subscriptionId);
                subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
                }).waitUntilStarted();
            } catch (Throwable e) {
                freshFailure.set(e);
            }
        });
        freshAttempt.start();

        freshSaved.await(500, TimeUnit.MILLISECONDS);

        releaseFinishing.countDown();
        freshAttempt.join(Duration.ofSeconds(5).toMillis());

        assertThat(freshFailure.get()).as("what the fresh attempt's cancel and subscribe threw").isNull();
        assertThat(freshSaved.await(5, TimeUnit.SECONDS))
                .as("the fresh attempt must have saved its own position by the time both attempts have finished")
                .isTrue();
        assertThat(storage.read(subscriptionId))
                .as("a finishing attempt's late, now-stale checkpoint delete must not remove a fresh attempt's own "
                        + "saved position for the same id")
                .isNotNull();
    }

    @Test
    void a_live_subscribe_for_the_same_id_while_a_catch_up_still_replays_is_refused_and_the_catch_up_goes_live_with_its_own_delegate_subscribe() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CatchupSubscription replaying = (CatchupSubscription) subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            received.add(nameOf(cloudEvent));
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // A live subscribe for the same id, classified LIVE by StartAt.now() so it bypasses catch-up entirely,
        // while the catch-up above is still blocked mid-replay
        Throwable thrown = catchThrowable(() -> subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
        }));
        releaseReplay.countDown();

        assertThat(thrown).as("the live subscribe for an id whose catch-up still replays").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(replaying.delegatedSubscription().get(5, TimeUnit.SECONDS))
                .as("the catch-up keeps its own delegate subscribe instead of being cancelled by the refused live subscribe")
                .isNotInstanceOf(CancelledSubscription.class);
        write(eventStore, nameDefined("event2"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).containsExactly("event1", "event2"));
        assertThat(subscription.isRunning(subscriptionId)).as("the subscription runs once the catch-up went live").isTrue();
    }

    @Test
    void a_second_catch_up_subscribe_for_the_same_id_while_the_first_still_replays_is_refused_and_the_first_delivers_every_event() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CopyOnWriteArrayList<String> receivedByFirst = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> receivedBySecond = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CatchupSubscription first = (CatchupSubscription) subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            receivedByFirst.add(nameOf(cloudEvent));
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        Throwable thrown = catchThrowable(() -> subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> receivedBySecond.add(nameOf(cloudEvent))));
        releaseReplay.countDown();

        assertThat(thrown).as("the second catch-up subscribe for an id whose catch-up still replays").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(first.delegatedSubscription().get(5, TimeUnit.SECONDS)).as("the first catch-up hands over").isNotInstanceOf(CancelledSubscription.class);
        write(eventStore, nameDefined("event3"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(receivedByFirst).as("what the first subscription received").containsExactly("event1", "event2", "event3"));
        assertThat(receivedBySecond).as("what the refused subscription received").isEmpty();
    }

    @Test
    void a_subscribe_for_the_same_id_after_a_cancel_while_the_first_catch_up_still_replays_is_accepted_and_delivers() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CopyOnWriteArrayList<String> receivedBySecond = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CatchupSubscription first = (CatchupSubscription) subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        subscription.cancelSubscription(subscriptionId);
        Subscription second = subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> receivedBySecond.add(nameOf(cloudEvent)));
        sleepFor(500);
        assertThat(receivedBySecond).as("what the second subscription received while the cancelled call is in the action").isEmpty();
        releaseReplay.countDown();
        assertThat(second.waitUntilStarted(Duration.ofSeconds(5))).as("the second subscription started").isTrue();

        assertThat(first.delegatedSubscription().get(5, TimeUnit.SECONDS)).as("the cancelled catch-up does not hand over").isInstanceOf(CancelledSubscription.class);
        write(eventStore, nameDefined("event3"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(receivedBySecond).containsExactly("event1", "event2", "event3"));
        assertThat(subscription.isRunning(subscriptionId)).as("the second subscription runs").isTrue();
    }

    @Test
    void a_subscribe_for_the_same_id_while_the_catch_up_waits_to_run_on_a_stopped_model_is_refused_and_a_start_that_resumes_runs_the_catch_up() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";
        CopyOnWriteArrayList<String> receivedByFirst = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> receivedBySecond = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        subscription.stop();
        CatchupSubscription first = (CatchupSubscription) subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> receivedByFirst.add(nameOf(cloudEvent)));
        assertThat(subscription.isPaused(subscriptionId)).as("the catch-up waits to run").isTrue();

        Throwable thrown = catchThrowable(() -> subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), cloudEvent -> receivedBySecond.add(nameOf(cloudEvent))));
        subscription.start(true);

        assertThat(thrown).as("the second subscribe for an id whose catch-up waits to run").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(first.delegatedSubscription().get(5, TimeUnit.SECONDS)).as("the first catch-up hands over").isNotInstanceOf(CancelledSubscription.class);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(receivedByFirst).as("what the first subscription received").containsExactly("event1"));
        assertThat(receivedBySecond).as("what the refused subscription received").isEmpty();
    }

    @Test
    void a_subscribe_for_the_same_id_while_the_catch_up_is_held_paused_is_refused_and_a_resume_runs_the_catch_up() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        String subscriptionId = "subscription";
        CopyOnWriteArrayList<String> receivedByFirst = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> receivedBySecond = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CatchupSubscription first = (CatchupSubscription) subscription.subscribePaused(subscriptionId, null, StartAtTime.beginningOfTime(), cloudEvent -> receivedByFirst.add(nameOf(cloudEvent)));
        assertThat(subscription.isPaused(subscriptionId)).as("the catch-up is held paused").isTrue();

        Throwable thrown = catchThrowable(() -> subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> receivedBySecond.add(nameOf(cloudEvent))));
        assertThat(thrown).as("the second subscribe for an id whose catch-up is held paused").isInstanceOf(DuplicateSubscriptionIdException.class);
        subscription.resumeSubscription(subscriptionId);

        assertThat(first.delegatedSubscription().get(5, TimeUnit.SECONDS)).as("the first catch-up hands over").isNotInstanceOf(CancelledSubscription.class);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(receivedByFirst).as("what the first subscription received").containsExactly("event1"));
        assertThat(receivedBySecond).as("what the refused subscription received").isEmpty();
    }

    /**
     * Copilot review on PR #839 (issue #827). cancelSubscription's own path used to always acquire the handover
     * lock, creating one for any id passed to it, including one this model never ran a catch-up for. A dual-mode
     * dispatcher calls cancelSubscription on every child for every cancellation regardless of which one, if any,
     * actually owns the id, so a caller passing arbitrary or tenant-scoped ids would grow this registry without
     * bound even faster than a real subscribe() ever would.
     */
    @Test
    void cancelling_an_id_this_model_never_ran_a_catch_up_for_does_not_create_a_handover_lock_for_it() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        String neverSubscribedId = "an-id-this-model-never-saw";

        subscription.cancelSubscription(neverSubscribedId);

        Field handoverLocksField = AbstractCatchupSubscriptionModel.class.getDeclaredField("handoverLocks");
        handoverLocksField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> handoverLocks = (Map<String, Object>) handoverLocksField.get(subscription);
        assertThat(handoverLocks)
                .as("cancelling an id this model never ran a catch-up for must not reserve a lock for it")
                .doesNotContainKey(neverSubscribedId);
    }

    @Test
    void is_catching_up_rejects_a_null_subscription_id() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        Throwable thrown = catchThrowable(() -> subscription.isCatchingUp(null));

        assertThat(thrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void a_shutdown_during_the_replay_stores_no_position_past_the_history_so_a_restart_delivers_all_of_it() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<NameDefined> history = Stream.of("event0", "event1", "event2", "event3", "event4").map(this::nameDefined).toList();
        history.forEach(e -> write(eventStore, e));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StartAt fromTheStoredPositionOrTheBeginning = StartAt.dynamic(__ -> storage.exists("subscription") ? StartAt.subscriptionModelDefault() : StartAtTime.beginningOfTime());
        CountDownLatch handlingFirstEvent = new CountDownLatch(1);
        CountDownLatch releaseFirstEvent = new CountDownLatch(1);
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel first = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1000)));
        first.subscribe("subscription", fromTheStoredPositionOrTheBeginning, e -> {
            received.add(e);
            if (received.size() == 1) {
                handlingFirstEvent.countDown();
                awaitLatch(releaseFirstEvent);
            }
        });
        awaitLatch(handlingFirstEvent);

        first.shutdown();
        releaseFirstEvent.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> !first.isCatchingUp("subscription"));

        assertThat(storage.read("subscription")).as("position stored by a replay the shutdown cut short").isNull();
        InMemorySubscriptionModel restartedLive = new InMemorySubscriptionModel();
        try {
            CopyOnWriteArrayList<CloudEvent> afterRestart = new CopyOnWriteArrayList<>();
            StreamCatchupSubscriptionModel second = new StreamCatchupSubscriptionModel(new CheckpointAwareInMemorySubscriptionModel(restartedLive), eventStore,
                    new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1000)));
            second.subscribe("subscription", fromTheStoredPositionOrTheBeginning, afterRestart::add);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(Stream.concat(received.stream(), afterRestart.stream()).map(CloudEvent::getId).distinct())
                    .as("history delivered across the shutdown and the restart")
                    .containsExactlyInAnyOrderElementsOf(history.stream().map(DomainEvent::eventId).toList()));
        } finally {
            restartedLive.shutdown();
        }
    }

    @Test
    void a_replay_that_stop_cut_short_runs_again_from_the_last_position_it_stored_so_the_stored_position_never_moves_back() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<NameDefined> history = Stream.of("event0", "event1", "event2", "event3", "event4", "event5").map(this::nameDefined).toList();
        history.forEach(e -> write(eventStore, e));
        CopyOnWriteArrayList<Long> stored = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                // The wrapped model's own position, stored once the replay hands over, is not a position in the history
                if (GlobalCheckpoint.isGlobalCheckpoint(checkpoint)) {
                    stored.add(GlobalCheckpoint.positionOf(checkpoint));
                }
                return super.save(id, checkpoint, condition);
            }
        };
        CountDownLatch handlingThirdEvent = new CountDownLatch(1);
        CountDownLatch releaseThirdEvent = new CountDownLatch(1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)).dcbCatchupPositionWindowSize(2));
        model.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), e -> {
            received.add(e.getId());
            if (received.size() == 3) {
                handlingThirdEvent.countDown();
                awaitLatch(releaseThirdEvent);
            }
        });
        awaitLatch(handlingThirdEvent);

        model.stop();
        model.start(true);
        releaseThirdEvent.countDown();

        List<String> historyIds = history.stream().map(DomainEvent::eventId).toList();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).containsAll(historyIds));
        await().atMost(Duration.ofSeconds(5)).until(() -> !model.isCatchingUp("subscription"));
        assertAll(
                () -> assertThat(stored).as("positions stored during the replay, in the order they were stored").isSorted(),
                () -> assertThat(received.subList(0, 3)).as("delivered before the stop").containsExactlyElementsOf(historyIds.subList(0, 3)),
                () -> assertThat(received).as("delivered in all").containsExactlyElementsOf(historyIds)
        );
    }

    @Test
    void a_restart_from_a_stored_time_position_delivers_the_events_written_in_that_same_millisecond() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<NameDefined> history = Stream.of("event0", "event1", "event2").map(this::nameDefined).toList();
        history.forEach(e -> eventStore.write(e.eventId(), inTheSameMillisecond().toCloudEvents(List.of(e))));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        // What a replay that stored event0's position and then stopped would have left behind
        storage.save("subscription", CatchupTimeCheckpoint.of(asTime(SAME_MILLISECOND), new StringBasedCheckpoint("stored live start"), BEGINNING_OF_TIME));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        model.subscribe("subscription", StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();

        assertThat(received).as("event1 and event2, written in the same millisecond as the stored position").contains(history.get(1).eventId(), history.get(2).eventId());
    }

    @Test
    void a_restart_on_a_position_store_from_a_time_position_its_replay_stored_delivers_the_rest_of_the_history() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        assertThat(eventStore.writesPosition()).isTrue();
        List<NameDefined> history = Stream.of(0, 1, 2).map(i -> new NameDefined(UUID.randomUUID().toString(), time.plusSeconds(i), "name", "event" + i)).toList();
        CloudEventConverter<DomainEvent> atTheirOwnTime = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test"))
                .idMapper(DomainEvent::eventId).timeMapper(e -> e.timestamp().toInstant().atOffset(ZoneOffset.UTC)).build();
        history.forEach(e -> eventStore.write(e.eventId(), atTheirOwnTime.toCloudEvents(List.of(e))));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        // What a replay from StartAtTime.offsetDateTime(..) that stored event0's time and then stopped would have left behind
        storage.save("subscription", CatchupTimeCheckpoint.of(asTime(history.get(0).timestamp().toInstant().atOffset(ZoneOffset.UTC)), new StringBasedCheckpoint("stored live start"), BEGINNING_OF_TIME));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        model.subscribe("subscription", StartAt.subscriptionModelDefault(), e -> received.add(e.getId())).waitUntilStarted();

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the history after the stored time position").contains(history.get(1).eventId(), history.get(2).eventId()));
    }

    @Test
    void a_time_replay_that_stop_cut_short_delivers_every_event_when_they_share_the_stored_millisecond() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<NameDefined> history = Stream.of("event0", "event1", "event2", "event3", "event4", "event5").map(this::nameDefined).toList();
        history.forEach(e -> eventStore.write(e.eventId(), inTheSameMillisecond().toCloudEvents(List.of(e))));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        CountDownLatch handlingThirdEvent = new CountDownLatch(1);
        CountDownLatch releaseThirdEvent = new CountDownLatch(1);
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel model = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        model.subscribe("subscription", StartAtTime.beginningOfTime(), e -> {
            received.add(e.getId());
            if (received.size() == 3) {
                handlingThirdEvent.countDown();
                awaitLatch(releaseThirdEvent);
            }
        });
        awaitLatch(handlingThirdEvent);

        model.stop();
        model.start(true);
        releaseThirdEvent.countDown();

        List<String> historyIds = history.stream().map(DomainEvent::eventId).toList();
        await().atMost(Duration.ofSeconds(5)).until(() -> !model.isCatchingUp("subscription"));
        assertThat(received).as("every history event delivered across the stop and start(true)").containsAll(historyIds);
    }

    @Test
    void a_stored_live_start_the_model_still_has_is_kept_and_the_replay_continues_from_the_stored_position() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2, storedLiveStart, 0, 4)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history.subList(2, 4)));
        // Once before the replay and once before the handover, since a replay from the beginning is not probed when subscribed
        assertThat(model.resumeProbes).containsExactly(storedLiveStart, storedLiveStart);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString());
    }

    @Test
    void a_stored_live_start_the_model_no_longer_has_replays_again_from_the_beginning_and_goes_live_from_a_start_read_now() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(3, agedOut, 0, 3)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.resumeProbes).containsExactly(agedOut, liveStartReadNow);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
    }

    @Test
    void a_stored_live_start_the_model_no_longer_has_is_refused_when_the_replay_began_after_the_beginning() {
        // Given a stored position whose live start the model has lost, from a replay that began at position 1
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        Checkpoint stored = GlobalCheckpoint.of(3, agedOut, 1, 3);
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        storage.save("subscription", stored);
        saved.clear();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When the subscription restarts
            Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)));

            // Then the subscribe is refused, naming the subscription and the origin
            assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from position 1")
                    .hasMessageContaining("Start the subscription from the beginning again");
            // And nothing was replayed, stored or handed over
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNotNull().extracting(Checkpoint::asString).isEqualTo(stored.asString());
            assertThat(model.resumeProbes).as("asked once, about the stored live start").containsExactly(agedOut);
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_live_start_the_model_loses_during_a_replay_that_began_after_the_beginning_is_refused_instead_of_replaying_again() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2, storedLiveStart, 1, 4)), cloudEvent -> {
                model.lost.add(storedLiveStart);
                toDomainEvents(received).accept(cloudEvent);
            });

            assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(20)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from position 1");
            assertThat(received).as("the rest of the stored replay, and no replay again").containsExactly(history.get(2), history.get(3));
            assertThat(model.resumeProbes).as("once when the subscribe is made, once before the replay and once after it").containsExactly(storedLiveStart, storedLiveStart, storedLiveStart);
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_start_at_a_bare_position_above_0_is_refused_and_nothing_is_delivered_or_stored() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3")).forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(1)), toDomainEvents(received));
            Throwable failure = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(20)));

            // Then the start is refused, naming the subscription, the position and the checkpoint to start from instead
            assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("after position 1")
                    .hasMessageContaining("EventMetadata.getCheckpoint()");
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNull();
            assertThat(model.resumeProbes).as("asked about a live start").isEmpty();
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_position_stored_without_a_live_start_is_refused_at_a_restart_and_the_stored_checkpoint_is_left_as_it_was() {
        // Given a position above 0 stored by a version that kept no live start beside it
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3")).forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        storage.save("subscription", GlobalCheckpoint.of(2));
        saved.clear();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When the subscription restarts
            Subscription started = subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received));
            Throwable failure = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(20)));

            // Then
            assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("after position 2");
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNotNull().extracting(Checkpoint::asString).isEqualTo(GlobalCheckpoint.of(2).asString());
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_start_at_position_0_without_a_live_start_reads_one_now_and_asks_the_model_about_it_only_after_the_replay() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.resumeProbes).containsExactly(liveStartReadNow);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
    }

    @Test
    void a_live_start_the_model_loses_during_the_replay_replays_again_from_the_origin_and_goes_live_from_a_start_read_now() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2, storedLiveStart, 0, 4)), cloudEvent -> {
            model.lost.add(storedLiveStart);
            toDomainEvents(received).accept(cloudEvent);
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(liveStartReadNow).toString()));
        assertThat(received).as("the rest of the stored replay, then all of the history again from the origin")
                .containsExactly(history.get(2), history.get(3), history.get(0), history.get(1), history.get(2), history.get(3));
        assertThat(model.resumeProbes).containsExactly(storedLiveStart, storedLiveStart, liveStartReadNow);
    }

    @Test
    @Timeout(30)
    void a_catch_up_whose_live_start_the_model_loses_during_every_replay_fails_after_four_replays_instead_of_replaying_forever() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        model.losesEveryLiveStart = true;
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

            assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(20)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("4 replays")
                    .hasMessageMatching("(?s).*replays\\. The longest replay took \\d+ ms .*")
                    .hasMessageContaining("oplog");
            assertThat(received).as("the whole history once for each of the 4 replays")
                    .containsExactlyElementsOf(Stream.generate(() -> history).limit(4).flatMap(List::stream).toList());
            assertThat(model.resumeProbes).as("asked once after each replay").hasSize(4);
            assertThat(model.liveStarts).as("nothing handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stored_live_start_the_model_no_longer_has_counts_the_replay_from_the_origin_as_one_run_again() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            // When
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2, agedOut, 0, 3)), toDomainEvents(received));
            Throwable failure = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(20)));

            // Then
            assertThat(failure).as("the failure once the replay from the origin and the 3 replays after it lost the live start")
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("4 replays");
            assertThat(received).as("the whole history once for each replay from the origin")
                    .containsExactlyElementsOf(Stream.generate(() -> history).limit(3).flatMap(List::stream).toList());
            assertThat(model.resumeProbes).as("the stored live start, then the one read for each replay from the origin").hasSize(4).first().isEqualTo(agedOut);
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(60)
    void a_failed_catch_up_nobody_waits_for_is_logged_at_error() {
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        write(eventStore, nameDefined("event1"));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            // Never waited for, as the Spring Boot starter does with a subscription that replays history by default
            subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), __ -> {
            });

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(logged.list)
                    .filteredOn(event -> event.getLevel() == Level.ERROR)
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.getFormattedMessage()).contains("subscription").contains("held paused");
                        assertThat(((ThrowableProxy) event.getThrowableProxy()).getThrowable()).isInstanceOf(CatchupCannotResumeException.class);
                    }));
        } finally {
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_replay_from_the_beginning_that_loses_the_live_start_each_time_is_held_paused_with_its_id_taken_and_a_resume_replays_it_again_and_hands_over() {
        // Given a history and a model that loses every live start
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        HeldPausedReports reports = new HeldPausedReports();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            subscription.addStartRefusalListener(reports);

            // When the replay from the beginning has lost its live start in each of its 4 replays
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));
            Throwable failure = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(20)));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").hasSize(1));

            // Then the handle failed, and the subscription is held paused with its id taken
            Throwable subscribeAgain = catchThrowable(() -> subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)));
            assertAll(
                    () -> assertThat(failure).as("the failure of the handle").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("4 replays"),
                    () -> assertThat(reports.heldPaused.get(0)).as("what the listener was told").isSameAs(failure),
                    () -> assertThat(subscription.isPaused("subscription")).as("paused").isTrue(),
                    () -> assertThat(subscription.isRunning("subscription")).as("running").isFalse(),
                    () -> assertThat(subscribeAgain).as("what a subscribe of the same id throws").isInstanceOf(DuplicateSubscriptionIdException.class)
            );

            // When the model has the live start again and the subscription is resumed
            model.losesEveryLiveStart = false;
            received.clear();
            Subscription resumed = subscription.resumeSubscription("subscription");

            // Then it replays again from the beginning and goes live, since that skips nothing
            assertThat(resumed.waitUntilStarted(Duration.ofSeconds(20))).as("the resumed subscription started").isTrue();
            assertThat(received).as("the whole history, replayed again").containsExactlyElementsOf(history);
            assertThat(model.liveStarts).as("handed over to the live model once").hasSize(1);
            assertThat(subscription.isRunning("subscription")).as("running").isTrue();
            assertThat(subscription.isPaused("subscription")).as("paused").isFalse();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_replay_that_started_above_the_beginning_and_loses_its_live_start_is_held_paused_and_recorded_so_a_resume_refuses_and_a_start_replays_nothing_until_it_is_cancelled() {
        // Given a stored position above 0 whose live start the model still has when the replay starts, and loses during it
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStart = new StringBasedCheckpoint("stored live start");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.answersTrueToTheFirstProbes = 2;
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        HeldPausedReports reports = new HeldPausedReports();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            subscription.addStartRefusalListener(reports);

            // When
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(1, liveStart, 1, 4)), toDomainEvents(received));
            Throwable failure = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(20)));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").hasSize(1));

            // Then
            assertAll(
                    () -> assertThat(failure).as("the failure of the handle").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("again from position 1"),
                    () -> assertThat(reports.heldPaused.get(0)).as("what the listener was told").isSameAs(failure),
                    () -> assertThat(received).as("the events after the stored position").containsExactlyElementsOf(history.subList(1, 4)),
                    () -> assertThat(subscription.isPaused("subscription")).as("paused").isTrue(),
                    () -> assertThat(model.liveStarts).as("handed over").isEmpty()
            );

            // When it is resumed or started
            model.answersTrueToTheFirstProbes = Integer.MAX_VALUE;
            received.clear();
            Throwable resume = catchThrowable(() -> subscription.resumeSubscription("subscription"));
            Throwable start = catchThrowable(() -> subscription.start(true));

            // Then the resume refuses, and nothing replays
            assertAll(
                    () -> assertThat(resume).as("what the resume throws").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("subscription"),
                    () -> assertThat(start).as("what start(true) throws").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("subscription"),
                    () -> assertThat(received).as("events delivered by a replay again").isEmpty(),
                    () -> assertThat(subscription.isPaused("subscription")).as("paused").isTrue(),
                    () -> assertThat(model.liveStarts).as("handed over").isEmpty()
            );

            // When it is cancelled
            subscription.cancelSubscription("subscription");
            Subscription again = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

            // Then the id subscribes again and goes live
            assertThat(again.waitUntilStarted(Duration.ofSeconds(20))).as("the subscription made after the cancel started").isTrue();
            assertThat(subscription.isRunning("subscription")).as("running").isTrue();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_catch_up_listener_that_fails_when_the_replay_runs_again_because_the_live_start_was_lost_holds_the_replay_and_a_resume_runs_it_and_hands_over() {
        // Given a history, a live start the model loses during the first replay, and a listener that fails the second time a catch-up starts
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        AtomicInteger starts = new AtomicInteger();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        subscription.listenForCatchup("subscription", new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                if (starts.incrementAndGet() == 2) {
                    throw new IllegalStateException("The listener failed");
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        try {
            // When the subscription is made, so the replay runs again once the model lost the live start
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received));

            // Then the replay is held, so the subscription reads as paused and not running
            await().untilAsserted(() -> assertThat(subscription.hasParkedReplay("subscription")).as("the replay is held").isTrue());
            assertAll(
                    () -> assertThat(subscription.isPaused("subscription")).as("paused").isTrue(),
                    () -> assertThat(subscription.isRunning("subscription")).as("running").isFalse(),
                    () -> assertThat(subscription.isCatchingUp("subscription")).as("catching up").isFalse(),
                    () -> assertThat(model.liveStarts).as("nothing handed over").isEmpty()
            );
            // And the failure of the listener is logged at ERROR
            assertThat(logged.list).filteredOn(event -> event.getLevel() == Level.ERROR && event.getFormattedMessage().contains("subscription")
                    && event.getFormattedMessage().contains("catch-up listener")).hasSize(1);
            // And a resume, once the model has the live start, runs the replay again and hands over
            model.losesEveryLiveStart = false;
            Subscription resumed = subscription.resumeSubscription("subscription");
            assertThat(resumed.waitUntilStarted(Duration.ofSeconds(15))).as("the replay handed over").isTrue();
            assertThat(started.waitUntilStarted(Duration.ofSeconds(15))).as("the handle the subscribe returned started").isTrue();
            assertThat(received).as("the history").containsAll(history);
            assertThat(model.liveStarts).as("handed over once").hasSize(1);
            assertThat(subscription.isPaused("subscription")).as("paused after the resume").isFalse();
            // And what is written after the hand over is delivered
            NameDefined afterHandOver = nameDefined("afterHandOver");
            write(eventStore, afterHandOver);
            await().untilAsserted(() -> assertThat(received).contains(afterHandOver));
        } finally {
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    void a_resume_from_a_stored_live_start_replays_no_further_than_the_stored_replay_end() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"), nameDefined("event4"));
        history.forEach(event -> write(eventStore, event));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        // event4 is above the stored replay end, so live delivery from the stored live start has it
        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2, storedLiveStart, 0, 3)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString()));
        assertThat(received).containsExactlyElementsOf(history.subList(2, 3));
    }

    @Test
    void a_time_catch_up_stores_the_live_start_it_read_before_the_replay_with_every_time_it_stores() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        AtomicInteger liveStartReadsWhenTheFirstEventArrived = new AtomicInteger(-1);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(savesRecordedIn(saved)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            liveStartReadsWhenTheFirstEventArrived.compareAndSet(-1, model.globalCheckpointReads.get());
            toDomainEvents(received).accept(cloudEvent);
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(liveStartReadsWhenTheFirstEventArrived).as("the live start is read once, before the first event of the replay").hasValue(1);
        assertThat(catchupTimeCheckpointsIn(saved))
                .as("the checkpoints the replay stores carry the live start and the time the replay started from")
                .containsExactly(CatchupTimeCheckpoint.of(asTime(at(1)), liveStartReadNow, BEGINNING_OF_TIME), CatchupTimeCheckpoint.of(asTime(at(2)), liveStartReadNow, BEGINNING_OF_TIME),
                        CatchupTimeCheckpoint.of(asTime(at(3)), liveStartReadNow, BEGINNING_OF_TIME));
        assertThat(model.resumeProbes).containsExactly(liveStartReadNow);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
    }

    @Test
    void a_stored_catch_up_time_whose_live_start_the_model_still_has_is_kept_and_the_replay_continues_from_the_stored_time() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(CatchupTimeCheckpoint.of(asTime(at(3)), storedLiveStart, asTime(at(2)))), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history.subList(2, 4)));
        // Once when the subscribe is made, once before the replay and once before the handover
        assertThat(model.resumeProbes).containsExactly(storedLiveStart, storedLiveStart, storedLiveStart);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString());
        assertThat(model.globalCheckpointReads).hasValue(0);
    }

    @Test
    void a_stored_catch_up_time_whose_live_start_the_model_no_longer_has_replays_again_from_the_beginning_of_time_and_goes_live_from_a_start_read_now() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));

        subscription.subscribe("subscription", StartAt.checkpoint(CatchupTimeCheckpoint.of(asTime(at(3)), agedOut, BEGINNING_OF_TIME)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.resumeProbes).containsExactly(agedOut, liveStartReadNow);
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
    }

    @Test
    void a_stored_catch_up_time_whose_live_start_the_model_no_longer_has_is_refused_when_the_replay_began_after_the_beginning_of_time() {
        // Given a stored time whose live start the model has lost, from a replay that began at the time of event2
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        Checkpoint stored = CatchupTimeCheckpoint.of(asTime(at(3)), agedOut, asTime(at(2)));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        storage.save("subscription", stored);
        saved.clear();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When the subscription restarts
            Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)));

            // Then the subscribe is refused, naming the subscription and the origin
            assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from time " + asTime(at(2)))
                    .hasMessageContaining("Start the subscription from the beginning again");
            // And nothing was replayed, stored or handed over
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNotNull().extracting(Checkpoint::asString).isEqualTo(stored.asString());
            assertThat(model.resumeProbes).as("asked once, about the stored live start").containsExactly(agedOut);
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_catch_up_time_read_back_from_a_storage_that_keeps_strings_is_refused_the_same_way() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        Checkpoint readBackAsAString = new StringBasedCheckpoint(CatchupTimeCheckpoint.of(asTime(at(3)), agedOut, asTime(at(2))).asString());
        try {
            assertThatThrownBy(() -> subscription.subscribe("subscription", StartAt.checkpoint(readBackAsAString), toDomainEvents(received)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from time " + asTime(at(2)));
            assertThat(received).as("delivered").isEmpty();
            assertThat(model.resumeProbes).containsExactly(agedOut);
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    void a_stored_catch_up_time_resumes_from_storage_when_the_subscription_starts_at_the_model_default() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", CatchupTimeCheckpoint.of(asTime(at(3)), storedLiveStart, asTime(at(2))));
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history.subList(2, 4)));
        assertThat(model.liveStarts).containsExactly(StartAt.checkpoint(storedLiveStart).toString());
        assertThat(model.globalCheckpointReads).hasValue(0);
    }

    @Test
    void a_live_start_the_model_loses_during_a_time_replay_that_began_after_the_beginning_of_time_is_refused_instead_of_replaying_again() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        StringBasedCheckpoint storedLiveStart = new StringBasedCheckpoint("stored live start");
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(CatchupTimeCheckpoint.of(asTime(at(3)), storedLiveStart, asTime(at(2)))), cloudEvent -> {
                model.lost.add(storedLiveStart);
                toDomainEvents(received).accept(cloudEvent);
            });

            assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(20)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from time " + asTime(at(2)));
            assertThat(received).as("the rest of the stored replay, and no replay again").containsExactly(history.get(2), history.get(3));
            assertThat(model.resumeProbes).as("once when the subscribe is made, once before the replay and once after it").containsExactly(storedLiveStart, storedLiveStart, storedLiveStart);
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_specific_time_start_whose_live_start_is_lost_before_the_handover_is_refused_at_once_instead_of_replaying_again() {
        // Given a start at the time of event2, whose live start the model loses during the replay
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            // When
            Subscription started = subscription.subscribe("subscription", StartAtTime.offsetDateTime(at(2)), toDomainEvents(received));

            // Then it fails at once, where it replayed again 3 times before
            assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(20)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("again from time " + asTime(at(2)))
                    .hasMessageNotContaining("replays");
            assertThat(received).as("the history from the time of event2, once").containsExactlyElementsOf(history.subList(1, 4));
            assertThat(model.resumeProbes).as("asked once, after the only replay").hasSize(1);
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_time_catch_up_whose_live_start_the_model_loses_during_every_replay_fails_after_four_replays_instead_of_replaying_forever() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        model.losesEveryLiveStart = true;
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        try {
            Subscription started = subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received));

            assertThatThrownBy(() -> started.waitUntilStarted(Duration.ofSeconds(20)))
                    .isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining("4 replays")
                    .hasMessageMatching("(?s).*replays\\. The longest replay took \\d+ ms .*")
                    .hasMessageContaining("oplog");
            assertThat(received).as("the whole history once for each of the 4 replays")
                    .containsExactlyElementsOf(Stream.generate(() -> history).limit(4).flatMap(List::stream).toList());
            assertThat(model.resumeProbes).as("asked once after each replay").hasSize(4);
            assertThat(model.liveStarts).as("nothing handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    static Stream<SortBy> sortsThatDoNotReadInAscendingTime() {
        return Stream.of(SortBy.natural(SortBy.SortDirection.ASCENDING), SortBy.descending(CloudEventV1.TIME));
    }

    @ParameterizedTest
    @MethodSource("sortsThatDoNotReadInAscendingTime")
    void a_time_replay_sorted_other_than_by_ascending_time_stores_the_time_it_started_from_so_a_restart_after_a_crash_delivers_every_event(SortBy catchupSort) {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event4", at(4)), writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event3", at(3)));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)).catchupPhaseSortBy(catchupSort);
        CountDownLatch handlingThirdEvent = new CountDownLatch(1);
        CountDownLatch crashed = new CountDownLatch(1);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel first = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, config);
        StartAt fromTheStoredTimeOrTheBeginning = StartAt.dynamic(__ -> storage.exists("subscription") ? StartAt.subscriptionModelDefault() : StartAtTime.beginningOfTime());

        // When
        first.subscribe("subscription", fromTheStoredTimeOrTheBeginning, cloudEvent -> {
            toDomainEvents(received).accept(cloudEvent);
            if (received.size() == 3) {
                handlingThirdEvent.countDown();
                awaitLatch(crashed);
            }
        });
        awaitLatch(handlingThirdEvent);
        Checkpoint storedAtTheCrash = storage.read("subscription");
        first.shutdown();
        crashed.countDown();

        // Then
        CatchupTimeCheckpoint attemptStart = CatchupTimeCheckpoint.of(BEGINNING_OF_TIME, new StringBasedCheckpoint("in-memory-global-position"), BEGINNING_OF_TIME);
        assertThat(storedAtTheCrash).as("stored after the second event of a bulk read that is not in ascending time").isEqualTo(attemptStart);
        assertThat(catchupTimeCheckpointsIn(saved)).as("stored for each event of the bulk read").containsOnly(attemptStart);
        InMemorySubscriptionModel restartedLive = new InMemorySubscriptionModel();
        try {
            CopyOnWriteArrayList<DomainEvent> afterRestart = new CopyOnWriteArrayList<>();
            StreamCatchupSubscriptionModel second = new StreamCatchupSubscriptionModel(new CheckpointAwareInMemorySubscriptionModel(restartedLive), eventStore, config);
            second.subscribe("subscription", fromTheStoredTimeOrTheBeginning, toDomainEvents(afterRestart)).waitUntilStarted();
            assertThat(afterRestart).as("delivered by the restart").containsExactlyInAnyOrderElementsOf(history);
        } finally {
            restartedLive.shutdown();
        }
    }

    @Test
    void a_time_replay_under_the_default_sort_stores_the_time_of_each_event_it_delivered() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event3", at(3)));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(savesRecordedIn(saved)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // When
        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received)).waitUntilStarted();

        // Then
        Checkpoint liveStart = new StringBasedCheckpoint("in-memory-global-position");
        assertThat(received).as("delivered in ascending time").containsExactly(history.get(1), history.get(0), history.get(2));
        assertThat(catchupTimeCheckpointsIn(saved)).as("stored for each event of the bulk read")
                .containsExactly(CatchupTimeCheckpoint.of(asTime(at(1)), liveStart, BEGINNING_OF_TIME), CatchupTimeCheckpoint.of(asTime(at(2)), liveStart, BEGINNING_OF_TIME),
                        CatchupTimeCheckpoint.of(asTime(at(3)), liveStart, BEGINNING_OF_TIME));
    }

    @Test
    void an_event_a_time_replay_reconciles_after_a_bulk_read_in_insertion_order_stores_its_own_time() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        writeAt(eventStore, "event2", at(2));
        writeAt(eventStore, "event1", at(1));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(savesRecordedIn(saved)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1))
                        .catchupPhaseSortBy(SortBy.natural(SortBy.SortDirection.ASCENDING)));

        // When
        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            toDomainEvents(received).accept(cloudEvent);
            // Written from inside the bulk read, so the replay reconciles it in a window after it
            if (received.size() == 1) {
                writeAt(eventStore, "writtenDuringTheReplay", at(3));
            }
        }).waitUntilStarted();

        // Then
        Checkpoint liveStart = new StringBasedCheckpoint("in-memory-global-position");
        CatchupTimeCheckpoint attemptStart = CatchupTimeCheckpoint.of(BEGINNING_OF_TIME, liveStart, BEGINNING_OF_TIME);
        assertThat(received).extracting(event -> ((NameDefined) event).name()).as("delivered").containsExactly("event2", "event1", "writtenDuringTheReplay");
        assertThat(catchupTimeCheckpointsIn(saved)).as("the time the attempt started from for the bulk read, then the time of the reconciled event")
                .containsExactly(attemptStart, attemptStart, CatchupTimeCheckpoint.of(asTime(at(3)), liveStart, BEGINNING_OF_TIME));
    }

    @Test
    void a_beginning_of_time_replay_delivers_events_without_a_time_and_stores_no_time_for_them() {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        writeWithoutATime(eventStore, "withoutATime");
        writeAt(eventStore, "event1", at(1));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(savesRecordedIn(saved)).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        AtomicBoolean writtenDuringTheReplay = new AtomicBoolean();
        AtomicBoolean started = new AtomicBoolean();

        // When
        Throwable failure = catchThrowable(() -> started.set(subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            toDomainEvents(received).accept(cloudEvent);
            // Written from inside the bulk read, so the replay reconciles it after event1
            if (cloudEvent.getTime() != null && writtenDuringTheReplay.compareAndSet(false, true)) {
                writeWithoutATime(eventStore, "reconciledWithoutATime");
            }
        }).waitUntilStarted(Duration.ofSeconds(10))));

        // Then
        Checkpoint liveStart = new StringBasedCheckpoint("in-memory-global-position");
        CatchupTimeCheckpoint attemptStart = CatchupTimeCheckpoint.of(BEGINNING_OF_TIME, liveStart, BEGINNING_OF_TIME);
        CatchupTimeCheckpoint atEvent1 = CatchupTimeCheckpoint.of(asTime(at(1)), liveStart, BEGINNING_OF_TIME);
        assertThat(failure).as("the replay failure").isNull();
        assertThat(started).as("started").isTrue();
        assertThat(received).extracting(event -> ((NameDefined) event).name()).as("delivered").containsExactly("withoutATime", "event1", "reconciledWithoutATime");
        assertThat(catchupTimeCheckpointsIn(saved)).as("stored for each event, an event without a time keeping the one before it")
                .containsExactly(attemptStart, atEvent1, atEvent1);
    }

    @Test
    void a_stored_plain_time_is_refused_at_the_default_start_and_the_stored_checkpoint_is_left_as_it_was() {
        // Given a time stored without a live start
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)), writeAt(eventStore, "event4", at(4)));
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, new StringBasedCheckpoint("live start read now"));
        CopyOnWriteArrayList<String> saved = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = savesRecordedIn(saved);
        storage.save("subscription", TimeBasedCheckpoint.from(at(3)));
        saved.clear();
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        try {
            // When the subscription starts at the model default
            Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)));

            // Then the start is refused at once, naming the subscription and the fix
            assertThat(failure).isInstanceOf(CatchupCannotResumeException.class)
                    .hasMessageContaining("subscription")
                    .hasMessageContaining(asTime(at(3)))
                    .hasMessageContaining("holds no live start")
                    .hasMessageContaining("EventMetadata.getCheckpoint()");
            // And nothing was delivered, stored or handed over
            assertThat(received).as("delivered").isEmpty();
            assertThat(saved).as("stored").isEmpty();
            assertThat(storage.read("subscription")).as("the stored checkpoint").isNotNull().extracting(Checkpoint::asString).isEqualTo(TimeBasedCheckpoint.from(at(3)).asString());
            assertThat(model.resumeProbes).as("asked about a live start").isEmpty();
            assertThat(model.liveStarts).as("handed over").isEmpty();
        } finally {
            subscription.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("beginningOfTimeAsStored")
    void a_stored_beginning_of_time_is_not_refused_at_the_default_start_and_replays_the_history(Checkpoint storedBeginningOfTime) {
        // Given the beginning of time stored without a live start, no event has an earlier time
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", storedBeginningOfTime);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        // When the subscription starts at the model default
        Throwable failure = catchThrowable(() -> subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted(Duration.ofSeconds(10)));

        // Then the start is not refused and every event is delivered
        assertThat(failure).as("the start failure").isNull();
        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
    }

    @Test
    void a_replay_origin_at_the_beginning_of_time_read_back_as_a_string_still_replays_every_event() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        // Only a replay that knows it starts at the beginning of time reads an event older than the epoch
        List<DomainEvent> history = List.of(writeAt(eventStore, "beforeTheEpoch", OffsetDateTime.parse("1969-12-31T23:59:59Z")), writeAt(eventStore, "event1", at(1)),
                writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        StringBasedCheckpoint agedOut = new StringBasedCheckpoint("aged out live start");
        model.lost.add(agedOut);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100));
        Checkpoint readBackAsAString = new StringBasedCheckpoint(CatchupTimeCheckpoint.of(asTime(at(2)), agedOut, BEGINNING_OF_TIME).asString());

        subscription.subscribe("subscription", StartAt.checkpoint(readBackAsAString), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.resumeProbes).containsExactly(agedOut, liveStartReadNow);
    }

    static Stream<Checkpoint> catchUpCheckpointsOfTheTimeKind() {
        StringBasedCheckpoint someLiveStart = new StringBasedCheckpoint("some live start");
        return Stream.of(CatchupTimeCheckpoint.of(asTime(at(2)), someLiveStart, BEGINNING_OF_TIME), TimeBasedCheckpoint.from(at(2)), new StringBasedCheckpoint(asTime(at(2))));
    }

    static Stream<Checkpoint> beginningOfTimeAsStored() {
        return Stream.of(TimeBasedCheckpoint.beginningOfTime(), new StringBasedCheckpoint(BEGINNING_OF_TIME));
    }

    static Stream<Checkpoint> catchUpCheckpointsOfThePositionKind() {
        return Stream.of(GlobalCheckpoint.of(2), GlobalCheckpoint.of(2, new StringBasedCheckpoint("some live start"), 0, 4));
    }

    @ParameterizedTest
    @MethodSource("catchUpCheckpointsOfTheTimeKind")
    void a_position_catch_up_replaces_a_stored_checkpoint_written_by_a_time_catch_up_with_the_live_start_it_read_before_the_replay(Checkpoint storedByATimeCatchUp) {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        List<DomainEvent> history = List.of(nameDefined("event1"), nameDefined("event2"), nameDefined("event3"));
        history.forEach(event -> write(eventStore, event));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", storedByATimeCatchUp);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.liveStarts).as("the wrapped model goes live from the live start read before the replay, not from the checkpoint the other kind of catch-up stored")
                .containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
        assertThat(storage.read("subscription")).isEqualTo(liveStartReadNow);
    }

    @ParameterizedTest
    @MethodSource("catchUpCheckpointsOfThePositionKind")
    void a_time_catch_up_replaces_a_stored_checkpoint_written_by_a_position_catch_up_with_the_live_start_it_read_before_the_replay(Checkpoint storedByAPositionCatchUp) {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        List<DomainEvent> history = List.of(writeAt(eventStore, "event1", at(1)), writeAt(eventStore, "event2", at(2)), writeAt(eventStore, "event3", at(3)));
        StringBasedCheckpoint liveStartReadNow = new StringBasedCheckpoint("live start read now");
        CheckpointAwareInMemorySubscriptionModel model = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel, liveStartReadNow);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", storedByAPositionCatchUp);
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(model, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));

        subscription.subscribe("subscription", StartAtTime.beginningOfTime(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(history));
        assertThat(model.liveStarts).as("the wrapped model goes live from the live start read before the replay, not from the checkpoint the other kind of catch-up stored")
                .containsExactly(StartAt.checkpoint(liveStartReadNow).toString());
        assertThat(storage.read("subscription")).isEqualTo(liveStartReadNow);
    }

    private static final OffsetDateTime BASE_TIME = OffsetDateTime.parse("2026-01-01T10:00:00Z");
    private static final String BEGINNING_OF_TIME = TimeBasedCheckpoint.beginningOfTime().asString();

    private static OffsetDateTime at(int seconds) {
        return BASE_TIME.plusSeconds(seconds);
    }

    private static String asTime(OffsetDateTime time) {
        return TimeBasedCheckpoint.from(time).asString();
    }

    private static List<CatchupTimeCheckpoint> catchupTimeCheckpointsIn(List<String> saved) {
        return saved.stream().map(StringBasedCheckpoint::new).filter(CatchupTimeCheckpoint::isCatchupTimeCheckpoint).map(CatchupTimeCheckpoint::parse).toList();
    }

    private static List<String> markedTimesIn(List<String> saved) {
        return saved.stream().filter(checkpoint -> CatchupCheckpoints.isMarkedTime(new StringBasedCheckpoint(checkpoint))).toList();
    }

    private static InMemoryCheckpointStorage savesRecordedIn(List<String> saved) {
        return new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saved.add(checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
    }

    private NameDefined writeAt(InMemoryEventStore eventStore, String name, OffsetDateTime at) {
        NameDefined event = nameDefined(name);
        CloudEventConverter<DomainEvent> atThatTime = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).timeMapper(__ -> at).build();
        eventStore.write(event.eventId(), atThatTime.toCloudEvents(List.of(event)));
        return event;
    }

    private void writeWithoutATime(InMemoryEventStore eventStore, String name) {
        NameDefined event = nameDefined(name);
        CloudEventConverter<DomainEvent> withoutATime = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).timeMapper(__ -> null).build();
        eventStore.write(event.eventId(), withoutATime.toCloudEvents(List.of(event)));
    }

    private static final OffsetDateTime SAME_MILLISECOND = OffsetDateTime.parse("2026-01-01T10:00:00.123Z");

    private static CloudEventConverter<DomainEvent> inTheSameMillisecond() {
        return new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).timeMapper(__ -> SAME_MILLISECOND).build();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    /**
     * Issue #1517. cancelSubscription waits a bounded time for a replay thread that is inside the subscriber's action
     * and then gives up, so a subscribe of the same id right after it starts a fresh attempt whose replay must still
     * not enter the action until the cancelled attempt's call has returned. The cancelled call stays in the action for
     * longer than that wait, so the fresh subscription has to hold back on its own.
     */
    @Test
    @Timeout(30)
    void a_fresh_subscribe_after_a_cancel_does_not_run_the_action_until_the_cancelled_attempts_call_has_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("history"));
        String subscriptionId = "subscription";
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
        AtomicInteger entered = new AtomicInteger();
        CountDownLatch cancelledCallEntered = new CountDownLatch(1);
        CountDownLatch freshCallEntered = new CountDownLatch(1);
        CountDownLatch releaseCancelledCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            int call = entered.incrementAndGet();
            calls.add("call " + call + " enters");
            if (call == 1) {
                cancelledCallEntered.countDown();
                awaitLatch(releaseCancelledCall);
            } else {
                freshCallEntered.countDown();
            }
            calls.add("call " + call + " returns");
        };

        try {
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
            assertThat(cancelledCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();
            subscription.cancelSubscription(subscriptionId);

            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
            boolean freshCallEnteredWhileTheCancelledCallIsInTheAction = freshCallEntered.await(1500, TimeUnit.MILLISECONDS);

            assertThat(freshCallEnteredWhileTheCancelledCallIsInTheAction)
                    .as("the fresh subscription's first call entered the action while the cancelled attempt's call was still in it, calls: %s", calls)
                    .isFalse();

            releaseCancelledCall.countDown();
            assertThat(freshCallEntered.await(5, TimeUnit.SECONDS)).as("the fresh subscription's first call enters once the cancelled call has returned").isTrue();
            assertThat(calls).as("the order the action was entered and left in").startsWith("call 1 enters", "call 1 returns", "call 2 enters");
        } finally {
            releaseCancelledCall.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issue #1517, for a replay a stop parked while its call was still in the action. The cancel drops the parked
     * replay, and a fresh subscribe of the id, which runs once the model is started again, still must not enter the
     * action until the call the stopped replay made has returned.
     */
    @Test
    @Timeout(30)
    void a_fresh_subscribe_after_a_stop_and_a_cancel_does_not_run_the_action_until_the_stopped_replays_call_has_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("history"));
        String subscriptionId = "subscription";
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CountDownLatch releaseStoppedCall = new CountDownLatch(1);
        CountDownLatch freshCallEntered = new CountDownLatch(1);
        CountDownLatch stoppedCallEntered = new CountDownLatch(1);
        CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
        AtomicInteger entered = new AtomicInteger();
        Consumer<CloudEvent> recording = cloudEvent -> {
            int call = entered.incrementAndGet();
            calls.add("call " + call + " enters");
            if (call == 1) {
                stoppedCallEntered.countDown();
                awaitLatch(releaseStoppedCall);
            } else {
                freshCallEntered.countDown();
            }
            calls.add("call " + call + " returns");
        };

        try {
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), recording);
            assertThat(stoppedCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();
            subscription.stop();
            subscription.cancelSubscription(subscriptionId);
            subscription.subscribe(subscriptionId, StartAtTime.beginningOfTime(), recording);
            subscription.start(true);

            boolean freshCallEnteredWhileTheStoppedCallIsInTheAction = freshCallEntered.await(1500, TimeUnit.MILLISECONDS);

            assertThat(freshCallEnteredWhileTheStoppedCallIsInTheAction)
                    .as("the fresh subscription's first call entered the action while the stopped replay's call was still in it, calls: %s", calls)
                    .isFalse();

            releaseStoppedCall.countDown();
            assertThat(freshCallEntered.await(5, TimeUnit.SECONDS)).as("the fresh subscription's first call enters once the stopped replay's call has returned").isTrue();
            assertThat(calls).as("the order the action was entered and left in").startsWith("call 1 enters", "call 1 returns", "call 2 enters");
        } finally {
            releaseStoppedCall.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issues #1515 and #1526. A cancel waits for the action of an event its replay already let through, so the caller
     * knows no call is left running once it returns, and the replay lets no further event through.
     */
    @Test
    @Timeout(30)
    void a_cancel_returns_only_once_the_action_of_the_event_the_replay_delivered_has_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        Stream.of("event1", "event2", "event3").map(this::nameDefined).forEach(event -> write(eventStore, event));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        RecordedAction action = new RecordedAction(() -> sleepFor(200));
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(action.firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();

            long before = System.nanoTime();
            subscription.cancelSubscription("subscription");
            Duration took = Duration.ofNanos(System.nanoTime() - before);
            List<String> whenTheCancelReturned = List.copyOf(action.calls);

            assertThat(whenTheCancelReturned).as("the calls to the action when the cancel returned").containsExactly("call 1 enters", "call 1 returns");
            assertThat(took).as("how long the cancel took with the action returning after 200 ms").isLessThan(Duration.ofMillis(700));
            assertThat(stillRanWarnings(logged)).as("the warnings logged").isEmpty();
            sleepFor(500);
            assertThat(action.calls).as("the calls to the action after the cancel returned").containsExactly("call 1 enters", "call 1 returns");
        } finally {
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_returns_only_once_the_action_of_the_event_the_replay_delivered_has_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        Stream.of("event1", "event2", "event3").map(this::nameDefined).forEach(event -> write(eventStore, event));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        RecordedAction action = new RecordedAction(() -> sleepFor(200));
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(action.firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();

            long before = System.nanoTime();
            subscription.stop();
            Duration took = Duration.ofNanos(System.nanoTime() - before);
            List<String> whenTheStopReturned = List.copyOf(action.calls);

            assertThat(whenTheStopReturned).as("the calls to the action when the stop returned").containsExactly("call 1 enters", "call 1 returns");
            assertThat(took).as("how long the stop took with the action returning after 200 ms").isLessThan(Duration.ofMillis(700));
            assertThat(stillRanWarnings(logged)).as("the warnings logged").isEmpty();
            sleepFor(500);
            assertThat(action.calls).as("the calls to the action after the stop returned").containsExactly("call 1 enters", "call 1 returns");
        } finally {
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_returns_after_about_a_second_and_logs_a_warning_when_the_action_runs_longer_and_the_replay_delivers_no_further_event() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        Stream.of("event1", "event2", "event3").map(this::nameDefined).forEach(event -> write(eventStore, event));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CountDownLatch releaseTheAction = new CountDownLatch(1);
        RecordedAction action = new RecordedAction(() -> awaitLatch(releaseTheAction));
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(action.firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();

            long before = System.nanoTime();
            subscription.cancelSubscription("subscription");
            Duration took = Duration.ofNanos(System.nanoTime() - before);

            assertThat(took).as("how long the cancel took with the action still running").isBetween(Duration.ofMillis(900), Duration.ofSeconds(5));
            assertThat(action.calls).as("the calls to the action when the cancel returned").containsExactly("call 1 enters");
            assertThat(stillRanWarnings(logged)).as("the warnings logged").singleElement()
                    .satisfies(message -> assertThat(message).startsWith("A cancel returned while the action of subscription subscription still ran").contains("1000 ms"));

            releaseTheAction.countDown();
            sleepFor(500);
            assertThat(action.calls).as("the calls to the action once it was released").containsExactly("call 1 enters", "call 1 returns");
        } finally {
            releaseTheAction.countDown();
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_returns_after_about_a_second_and_logs_a_warning_when_the_action_runs_longer_and_the_replay_delivers_no_further_event() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        Stream.of("event1", "event2", "event3").map(this::nameDefined).forEach(event -> write(eventStore, event));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CountDownLatch releaseTheAction = new CountDownLatch(1);
        RecordedAction action = new RecordedAction(() -> awaitLatch(releaseTheAction));
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(action.firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();

            long before = System.nanoTime();
            subscription.stop();
            Duration took = Duration.ofNanos(System.nanoTime() - before);

            assertThat(took).as("how long the stop took with the action still running").isBetween(Duration.ofMillis(900), Duration.ofSeconds(5));
            assertThat(action.calls).as("the calls to the action when the stop returned").containsExactly("call 1 enters");
            assertThat(stillRanWarnings(logged)).as("the warnings logged").singleElement()
                    .satisfies(message -> assertThat(message).startsWith("A stop returned while the action of subscription subscription still ran").contains("1000 ms"));

            releaseTheAction.countDown();
            sleepFor(500);
            assertThat(action.calls).as("the calls to the action once it was released").containsExactly("call 1 enters", "call 1 returns");
        } finally {
            releaseTheAction.countDown();
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void two_replays_whose_actions_cancel_each_other_both_cancels_return_once_their_wait_is_over() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CountDownLatch xEntered = new CountDownLatch(1);
        CountDownLatch yEntered = new CountDownLatch(1);
        CountDownLatch xCancelledY = new CountDownLatch(1);
        CountDownLatch yCancelledX = new CountDownLatch(1);

        try {
            subscription.subscribe("x", StartAtTime.beginningOfTime(), cloudEvent -> {
                xEntered.countDown();
                awaitUpTo(yEntered, 5);
                subscription.cancelSubscription("y");
                xCancelledY.countDown();
            });
            subscription.subscribe("y", StartAtTime.beginningOfTime(), cloudEvent -> {
                yEntered.countDown();
                awaitUpTo(xEntered, 5);
                subscription.cancelSubscription("x");
                yCancelledX.countDown();
            });

            assertThat(xCancelledY.await(6, TimeUnit.SECONDS)).as("the cancel of y that x's action made returned").isTrue();
            assertThat(yCancelledX.await(6, TimeUnit.SECONDS)).as("the cancel of x that y's action made returned").isTrue();
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_called_from_inside_the_action_of_a_replay_returns_at_once_and_stops_the_model_once_the_action_has_returned() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        AtomicReference<Throwable> thrownByTheStop = new AtomicReference<>();
        CountDownLatch stopReturnedOrThrew = new CountDownLatch(1);
        CountDownLatch releaseTheAction = new CountDownLatch(1);

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
                thrownByTheStop.set(catchThrowable(subscription::stop));
                stopReturnedOrThrew.countDown();
                awaitLatch(releaseTheAction);
            });

            assertThat(stopReturnedOrThrew.await(5, TimeUnit.SECONDS)).as("the stop made from the action returned or threw while the action still ran").isTrue();
            assertThat(thrownByTheStop.get()).as("what the stop made from the action of a replay of the model threw").isNull();
            assertThat(subscription.isRunning()).as("whether the model runs while the action that made the stop still runs").isTrue();
            releaseTheAction.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(subscription.isRunning()).as("whether the model runs once the action that made the stop has returned").isFalse();
                assertThat(subscription.isRunning("subscription")).as("whether the subscription runs once the action that made the stop has returned").isFalse();
            });
        } finally {
            releaseTheAction.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issue #1258. A cancel that comes while the replay stores a position has to end with no position stored, whether
     * the write lands before or after the cancel deletes the stored one.
     */
    @Test
    @Timeout(30)
    void a_cancel_that_comes_while_the_replay_stores_a_position_leaves_no_stored_position_once_the_store_is_released() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        CountDownLatch saveEntered = new CountDownLatch(1);
        CountDownLatch releaseTheSave = new CountDownLatch(1);
        AtomicBoolean blocked = new AtomicBoolean();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                if (blocked.compareAndSet(false, true)) {
                    saveEntered.countDown();
                    awaitLatch(releaseTheSave);
                }
                return super.save(id, checkpoint, condition);
            }
        };
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();

            Thread cancel = new Thread(() -> subscription.cancelSubscription("subscription"));
            cancel.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);
            releaseTheSave.countDown();
            cancel.join(5000);

            assertThat(cancel.isAlive()).as("the cancel returned once the store was released").isFalse();
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();
            sleepFor(500);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription later").isNull();
        } finally {
            releaseTheSave.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issue #1258. The replay records its store as under way under the handover lock and stores after the lock is
     * released. A cancel that comes between the two has to wait for the store, not return ahead of it, and then delete
     * the position the store wrote.
     */
    @Test
    @Timeout(30)
    void a_cancel_that_comes_after_the_replay_released_the_handover_lock_for_a_store_returns_only_once_the_store_has_returned_and_deletes_the_stored_position() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        AtomicInteger saves = new AtomicInteger();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                saves.incrementAndGet();
                return super.save(id, checkpoint, condition);
            }
        };
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CountDownLatch replayInTheHook = new CountDownLatch(1);
        CountDownLatch releaseTheHook = new CountDownLatch(1);
        AtomicBoolean hooked = new AtomicBoolean();
        subscription.runBetweenHandoverLockAndStore(() -> {
            if (hooked.compareAndSet(false, true)) {
                replayInTheHook.countDown();
                awaitLatch(releaseTheHook);
            }
        });

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(replayInTheHook.await(5, TimeUnit.SECONDS)).as("the replay is between the handover lock and the store").isTrue();

            Thread cancel = new Thread(() -> subscription.cancelSubscription("subscription"));
            cancel.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() == Thread.State.WAITING);
            assertThat(cancel.isAlive()).as("the cancel is still waiting while the store is held at the hook").isTrue();
            assertThat(saves.get()).as("the stores that have been made while the first is held at the hook").isZero();

            releaseTheHook.countDown();
            cancel.join(5000);

            assertThat(cancel.isAlive()).as("the cancel returned once the store was released").isFalse();
            assertThat(saves.get()).as("the stores the cancel waited for").isGreaterThanOrEqualTo(1);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();
        } finally {
            releaseTheHook.countDown();
            subscription.shutdown();
        }
    }

    /**
     * Issue #1515. The replay lets an event through and a cancel reads the call that event's action is in under one
     * handover lock. A cancel that comes after the replay let the event through, but before it called the action, has
     * to wait for that call, not return ahead of it.
     */
    @Test
    @Timeout(30)
    void a_cancel_that_comes_after_the_replay_let_an_event_through_returns_only_once_the_action_for_that_event_has_returned_or_after_the_wait() throws InterruptedException {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CountDownLatch replayInTheHook = new CountDownLatch(1);
        CountDownLatch releaseTheHook = new CountDownLatch(1);
        AtomicBoolean hooked = new AtomicBoolean();
        AtomicLong sequence = new AtomicLong();
        AtomicLong actionStartedAt = new AtomicLong();
        AtomicLong cancelReturnedAt = new AtomicLong();
        AtomicInteger actionCalls = new AtomicInteger();
        subscription.runBetweenHandoverLockAndAction(() -> {
            if (hooked.compareAndSet(false, true)) {
                replayInTheHook.countDown();
                awaitLatch(releaseTheHook);
            }
        });

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
                if (actionCalls.incrementAndGet() == 1) {
                    actionStartedAt.set(sequence.incrementAndGet());
                }
            });
            assertThat(replayInTheHook.await(5, TimeUnit.SECONDS)).as("the replay is between the handover lock and the action").isTrue();

            Thread cancel = new Thread(() -> {
                subscription.cancelSubscription("subscription");
                cancelReturnedAt.set(sequence.incrementAndGet());
            });
            cancel.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> !cancel.isAlive() || cancel.getState() != Thread.State.RUNNABLE && Arrays.stream(cancel.getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals("completesWithin") || frame.getMethodName().equals("awaitTheAdmittedActions")));
            releaseTheHook.countDown();
            cancel.join(5000);

            assertThat(cancel.isAlive()).as("the cancel returned").isFalse();
            await().atMost(Duration.ofSeconds(5)).until(() -> actionStartedAt.get() != 0);
            assertThat(actionStartedAt.get()).as("the action for the first event started (sequence)").isPositive();
            assertThat(actionStartedAt.get()).as("the action for the first event started before the cancel returned").isLessThan(cancelReturnedAt.get());
            assertThat(actionCalls.get()).as("the calls to the action, the second event is not delivered").isEqualTo(1);
        } finally {
            releaseTheHook.countDown();
            subscription.shutdown();
        }
    }

    /**
     * A subscribe that goes straight to the live delegate has to wait for a cancelled replay whose call to the action
     * still runs, as a subscribe that starts a replay does, so the action never runs twice at once. The cancel gave up
     * waiting for that call after a second, and deleted the position, so the default start finds no position.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("startsThatGoStraightToLive")
    @Timeout(30)
    void a_subscribe_that_goes_straight_to_live_after_a_cancel_gave_up_on_a_replays_call_does_not_run_the_action_until_that_call_has_returned(StartAt liveStart) throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("history"));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
        AtomicInteger entered = new AtomicInteger();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch liveCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            int call = entered.incrementAndGet();
            calls.add("call " + call + " enters");
            if (call == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            } else {
                liveCallEntered.countDown();
            }
            calls.add("call " + call + " returns");
        };
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);
        Call subscribeLive = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            assertThat(stillRanWarnings(logged)).as("the warnings logged by the cancel, which gave up on the replay's call").hasSize(1);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();

            // When
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", liveStart, action));
            // A live event arrives, written again until the window ends, since one written before the live subscription exists is not delivered to it
            long windowEnds = System.nanoTime() + Duration.ofMillis(1500).toNanos();
            while (System.nanoTime() < windowEnds && !liveCallEntered.await(100, TimeUnit.MILLISECONDS)) {
                write(eventStore, nameDefined("live"));
            }

            // Then
            assertThat(liveCallEntered.getCount() == 0)
                    .as("a second call entered the action while the replay's call was still in it, calls: %s", calls)
                    .isFalse();

            // When the replay's call is released
            releaseReplayCall.countDown();

            // Then the live event reaches the action
            long deliveryWindowEnds = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deliveryWindowEnds && !liveCallEntered.await(100, TimeUnit.MILLISECONDS)) {
                write(eventStore, nameDefined("live"));
            }
            assertThat(liveCallEntered.getCount() == 0)
                    .as("the live event was delivered to the action once the replay's call was released, calls: %s", calls)
                    .isTrue();
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    /**
     * A cancel that returned has forgotten the subscription, so the live event waiting for the replay a cancel ended has
     * nothing to be handed to and ends its wait by throwing AttemptNotMade instead of waiting for that replay.
     */
    @Test
    @Timeout(30)
    void a_live_event_waiting_for_an_ended_replay_throws_without_reaching_the_action_once_a_cancel_of_its_subscription_has_returned() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call subscribeLive = null;
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, nameDefined("live"));
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));

            // Then
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isNull();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(liveCallWaitsForTheEndedReplays(liveModel)).as("the live event still waits for the ended replay").isFalse());

            // When the replay's call is released
            releaseReplayCall.countDown();

            // Then the live call threw AttemptNotMade and the live event never reached the action
            await().atMost(Duration.ofSeconds(5)).until(() -> liveModel.liveCallsEnded() > 0);
            assertAll(
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(received).as("the events the action was given").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_event_waiting_for_an_ended_replay_does_not_reach_the_action_when_its_subscription_was_cancelled_and_subscribed_again_within_the_poll_of_its_wait() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            try {
                received.add(cloudEvent.getId());
                if (calls.incrementAndGet() == 1) {
                    replayCallEntered.countDown();
                    awaitLatch(releaseReplayCall);
                }
            } finally {
                inTheAction.decrementAndGet();
            }
        };
        Call subscribeLive = null;
        Call cancelAndSubscribeAgain = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When
            cancelAndSubscribeAgain = Call.start(() -> {
                subscription.cancelSubscription("subscription");
                subscription.subscribe("subscription", StartAt.now(), action);
            });
            assertThat(cancelAndSubscribeAgain.returnsWithin(Duration.ofSeconds(5))).as("the cancel and the subscribe after it returned").isTrue();
            assertThat(cancelAndSubscribeAgain.failure()).as("what the cancel and the subscribe threw").isNull();
            releaseReplayCall.countDown();
            try {
                await().atMost(Duration.ofMillis(1500)).until(() -> received.contains(live.eventId()));
            } catch (org.awaitility.core.ConditionTimeoutException ignored) {
                // Asserted on below
            }

            // Then
            assertThat(received).as("the events the action was given, the live event belongs to a subscription whose cancel returned").doesNotContain(live.eventId());
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            if (cancelAndSubscribeAgain != null) {
                cancelAndSubscribeAgain.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_replay_started_after_a_cancel_of_a_replay_that_never_reached_the_action_does_not_call_the_action_while_an_earlier_replays_call_is_still_in_it() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("history"));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            try {
                if (calls.incrementAndGet() == 1) {
                    firstCallEntered.countDown();
                    awaitLatch(releaseFirstCall);
                }
            } finally {
                inTheAction.decrementAndGet();
            }
        };
        Call firstCancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first replay's call is in the action").isTrue();
            firstCancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(firstCancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            Subscription secondReplay = subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            subscription.cancelSubscription("subscription");
            assertThat(secondReplay.waitUntilStarted(Duration.ofSeconds(5))).as("the second replay, which never reached the action, has ended").isFalse();

            // When
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            try {
                await().atMost(Duration.ofMillis(1500)).until(() -> mostInTheAction.get() > 1);
            } catch (org.awaitility.core.ConditionTimeoutException ignored) {
                // Asserted on below
            }

            // Then
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            releaseFirstCall.countDown();
            if (firstCancel != null) {
                firstCancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_pause_and_a_resume_while_a_live_event_waits_for_an_ended_replay_do_not_let_a_second_live_event_call_the_action_beside_the_replay() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            try {
                received.add(cloudEvent.getId());
                if (calls.incrementAndGet() == 1) {
                    replayCallEntered.countDown();
                    awaitLatch(releaseReplayCall);
                }
            } finally {
                inTheAction.decrementAndGet();
            }
        };
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live1);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When
            subscription.pauseSubscription("subscription");
            subscription.resumeSubscription("subscription");
            write(eventStore, live2);
            assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId());
            releaseReplayCall.countDown();

            // Then
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live1.eventId(), live2.eventId()));
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_call_of_a_subscription_that_went_live_from_its_replay_is_waited_for_before_the_next_subscribe_of_the_id_delivers() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        CountDownLatch firstLiveCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstLiveCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            try {
                received.add(cloudEvent.getId());
                if (cloudEvent.getId().equals(live1.eventId())) {
                    firstLiveCallEntered.countDown();
                    awaitLatch(releaseFirstLiveCall);
                }
            } finally {
                inTheAction.decrementAndGet();
            }
        };
        Call cancel = null;

        try {
            assertThat(subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action).waitUntilStarted(Duration.ofSeconds(5))).as("the subscription went live from its replay").isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> !subscription.isCatchingUp("subscription"));
            write(eventStore, live1);
            assertThat(firstLiveCallEntered.await(5, TimeUnit.SECONDS)).as("the live call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the live call is still in the action").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isNull();

            // When
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live2);

            // Then the second subscription's call is not in the action beside the first one's
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given while the first live call is still in it").containsExactly(history.eventId(), live1.eventId()));

            // When the first live call returns
            releaseFirstLiveCall.countDown();

            // Then
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live1.eventId(), live2.eventId()));
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            releaseFirstLiveCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_call_that_did_not_wait_for_a_replay_throws_without_reaching_the_action_when_it_is_made_after_a_cancel_of_its_subscription() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CloudEvent late = CloudEventBuilder.v1().withId("late").withSource(URI.create("urn:test")).withType("late").build();

        try {
            subscription.subscribe("subscription", StartAt.now(), cloudEvent -> received.add(cloudEvent.getId()));
            subscription.cancelSubscription("subscription");

            // When
            Throwable thrown = catchThrowable(() -> liveModel.deliverToTheCancelled("subscription", late));

            // Then
            assertAll(
                    () -> assertThat(thrown).as("what the live call threw").isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(received).as("the events the action was given").isEmpty()
            );
        } finally {
            subscription.shutdown();
        }
    }

    /**
     * The documented limit of the gate on a live call after a cancel. A subclass that overrides the protected
     * startCatchupAsync and calls super on another thread starts the replay without the subscription's record, so the
     * live delegate's call that comes after a cancel reaches the action, as it did in 0.34.0.
     */
    @Test
    @Timeout(30)
    void known_limit_a_late_live_call_after_a_cancel_reaches_the_action_when_a_subclass_starts_the_replay_on_another_thread_as_in_0_34_0() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore, new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
                // Not waited for here, since the subscribing thread holds the handover lock that the call on the other thread takes
                CompletableFuture<Subscription> started = new CompletableFuture<>();
                Thread.ofVirtual().start(() -> {
                    try {
                        started.complete(super.startCatchupAsync(subscriptionId, catchup, holdPaused).get());
                    } catch (Exception e) {
                        started.completeExceptionally(e);
                    }
                });
                return started;
            }
        };
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CloudEvent late = CloudEventBuilder.v1().withId("late").withSource(URI.create("urn:test")).withType("late").build();

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> received.add(cloudEvent.getId()));
            await().atMost(Duration.ofSeconds(5)).until(() -> received.contains(history.eventId()) && liveModel.isRunning("subscription"));
            subscription.cancelSubscription("subscription");

            // When
            Throwable thrown = catchThrowable(() -> liveModel.deliverToTheCancelled("subscription", late));

            // Then
            assertAll(
                    () -> assertThat(thrown).as("what the late live call threw").isNull(),
                    () -> assertThat(received).as("the events the action was given").containsExactly(history.eventId(), "late")
            );
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_event_that_waits_for_an_ended_replay_reaches_the_action_after_that_replays_call_returned_when_it_returns_while_the_catchup_model_shuts_down() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        // Waits for the live calls on shutdown, and releases the replay's call at the moment its shutdown begins, which is
        // before the live call has waited a second since this model began to shut down
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints()
                .joiningLiveCallsOnShutdown().runningAtShutdown(releaseReplayCall::countDown);
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, new InMemoryCheckpointStorage());
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> trace = new CopyOnWriteArrayList<>();
        Consumer<CloudEvent> action = cloudEvent -> {
            trace.add("enter " + cloudEvent.getId());
            if (cloudEvent.getId().equals(history.eventId())) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
            trace.add("exit " + cloudEvent.getId());
        };
        Call cancel = null;
        Call shutdown = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When the catch-up model shuts down, which returns the replay's call before the live call has waited a second
            shutdown = Call.start(subscription::shutdown);

            // Then the live call reaches the action once the replay's call has returned
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(trace).as("what the action was given, in order").containsExactly("enter " + history.eventId(), "exit " + history.eventId(), "enter " + live.eventId(), "exit " + live.eventId()));
            boolean shutdownReturned = shutdown.returnsWithin(Duration.ofSeconds(5));
            assertAll(
                    () -> assertThat(shutdownReturned).as("the catch-up model's shutdown returned").isTrue(),
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").isEmpty()
            );
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            if (shutdown != null) {
                shutdown.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_event_that_waits_for_an_ended_replay_is_not_stored_past_when_the_catchup_model_shuts_down() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints();
        InMemoryCheckpointStorage liveStorage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, liveStorage);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call cancel = null;
        Call shutdown = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When
            shutdown = Call.start(subscription::shutdown);
            await().atMost(Duration.ofSeconds(10)).until(liveModel::shutdownBegan);

            // Then the live call throws a second after the shutdown began, while the replay's call is still in the action
            await().atMost(Duration.ofSeconds(5)).until(() -> liveModel.liveCallsEnded() > 0);
            boolean shutdownReturned = shutdown.returnsWithin(Duration.ofSeconds(5));
            assertAll(
                    () -> assertThat(shutdownReturned).as("the catch-up model's shutdown returned").isTrue(),
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(liveStorage.read("subscription")).as("the checkpoint stored for the live event that was never given to the action").isNull(),
                    () -> assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            if (shutdown != null) {
                shutdown.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_event_that_waits_for_an_ended_replay_does_not_ask_the_live_delegate_whether_the_subscription_is_running_once_the_catchup_model_began_to_shut_down() throws InterruptedException {
        // Given a live delegate whose isRunning blocks once its shutdown has begun, and whose shutdown waits for the live call
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CountDownLatch releaseIsRunning = new CountDownLatch(1);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints()
                .joiningLiveCallsOnShutdown().blockingIsRunningAtShutdownUntil(releaseIsRunning);
        InMemoryCheckpointStorage liveStorage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, liveStorage);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call cancel = null;
        Call shutdown = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When the catch-up model shuts down while the replay's call is still in the action
            long beforeShutdown = System.nanoTime();
            shutdown = Call.start(subscription::shutdown);
            boolean shutdownReturned = shutdown.returnsWithin(Duration.ofSeconds(5));
            releaseIsRunning.countDown();

            // Then the shutdown returned well before the live delegate's isRunning would have, and the live call threw a second after the shutdown began without reaching the action
            assertThat(shutdownReturned).as("the catch-up model's shutdown returned while the replay's call is still in the action").isTrue();
            assertAll(
                    () -> assertThat(Duration.ofNanos(liveModel.lastLiveCallEndedNanos() - beforeShutdown)).as("how long after the shutdown began the live call ended").isGreaterThanOrEqualTo(AbstractCatchupSubscriptionModel.ACTION_WAIT),
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(liveStorage.read("subscription")).as("the checkpoint stored for the live event that was never given to the action").isNull(),
                    () -> assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            if (shutdown != null) {
                shutdown.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_live_event_that_waits_for_an_ended_replay_throws_a_second_after_the_catchup_model_began_to_shut_down() throws InterruptedException {
        // Given a live delegate whose shutdown waits for the live call without interrupting it
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints().joiningLiveCallsOnShutdown();
        InMemoryCheckpointStorage liveStorage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, liveStorage);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call cancel = null;
        Call shutdown = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When the catch-up model shuts down while the replay's call is still in the action
            long beforeShutdown = System.nanoTime();
            shutdown = Call.start(subscription::shutdown);

            // Then the shutdown returns, although the replay's call is still in the action
            boolean shutdownReturned = shutdown.returnsWithin(Duration.ofSeconds(5));
            assertThat(shutdownReturned).as("the catch-up model's shutdown returned while the replay's call is still in the action").isTrue();

            // And the live call threw no sooner than a second after the shutdown began
            assertAll(
                    () -> assertThat(Duration.ofNanos(liveModel.lastLiveCallEndedNanos() - beforeShutdown)).as("how long after the shutdown began the live call ended").isGreaterThanOrEqualTo(AbstractCatchupSubscriptionModel.ACTION_WAIT),
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(liveStorage.read("subscription")).as("the checkpoint stored for the live event that was never given to the action").isNull(),
                    () -> assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            if (shutdown != null) {
                shutdown.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void an_interrupt_of_a_live_event_that_waits_for_an_ended_replay_does_not_end_its_wait_while_the_catchup_model_shuts_down() throws InterruptedException {
        // Given a live delegate that interrupts the waiting live call as its shutdown begins
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints().joiningLiveCallsOnShutdown();
        liveModel.runningAtShutdown(() -> Objects.requireNonNull(liveModel.deliveryThread()).interrupt());
        InMemoryCheckpointStorage liveStorage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, liveStorage);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call cancel = null;
        Call shutdown = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When the catch-up model shuts down, and the waiting live call is interrupted as it does
            long beforeShutdown = System.nanoTime();
            shutdown = Call.start(subscription::shutdown);

            // Then the shutdown returns, and the interrupt didn't end the wait, which only the second after the shutdown began does
            boolean shutdownReturned = shutdown.returnsWithin(Duration.ofSeconds(5));
            assertThat(shutdownReturned).as("the catch-up model's shutdown returned while the replay's call is still in the action").isTrue();
            assertAll(
                    () -> assertThat(Duration.ofNanos(liveModel.lastLiveCallEndedNanos() - beforeShutdown)).as("how long after the shutdown began the interrupted live call ended").isGreaterThanOrEqualTo(AbstractCatchupSubscriptionModel.ACTION_WAIT),
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(liveStorage.read("subscription")).as("the checkpoint stored for the live event that was never given to the action").isNull(),
                    () -> assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (cancel != null) {
                cancel.join();
            }
            if (shutdown != null) {
                shutdown.join();
            }
            subscription.shutdown();
        }
    }

    @ParameterizedTest
    @MethodSource("whatEndsTheSubscriptionAWaitingLiveEventBelongsTo")
    @Timeout(30)
    void a_live_event_that_waits_for_an_ended_replay_throws_and_is_not_stored_past_once_the_subscription_is_cancelled(boolean subscribesTheIdAgain) throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel).stampingCheckpoints();
        InMemoryCheckpointStorage liveStorage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durableLiveModel = new DurableSubscriptionModel(liveModel, liveStorage);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(durableLiveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call firstCancel = null;
        Call secondCancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            firstCancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(firstCancel.returnsWithin(Duration.ofSeconds(5))).as("the first cancel returned while the replay's call is still in the action").isTrue();
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));

            // When
            secondCancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(secondCancel.returnsWithin(Duration.ofSeconds(5))).as("the second cancel returned while the replay's call is still in the action").isTrue();
            if (subscribesTheIdAgain) {
                subscription.subscribe("subscription", StartAt.now(), action);
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> liveModel.liveCallsEnded() > 0);

            // Then
            assertAll(
                    () -> assertThat(liveModel.thrownByTheCallback()).as("what the live call threw").singleElement().isInstanceOf(AttemptNotMade.class),
                    () -> assertThat(received).as("the events the action was given while the replay's call is still in it").containsExactly(history.eventId())
            );
        } finally {
            releaseReplayCall.countDown();
            if (firstCancel != null) {
                firstCancel.join();
            }
            if (secondCancel != null) {
                secondCancel.join();
            }
            subscription.shutdown();
        }
    }

    private static Stream<Arguments> whatEndsTheSubscriptionAWaitingLiveEventBelongsTo() {
        return Stream.of(
                Arguments.of(Named.of("a cancel", false)),
                Arguments.of(Named.of("a cancel and a later subscribe of the same id", true)));
    }

    @Test
    @Timeout(30)
    void a_subscribe_after_a_cancel_does_not_replay_into_the_action_while_a_live_call_of_the_cancelled_subscription_is_still_in_it() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined live = nameDefined("live");
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch liveCallEntered = new CountDownLatch(1);
        CountDownLatch releaseLiveCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            try {
                received.add(cloudEvent.getId());
                if (calls.incrementAndGet() == 1) {
                    liveCallEntered.countDown();
                    awaitLatch(releaseLiveCall);
                }
            } finally {
                inTheAction.decrementAndGet();
            }
        };

        try {
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            assertThat(liveCallEntered.await(5, TimeUnit.SECONDS)).as("the live call is in the action").isTrue();
            subscription.cancelSubscription("subscription");

            // When
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            try {
                await().atMost(Duration.ofMillis(1500)).until(() -> received.size() > 1);
            } catch (org.awaitility.core.ConditionTimeoutException ignored) {
                // Asserted on below
            }

            // Then
            assertThat(received).as("the events the action was given while the live call is still in it").containsExactly(live.eventId());
            releaseLiveCall.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(live.eventId(), live.eventId()));
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            releaseLiveCall.countDown();
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void an_action_that_cancels_and_subscribes_its_own_id_on_a_live_call_returns_and_the_new_subscription_replays_after_it() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined live = nameDefined("live");
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger inTheAction = new AtomicInteger();
        AtomicInteger mostInTheAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Consumer<CloudEvent>> action = new AtomicReference<>();
        CountDownLatch subscribedInTheAction = new CountDownLatch(1);
        CountDownLatch firstCallReturned = new CountDownLatch(1);
        action.set(cloudEvent -> {
            mostInTheAction.accumulateAndGet(inTheAction.incrementAndGet(), Math::max);
            boolean first = calls.incrementAndGet() == 1;
            try {
                received.add(cloudEvent.getId());
                if (first) {
                    subscription.cancelSubscription("subscription");
                    subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action.get());
                    subscribedInTheAction.countDown();
                }
            } finally {
                inTheAction.decrementAndGet();
                if (first) {
                    firstCallReturned.countDown();
                }
            }
        });

        try {
            subscription.subscribe("subscription", StartAt.now(), action.get());

            // When
            write(eventStore, live);

            // Then
            assertThat(subscribedInTheAction.await(5, TimeUnit.SECONDS)).as("the subscribe inside the action returned").isTrue();
            assertThat(firstCallReturned.await(5, TimeUnit.SECONDS)).as("the live call returned").isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(live.eventId(), live.eventId()));
            assertThat(mostInTheAction.get()).as("the most calls that were in the action at once").isEqualTo(1);
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_subscribe_after_a_cancel_replays_into_the_action_once_a_live_call_of_the_cancelled_subscription_has_thrown() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined live = nameDefined("live");
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch liveCallEntered = new CountDownLatch(1);
        CountDownLatch releaseLiveCall = new CountDownLatch(1);
        CountDownLatch replayed = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            if (calls.incrementAndGet() == 1) {
                liveCallEntered.countDown();
                awaitLatch(releaseLiveCall);
                throw new IllegalStateException("the live call fails");
            }
            received.add(cloudEvent.getId());
            replayed.countDown();
        };

        try {
            subscription.subscribe("subscription", StartAt.now(), action);
            write(eventStore, live);
            assertThat(liveCallEntered.await(5, TimeUnit.SECONDS)).as("the live call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            releaseLiveCall.countDown();

            // When
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);

            // Then
            assertThat(replayed.await(5, TimeUnit.SECONDS)).as("the replay reached the action after the live call threw").isTrue();
            assertThat(received).as("the events the action was given").containsExactly(live.eventId());
        } finally {
            releaseLiveCall.countDown();
            subscription.shutdown();
        }
    }

    /**
     * A cancel that throws before the live delegate has cancelled leaves the live delegate running the subscription, so
     * the live event waiting for the ended replay has to reach the action once that replay returns. The live delegate
     * stores its position past an event it is told is done, and won't deliver that event again.
     */
    @Test
    @Timeout(30)
    void a_live_event_waiting_for_an_ended_replay_is_kept_and_reaches_the_action_in_order_when_the_cancel_failed_without_cancelling_the_live_delegate() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call subscribeLive = null;
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, live1);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));
            liveModel.cancelThrows(new IllegalStateException("the live delegate could not cancel"));

            // When
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));

            // Then
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isInstanceOf(IllegalStateException.class).hasMessage("the live delegate could not cancel");
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(liveModel.acknowledged("subscription")).as("the events the live delegate was told are done").isEmpty());
            assertThat(liveModel.isRunning("subscription")).as("the live delegate still runs the subscription").isTrue();

            // When the replay's call is released and a later live event is written
            releaseReplayCall.countDown();
            write(eventStore, live2);

            // Then both live events reach the action once each, in order
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live1.eventId(), live2.eventId()));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(liveModel.acknowledged("subscription")).as("the events the live delegate was told are done").containsExactly(live1.eventId(), live2.eventId()));
            assertThat(liveModel.resumeCalls()).as("the resumes of the live delegate").isZero();
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    /**
     * An interrupt of the live delegate's delivery thread while its event waits for the ended replay is waited out, and
     * the action must not start on an interrupted thread, since an action that reads the status could skip the event as
     * cancelled and the live delegate would then store its position past it. The status is set again once the action has
     * returned.
     */
    @Test
    @Timeout(30)
    void a_live_event_waiting_for_an_ended_replay_reaches_the_action_without_the_interrupt_the_wait_swallowed_and_the_interrupt_is_set_again_after_the_action() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live = nameDefined("live");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<@Nullable Boolean> interruptedAtActionEntry = new AtomicReference<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (cloudEvent.getId().equals(live.eventId())) {
                interruptedAtActionEntry.set(Thread.currentThread().isInterrupted());
            } else {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call subscribeLive = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, live);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Thread delivery = liveModel.deliveryThread();
                assertThat(delivery).as("the thread the live event was delivered on").isNotNull();
                assertThat(delivery.getState()).as("the state of the delivery thread while the live event waits for the ended replay").isEqualTo(Thread.State.TIMED_WAITING);
            });

            // When the delivery thread is interrupted while it waits, and then the replay's call is released
            Thread delivery = liveModel.deliveryThread();
            delivery.interrupt();
            releaseReplayCall.countDown();

            // Then
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(liveModel.interruptedAfterTheAction()).as("the interrupt status was recorded after the action returned").isNotNull());
            assertThat(interruptedAtActionEntry.get()).as("the interrupt status of the delivery thread when the action started").isFalse();
            assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live.eventId());
            assertThat(liveModel.interruptedAfterTheAction()).as("the interrupt status of the delivery thread after the action returned").isTrue();
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            subscription.shutdown();
        }
    }

    /**
     * An interrupt that is already set on the live delegate's delivery thread when the ended replay has returned is not
     * waited out by anything, and the action must still not start on an interrupted thread, since an action that reads
     * the status could skip the event as cancelled and the live delegate would then store its position past it. The
     * status is set again once the action has returned.
     */
    @Test
    @Timeout(30)
    void a_live_event_whose_thread_is_already_interrupted_after_the_ended_replay_returned_reaches_the_action_without_the_interrupt_and_the_interrupt_is_set_again_after_the_action() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live1 = nameDefined("live-1");
        NameDefined live2 = nameDefined("live-2");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        AtomicReference<@Nullable Boolean> interruptedAtActionEntry = new AtomicReference<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (cloudEvent.getId().equals(live2.eventId())) {
                interruptedAtActionEntry.set(Thread.currentThread().isInterrupted());
            } else if (cloudEvent.getId().equals(history.eventId())) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call subscribeLive = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, live1);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                Thread delivery = liveModel.deliveryThread();
                assertThat(delivery).as("the thread the live event was delivered on").isNotNull();
                assertThat(delivery.getState()).as("the state of the delivery thread while the live event waits for the ended replay").isEqualTo(Thread.State.TIMED_WAITING);
            });
            releaseReplayCall.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given once the ended replay returned").containsExactly(history.eventId(), live1.eventId()));

            // When the next live event is delivered on a thread whose interrupt status is already set
            liveModel.interruptTheNextDelivery();
            write(eventStore, live2);

            // Then
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(liveModel.interruptedAfterTheAction()).as("the interrupt status was recorded after the action returned for the second live event").isNotNull());
            assertThat(interruptedAtActionEntry.get()).as("the interrupt status of the delivery thread when the action started").isFalse();
            assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live1.eventId(), live2.eventId());
            assertThat(liveModel.interruptedAfterTheAction()).as("the interrupt status of the delivery thread after the action returned").isTrue();
        } finally {
            releaseReplayCall.countDown();
            if (subscribeLive != null) {
                subscribeLive.join();
            }
            subscription.shutdown();
        }
    }

    /**
     * A cancel that cancels the live delegate and then throws, as a durable live delegate does when deleting its stored
     * position fails, keeps the subscription, so the live event waiting for the ended replay reaches the action and a
     * resume subscribes again.
     */
    @Test
    @Timeout(30)
    void a_subscription_whose_cancel_cancelled_the_live_delegate_and_then_failed_is_resumed_and_gives_the_action_its_live_events_in_order() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        NameDefined history = nameDefined("history");
        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        write(eventStore, history);
        CheckpointAwareInMemorySubscriptionModel liveModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(liveModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        Consumer<CloudEvent> action = cloudEvent -> {
            received.add(cloudEvent.getId());
            if (received.size() == 1) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };
        Call subscribeLive = null;
        Call cancel = null;
        Call resume = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(5, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            subscription.cancelSubscription("subscription");
            subscribeLive = Call.start(() -> subscription.subscribe("subscription", StartAt.now(), action));
            assertThat(subscribeLive.returnsWithin(Duration.ofSeconds(5))).as("the live subscribe returned").isTrue();
            write(eventStore, live1);
            await().atMost(Duration.ofSeconds(5)).until(() -> liveCallWaitsForTheEndedReplays(liveModel));
            liveModel.cancelsAndThenThrows(new IllegalStateException("the live delegate could not delete its position"));
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned while the replay's call is still in the action").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isInstanceOf(IllegalStateException.class).hasMessage("the live delegate could not delete its position");
            releaseReplayCall.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsExactly(history.eventId(), live1.eventId()));

            // When
            resume = Call.start(() -> subscription.resumeSubscription("subscription"));
            assertThat(resume.returnsWithin(Duration.ofSeconds(10))).as("the resume returned").isTrue();
            write(eventStore, live2);

            // Then
            assertThat(resume.failure()).as("what the resume threw").isNull();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received).as("the events the action was given").containsSubsequence(live1.eventId(), live2.eventId()));
        } finally {
            releaseReplayCall.countDown();
            for (Call call : new Call[]{subscribeLive, cancel, resume}) {
                if (call != null) {
                    call.join();
                }
            }
            subscription.shutdown();
        }
    }

    // Whether the thread liveModel delivered on is parked in the wait for the replays a cancel or a stop ended
    private static boolean liveCallWaitsForTheEndedReplays(CheckpointAwareInMemorySubscriptionModel liveModel) {
        Thread delivery = liveModel.deliveryThread();
        return delivery != null && delivery.getState() != Thread.State.RUNNABLE && Arrays.stream(delivery.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("acceptAfterEndedReplays"));
    }

    private static Stream<Arguments> startsThatGoStraightToLive() {
        return Stream.of(
                Arguments.of(Named.of("the default start, which finds no stored position", StartAt.subscriptionModelDefault())),
                Arguments.of(Named.of("now", StartAt.now())),
                Arguments.of(Named.of("a dynamic start the model declines", StartAt.dynamic(context -> null))));
    }

    @Test
    @Timeout(30)
    void a_cancel_called_from_inside_the_action_of_a_replay_does_not_wait_for_that_action() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        AtomicLong cancelTookMillis = new AtomicLong(-1);
        CountDownLatch cancelReturned = new CountDownLatch(1);
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        catchupLog.addAppender(logged);

        try {
            // When
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
                long before = System.nanoTime();
                subscription.cancelSubscription("subscription");
                cancelTookMillis.set(Duration.ofNanos(System.nanoTime() - before).toMillis());
                cancelReturned.countDown();
            });

            // Then
            assertThat(cancelReturned.await(5, TimeUnit.SECONDS)).as("the cancel made from the action returned").isTrue();
            assertThat(cancelTookMillis.get()).as("how many milliseconds the cancel from inside the action took").isLessThan(500);
            assertThat(stillRanWarnings(logged)).as("the warnings logged").isEmpty();
        } finally {
            catchupLog.detachAppender(logged);
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_pause_returns_while_the_replay_stores_a_position_instead_of_waiting_for_the_store() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call pause = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();

            // When
            pause = Call.start(() -> subscription.pauseSubscription("subscription"));

            // Then
            assertThat(pause.returnsWithin(Duration.ofSeconds(2))).as("the pause returned while the replay's store was blocked").isTrue();
            assertThat(pause.failure()).as("what the pause threw").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            if (pause != null) {
                pause.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_stop_returns_while_the_replay_stores_a_position_instead_of_waiting_for_the_store() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call stop = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();

            // When
            stop = Call.start(subscription::stop);

            // Then
            assertThat(stop.returnsWithin(Duration.ofSeconds(2))).as("the stop returned while the replay's store was blocked").isTrue();
            assertThat(stop.failure()).as("what the stop threw").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            if (stop != null) {
                stop.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void the_position_a_replay_was_storing_when_a_stop_came_is_stored_once_the_store_is_released() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call stop = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
            stop = Call.start(subscription::stop);
            stop.returnsWithin(Duration.ofSeconds(1));

            // When
            storage.releaseTheSave.countDown();

            // Then
            assertThat(stop.returnsWithin(Duration.ofSeconds(5))).as("the stop returned once the store was released").isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(storage.read("subscription")).as("the position stored for the stopped subscription").isNotNull());
        } finally {
            storage.releaseTheSave.countDown();
            if (stop != null) {
                stop.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(60)
    void a_resume_at_a_given_position_after_a_stop_stores_its_position_only_after_the_store_the_stopped_replay_began_has_returned() throws InterruptedException {
        // Given a replay a subscribe started, storing a position that a stop did not wait for
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        OrderedWritesStorage storage = new OrderedWritesStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Checkpoint given = GlobalCheckpoint.of(7, new StringBasedCheckpoint("live-start"), 0, 7);
        Call stop = null;
        Call resume = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
            storage.seed("subscription", GlobalCheckpoint.of(0));
            stop = Call.start(subscription::stop);
            assertThat(stop.returnsWithin(Duration.ofSeconds(5))).as("the stop returned while the replay's store was blocked").isTrue();

            // When
            resume = Call.start(() -> subscription.resumeSubscription("subscription", StartAt.checkpoint(given)));
            Call resumeCall = resume;
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(resumeCall.returnsWithin(Duration.ZERO) || resumeCall.waitsInTheCheckAfterWhichItStores())
                    .as("the resume has returned or waits for the replay").isTrue());
            storage.releaseTheSave.countDown();

            // Then
            assertThat(resume.returnsWithin(Duration.ofSeconds(15))).as("the resume returned once the store was released").isTrue();
            assertThat(storage.writes).as("the writes in the order the storage returned them, with what the resume threw: " + resume.failure()).containsSubsequence("replay", given.asString());
        } finally {
            storage.releaseTheSave.countDown();
            if (stop != null) {
                stop.join();
            }
            if (resume != null) {
                resume.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_that_comes_before_the_replay_checks_whether_to_store_a_position_leaves_no_stored_position() throws InterruptedException {
        // Given the replay held in its action, which it leaves for the check that comes before the store
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        RecordedAction action = new RecordedAction(() -> awaitLatch(storage.releaseTheSave));

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), action);
            assertThat(action.firstCallEntered.await(5, TimeUnit.SECONDS)).as("the first call is in the action").isTrue();

            // When
            subscription.cancelSubscription("subscription");
            storage.releaseTheSave.countDown();

            // Then
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(action.calls).as("the calls to the action").containsExactly("call 1 enters", "call 1 returns"));
            sleepFor(300);
            assertThat(storage.saveEntered.getCount()).as("the number of saves the replay began").isEqualTo(1);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_that_comes_while_the_replay_stores_a_position_returns_only_after_the_store_has_returned_and_leaves_no_stored_position() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();

            // When
            cancel = Call.start(() -> {
                subscription.cancelSubscription("subscription");
                storage.events.add("cancel returns");
            });

            // Then
            assertThat(cancel.returnsWithin(Duration.ofMillis(500))).as("the cancel returned while the replay's store was blocked").isFalse();
            storage.releaseTheSave.countDown();
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned once the store was released").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isNull();
            assertThat(storage.events).as("the order the storage was asked to do things and the cancel returned in").containsSubsequence("save returns", "deleted", "cancel returns");
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();
            sleepFor(300);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription later").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_that_comes_after_a_stop_parked_the_replay_while_it_stores_a_position_still_waits_for_the_store_and_leaves_no_stored_position() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call stop = null;
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
            stop = Call.start(subscription::stop);
            stop.returnsWithin(Duration.ofSeconds(1));

            // When
            cancel = Call.start(() -> {
                subscription.cancelSubscription("subscription");
                storage.events.add("cancel returns");
            });

            // Then
            assertThat(cancel.returnsWithin(Duration.ofMillis(500))).as("the cancel returned while the replay's store was blocked").isFalse();
            storage.releaseTheSave.countDown();
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned once the store was released").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isNull();
            assertThat(storage.events).as("the order the storage was asked to do things and the cancel returned in").containsSubsequence("save returns", "deleted", "cancel returns");
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();

            subscription.start(true);
            sleepFor(300);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription once the model started again").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            if (stop != null) {
                stop.join();
            }
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_that_comes_while_the_replays_store_then_fails_goes_on_and_deletes_and_leaves_no_stored_position() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        storage.saveFailure = new IllegalStateException("the store failed");
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            cancel.returnsWithin(Duration.ofMillis(300));

            // When
            storage.releaseTheSave.countDown();

            // Then
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned once the store failed").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isNull();
            assertThat(storage.events).as("what the storage was asked to do").contains("deleted").doesNotContain("save returns");
            sleepFor(300);
            assertThat(storage.read("subscription")).as("the position stored for the cancelled subscription").isNull();
        } finally {
            storage.releaseTheSave.countDown();
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void a_cancel_whose_delete_of_the_stored_position_throws_throws_that_to_its_caller() throws InterruptedException {
        // Given
        InMemoryEventStore eventStore = twoEventsInTimeOrder();
        BlockingSaveStorage storage = new BlockingSaveStorage();
        IllegalStateException deleteFailure = new IllegalStateException("the delete failed");
        storage.deleteFailure = deleteFailure;
        StreamCatchupSubscriptionModel subscription = storingDuringCatchup(eventStore, storage);
        Call cancel = null;

        try {
            subscription.subscribe("subscription", StartAtTime.beginningOfTime(), cloudEvent -> {
            });
            assertThat(storage.saveEntered.await(5, TimeUnit.SECONDS)).as("the replay is storing a position").isTrue();
            cancel = Call.start(() -> subscription.cancelSubscription("subscription"));
            cancel.returnsWithin(Duration.ofMillis(300));

            // When
            storage.releaseTheSave.countDown();

            // Then
            assertThat(cancel.returnsWithin(Duration.ofSeconds(5))).as("the cancel returned once the store was released").isTrue();
            assertThat(cancel.failure()).as("what the cancel threw").isSameAs(deleteFailure);
        } finally {
            storage.releaseTheSave.countDown();
            if (cancel != null) {
                cancel.join();
            }
            subscription.shutdown();
        }
    }

    private InMemoryEventStore twoEventsInTimeOrder() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        return eventStore;
    }

    private StreamCatchupSubscriptionModel storingDuringCatchup(InMemoryEventStore eventStore, CheckpointStorage storage) {
        return new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
    }

    /**
     * Issue #1257. A handover lock was kept for every id a catch-up ever ran for.
     */
    @Test
    @Timeout(120)
    void subscribing_and_cancelling_thousands_of_ids_leaves_no_handover_lock_behind() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        write(eventStore, nameDefined("event1"));
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        try {
            for (int i = 0; i < 3000; i++) {
                String id = "subscription-" + i;
                subscription.subscribe(id, StartAtTime.beginningOfTime(), cloudEvent -> {
                });
                subscription.cancelSubscription(id);
            }

            Map<String, ?> handoverLocks = handoverLocksOf(subscription);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(handoverLocks.size()).as("the handover locks left after 3000 ids were subscribed and cancelled").isZero());
        } finally {
            subscription.shutdown();
        }
    }

    @Test
    @Timeout(120)
    void threads_that_lock_an_id_are_never_inside_it_together_while_the_id_is_cancelled_again_and_again_and_no_lock_is_left_behind() throws Exception {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel).withoutStreamPosition();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger mostInsideAtOnce = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        AtomicBoolean lockersDone = new AtomicBoolean();
        List<Thread> lockers = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            Thread locker = new Thread(() -> {
                awaitLatch(go);
                for (int i = 0; i < 500; i++) {
                    try (AbstractCatchupSubscriptionModel.HandoverLock ignored = subscription.lockHandover("subscription")) {
                        mostInsideAtOnce.accumulateAndGet(inside.incrementAndGet(), Math::max);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                        inside.decrementAndGet();
                    }
                }
            });
            lockers.add(locker);
            locker.start();
        }
        Thread canceller = new Thread(() -> {
            awaitLatch(go);
            while (!lockersDone.get()) {
                subscription.cancelSubscription("subscription");
            }
        });
        canceller.start();

        try {
            go.countDown();
            for (Thread locker : lockers) {
                locker.join();
            }
            lockersDone.set(true);
            canceller.join();

            assertThat(mostInsideAtOnce.get()).as("the most threads that held the lock of the id at once").isEqualTo(1);
            assertThat(handoverLocksOf(subscription).size()).as("the handover locks left once every thread is done").isZero();
        } finally {
            lockersDone.set(true);
            subscription.shutdown();
        }
    }

    // A call made on a thread of its own, so a test can ask whether it has returned while something it waits for is held
    private static final class Call {
        private final CountDownLatch returned = new CountDownLatch(1);
        private final AtomicReference<@Nullable Throwable> failure = new AtomicReference<>();
        private final Thread thread;

        private Call(Runnable call) {
            thread = new Thread(() -> {
                try {
                    call.run();
                } catch (Throwable e) {
                    failure.set(e);
                } finally {
                    returned.countDown();
                }
            });
        }

        static Call start(Runnable call) {
            Call started = new Call(call);
            started.thread.start();
            return started;
        }

        boolean returnsWithin(Duration timeout) {
            try {
                return returned.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Nullable Throwable failure() {
            return failure.get();
        }

        // Whether the call is parked in the wait for the replays in the way of a resume
        boolean waitsInTheCheckAfterWhichItStores() {
            return thread.getState() != Thread.State.RUNNABLE && Arrays.stream(thread.getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals("completesWithin") || frame.getMethodName().equals("awaitTheActions"));
        }

        void join() throws InterruptedException {
            thread.join(5000);
        }
    }

    // A checkpoint storage whose first save waits until the test lets it through, and then fails when asked to or stores
    // the checkpoint. Records, in order, what returned from a save and a delete that went through.
    private static final class BlockingSaveStorage extends InMemoryCheckpointStorage {
        final CountDownLatch saveEntered = new CountDownLatch(1);
        final CountDownLatch releaseTheSave = new CountDownLatch(1);
        final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();
        volatile @Nullable RuntimeException saveFailure;
        volatile @Nullable RuntimeException deleteFailure;
        private final AtomicBoolean blocked = new AtomicBoolean();

        @Override
        public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (blocked.compareAndSet(false, true)) {
                saveEntered.countDown();
                awaitLatch(releaseTheSave);
                RuntimeException failure = saveFailure;
                if (failure != null) {
                    throw failure;
                }
            }
            Checkpoint saved = super.save(id, checkpoint, condition);
            events.add("save returns");
            return saved;
        }

        @Override
        public void delete(String subscriptionId) {
            RuntimeException failure = deleteFailure;
            if (failure != null) {
                throw failure;
            }
            super.delete(subscriptionId);
            events.add("deleted");
        }
    }

    // A checkpoint storage whose first save, the replay's, waits until the test lets it through. Records what returned from
    // a save in order, as the replay's or as the checkpoint another call saved.
    private static final class OrderedWritesStorage extends InMemoryCheckpointStorage {
        final CountDownLatch saveEntered = new CountDownLatch(1);
        final CountDownLatch releaseTheSave = new CountDownLatch(1);
        final CopyOnWriteArrayList<String> writes = new CopyOnWriteArrayList<>();
        private final AtomicBoolean blocked = new AtomicBoolean();

        @Override
        public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            boolean replay = blocked.compareAndSet(false, true);
            if (replay) {
                saveEntered.countDown();
                awaitLatch(releaseTheSave);
            }
            Checkpoint saved = super.save(id, checkpoint, condition);
            writes.add(replay ? "replay" : checkpoint.asString());
            return saved;
        }

        void seed(String id, Checkpoint checkpoint) {
            super.save(id, checkpoint, CheckpointWriteCondition.any());
        }
    }

    private static Map<String, ?> handoverLocksOf(AbstractCatchupSubscriptionModel model) throws Exception {
        Field field = AbstractCatchupSubscriptionModel.class.getDeclaredField("handoverLocks");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ?> locks = (Map<String, ?>) field.get(model);
        return locks;
    }

    private static @Nullable CompletableFuture<?> earlierAttemptsDoneOfCurrentAttempt(AbstractCatchupSubscriptionModel model, String subscriptionId) throws ReflectiveOperationException {
        Field currentAttemptField = AbstractCatchupSubscriptionModel.class.getDeclaredField("currentAttempt");
        currentAttemptField.setAccessible(true);
        Object attempt = ((Map<?, ?>) currentAttemptField.get(model)).get(subscriptionId);
        if (attempt == null) {
            return null;
        }
        Field earlierAttemptsDoneField = attempt.getClass().getDeclaredField("earlierAttemptsDone");
        earlierAttemptsDoneField.setAccessible(true);
        return (CompletableFuture<?>) earlierAttemptsDoneField.get(attempt);
    }

    private static List<String> stillRanWarnings(ListAppender<ILoggingEvent> logged) {
        return new ArrayList<>(logged.list).stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("still ran for an event its catch-up replay delivered"))
                .toList();
    }

    private static void sleepFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void awaitUpTo(CountDownLatch latch, int seconds) {
        try {
            latch.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // Records when each call to it enters and returns, and runs inTheFirstCall in the first one
    private static final class RecordedAction implements Consumer<CloudEvent> {
        final CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
        final CountDownLatch firstCallEntered = new CountDownLatch(1);
        private final AtomicInteger entered = new AtomicInteger();
        private final Runnable inTheFirstCall;

        RecordedAction(Runnable inTheFirstCall) {
            this.inTheFirstCall = inTheFirstCall;
        }

        @Override
        public void accept(CloudEvent cloudEvent) {
            int call = entered.incrementAndGet();
            calls.add("call " + call + " enters");
            if (call == 1) {
                firstCallEntered.countDown();
                inTheFirstCall.run();
            }
            calls.add("call " + call + " returns");
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

    // One log for deliveries and saves, so the order between them shows
    private List<String> replayThreeEventsLoggingEachDeliveryAndSave(InMemoryEventStore eventStore, StartAt startAt) {
        write(eventStore, nameDefined("event1"));
        write(eventStore, nameDefined("event2"));
        write(eventStore, nameDefined("event3"));
        CopyOnWriteArrayList<String> log = new CopyOnWriteArrayList<>();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint save(String id, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                log.add("saved:" + checkpoint.asString());
                return super.save(id, checkpoint, condition);
            }
        };
        CopyOnWriteArrayList<CloudEvent> received = new CopyOnWriteArrayList<>();
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", startAt, cloudEvent -> {
            log.add("delivered:" + EventMetadata.from(cloudEvent).getCheckpoint().orElse("none"));
            received.add(cloudEvent);
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(entriesRightAfterEachDelivery(log)).hasSize(3));
        assertThat(received).as("the replayed events").hasSize(3).allSatisfy(cloudEvent -> assertThat(cloudEvent).isInstanceOf(CheckpointAwareCloudEvent.class));
        return log;
    }

    private static List<String> entriesRightAfterEachDelivery(List<String> log) {
        return IntStream.range(0, log.size() - 1).filter(i -> log.get(i).startsWith("delivered:")).mapToObj(i -> log.get(i + 1)).toList();
    }

    private static List<String> deliveriesAsSaves(List<String> log) {
        return deliveredCheckpoints(log).stream().map(checkpoint -> "saved:" + checkpoint).toList();
    }

    private static List<String> deliveredCheckpoints(List<String> log) {
        return log.stream().filter(entry -> entry.startsWith("delivered:")).map(entry -> entry.substring("delivered:".length())).toList();
    }

    private static final class PositionOnlyInMemoryEventStore extends InMemoryEventStore {
        private PositionRange lastPositionRange;

        private PositionOnlyInMemoryEventStore(Consumer<List<CloudEvent>> listener) {
            super(listener);
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            lastPositionRange = range;
            return super.readInPositionOrder(filter, range);
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            throw new AssertionError("Position-enabled beginning-of-time catch-up must not use the time-based query path");
        }

        @Override
        public long count(Filter filter) {
            throw new AssertionError("Position-enabled beginning-of-time catch-up must not use the time-based count path");
        }
    }

    // Reads by time through an in-memory store while exposing no PositionOrderedReader, as a store that has no notion of a position does
    private static final class QueriesOnlyEventStore implements EventStoreQueries, CatchupReads {
        private final InMemoryEventStore delegate;

        private QueriesOnlyEventStore(InMemoryEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
            return TimeOrderedCatchupReads.over(this).openCatchupReadScope(notBefore);
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return delegate.query(filter, skip, limit, sortBy);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }

    /**
     * Records what a catch-up tells a start refusal listener, and fails a refusal, which these tests never expect.
     */
    private static final class HeldPausedReports implements StartRefusalReportingSubscriptions.StartRefusalListener {
        private final List<CatchupCannotResumeException> heldPaused = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            throw new AssertionError("A catch-up subscription model was not expected to refuse the start of " + subscriptionId, refusal);
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            heldPaused.add(refusal);
        }

        @Override
        public void handleHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
        }
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link CheckpointAwareSubscriptionModel} for
     * these tests, mirroring the catchup-subscription module's own test double: any position start is translated
     * to {@code now}, since the in-memory model only supports {@code now}/{@code default}.
     */
    private static final class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final InMemorySubscriptionModel delegate;
        private final @Nullable Checkpoint checkpoint;
        // The live starts canResumeFrom answers false for, every one when losesEveryLiveStart is set, and what it and
        // subscribe were asked
        private final Set<Checkpoint> lost = ConcurrentHashMap.newKeySet();
        private volatile boolean losesEveryLiveStart = false;
        // canResumeFrom answers true to this many probes and false to every one after them
        private volatile int answersTrueToTheFirstProbes = Integer.MAX_VALUE;
        private final List<Checkpoint> resumeProbes = new CopyOnWriteArrayList<>();
        private final List<String> liveStarts = new CopyOnWriteArrayList<>();
        private final AtomicInteger globalCheckpointReads = new AtomicInteger();
        // The ids of the events whose callback returned normally, per subscription id, which is what a durable live
        // delegate stores as its position
        private final ConcurrentHashMap<String, List<String>> acknowledged = new ConcurrentHashMap<>();
        // What the callback it was given threw, and how many of its calls have ended, returned or thrown
        private final List<Throwable> thrownByTheCallback = new CopyOnWriteArrayList<>();
        private final AtomicInteger liveCallsEnded = new AtomicInteger();
        // System.nanoTime() as the last live call ended, 0 before one has
        private volatile long lastLiveCallEndedNanos;
        // Gives each event a checkpoint, as a live model that reads a stream does, so a durable model over this one can store it
        private volatile boolean stampsCheckpoints = false;
        private volatile boolean shutdownBegan = false;
        // Set for a live delegate whose shutdown waits for the live calls it has in flight without interrupting them.
        // Every other shutdown clears the subscriptions first, as the in-memory model does, and interrupts the threads
        // after waiting 5 seconds for them
        private volatile boolean joinsLiveCallsOnShutdown = false;
        // Set for a live delegate whose isRunning(id) blocks from the moment its shutdown begins, until the latch is
        // released or 10 seconds have passed
        private volatile @Nullable CountDownLatch isRunningBlockedAtShutdown;
        // Run on the thread that shuts this model down, as soon as the shutdown begins, so a test acts at that moment
        // instead of after a poll that can come late on a loaded machine
        private volatile @Nullable Runnable atShutdown;
        private final AtomicInteger liveCallsStarted = new AtomicInteger();
        // The callback each id was last subscribed with, kept after a cancel, for a call a live delegate makes late
        private final ConcurrentHashMap<String, Consumer<CloudEvent>> everSubscribed = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, LiveSubscription> subscriptions = new ConcurrentHashMap<>();
        // What a cancel that cancelled and then threw left for resumeSubscription to subscribe again
        private final ConcurrentHashMap<String, LiveSubscription> keptByAFailedCancel = new ConcurrentHashMap<>();
        private final AtomicInteger resumeCalls = new AtomicInteger();
        private volatile @Nullable RuntimeException cancelFailure;
        private volatile boolean cancelFailureCancelsFirst = false;
        // The thread the live delegate last delivered on, and its interrupt status right after the callback it was given
        // returned normally
        private volatile @Nullable Thread deliveryThread;
        private volatile @Nullable Boolean interruptedAfterTheAction;
        // Set before the next delivery reaches the callback it was given, once, as an interrupt that is already pending
        // when the live event is delivered
        private volatile boolean interruptBeforeTheNextDelivery = false;

        private record LiveSubscription(@Nullable SubscriptionFilter filter, Consumer<CloudEvent> action) {
        }

        private CheckpointAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate) {
            this(delegate, new StringBasedCheckpoint("in-memory-global-position"));
        }

        /**
         * @param checkpoint What {@link #globalCheckpoint()} answers. Null is a documented answer on the real
         *                   interface, meaning the model cannot report where the feed is, so it is a state a test has
         *                   to be able to put a model in.
         */
        private CheckpointAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate, @Nullable Checkpoint checkpoint) {
            this.delegate = delegate;
            this.checkpoint = checkpoint;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            liveStarts.add(String.valueOf(resolved));
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            subscriptions.put(subscriptionId, new LiveSubscription(filter, action));
            everSubscribed.put(subscriptionId, action);
            Consumer<CloudEvent> tracked = cloudEvent -> {
                liveCallsStarted.incrementAndGet();
                deliveryThread = Thread.currentThread();
                if (interruptBeforeTheNextDelivery) {
                    interruptBeforeTheNextDelivery = false;
                    Thread.currentThread().interrupt();
                }
                try {
                    action.accept(stampsCheckpoints ? new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint("checkpoint-of-" + cloudEvent.getId())) : cloudEvent);
                    interruptedAfterTheAction = Thread.currentThread().isInterrupted();
                    acknowledged.computeIfAbsent(subscriptionId, id -> new CopyOnWriteArrayList<>()).add(cloudEvent.getId());
                } catch (Throwable thrown) {
                    thrownByTheCallback.add(thrown);
                    throw thrown;
                } finally {
                    lastLiveCallEndedNanos = System.nanoTime();
                    liveCallsEnded.incrementAndGet();
                }
            };
            return delegate.subscribe(subscriptionId, filter, startAtToUse, tracked);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointReads.incrementAndGet();
            return checkpoint;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            resumeProbes.add(checkpoint);
            return !losesEveryLiveStart && !lost.contains(checkpoint) && resumeProbes.size() <= answersTrueToTheFirstProbes;
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            CountDownLatch blocked = isRunningBlockedAtShutdown;
            if (blocked != null && shutdownBegan) {
                // Bounded, so a caller that waits on it makes the test fail on what it asserts and not hang
                try {
                    blocked.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            resumeCalls.incrementAndGet();
            LiveSubscription kept = keptByAFailedCancel.remove(subscriptionId);
            return kept == null ? delegate.resumeSubscription(subscriptionId) : subscribe(subscriptionId, kept.filter(), StartAt.now(), kept.action());
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            RuntimeException failure = cancelFailure;
            if (failure != null && !cancelFailureCancelsFirst) {
                throw failure;
            }
            LiveSubscription cancelled = subscriptions.remove(subscriptionId);
            delegate.cancelSubscription(subscriptionId);
            if (failure != null) {
                if (cancelled != null) {
                    keptByAFailedCancel.put(subscriptionId, cancelled);
                }
                throw failure;
            }
        }

        // From now on a cancel throws failure without cancelling, as a live delegate that can't reach what holds the
        // subscription does, so the subscription keeps running
        private void cancelThrows(RuntimeException failure) {
            this.cancelFailure = failure;
            this.cancelFailureCancelsFirst = false;
        }

        // From now on a cancel cancels the subscription and then throws failure, as a durable live delegate does when
        // deleting its stored position fails. A resume subscribes it again. DurableSubscriptionModel over NativeMongoSubscriptionModel throws UnknownSubscriptionException on that resume instead, so the resume here models a live delegate that keeps the id.
        private void cancelsAndThenThrows(RuntimeException failure) {
            this.cancelFailure = failure;
            this.cancelFailureCancelsFirst = true;
        }

        private List<String> acknowledged(String subscriptionId) {
            return List.copyOf(acknowledged.getOrDefault(subscriptionId, List.of()));
        }

        private int resumeCalls() {
            return resumeCalls.get();
        }

        private CheckpointAwareInMemorySubscriptionModel stampingCheckpoints() {
            stampsCheckpoints = true;
            return this;
        }

        // The in-memory model waits for its delivery threads when it shuts down, so a live call that leaves the catch-up
        // wrapper while it shuts down reaches the durable model's save before that model stops writing
        @Override
        public void shutdown() {
            shutdownBegan = true;
            Runnable atShutdown = this.atShutdown;
            if (atShutdown != null) {
                atShutdown.run();
            }
            if (joinsLiveCallsOnShutdown) {
                // Bounded, so a live call that is never ended makes the test fail on what it asserts and not hang
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (liveCallsEnded.get() < liveCallsStarted.get() && System.nanoTime() < deadline) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            if (stampsCheckpoints || joinsLiveCallsOnShutdown) {
                delegate.shutdown();
            }
        }

        private CheckpointAwareInMemorySubscriptionModel runningAtShutdown(Runnable atShutdown) {
            this.atShutdown = atShutdown;
            return this;
        }

        // From now on a shutdown waits for the live calls in flight to end, without interrupting their threads, and
        // before it clears anything
        private CheckpointAwareInMemorySubscriptionModel joiningLiveCallsOnShutdown() {
            this.joinsLiveCallsOnShutdown = true;
            return this;
        }

        // From now on isRunning(id) blocks once this model's shutdown has begun, as a live delegate whose answer waits for
        // its own shutdown to finish does, until release is counted down or 10 seconds have passed
        private CheckpointAwareInMemorySubscriptionModel blockingIsRunningAtShutdownUntil(CountDownLatch release) {
            this.isRunningBlockedAtShutdown = release;
            return this;
        }

        // Calls the callback the id was last subscribed with on the calling thread, as a live delegate whose delivery was
        // already under way when the cancel returned does, and lets what it throws through
        private void deliverToTheCancelled(String subscriptionId, CloudEvent cloudEvent) {
            everSubscribed.get(subscriptionId).accept(cloudEvent);
        }

        // Whether the catch-up model, which marks itself as shutting down first, has asked this model to shut down
        private boolean shutdownBegan() {
            return shutdownBegan;
        }

        private List<Throwable> thrownByTheCallback() {
            return List.copyOf(thrownByTheCallback);
        }

        private long lastLiveCallEndedNanos() {
            return lastLiveCallEndedNanos;
        }

        private int liveCallsEnded() {
            return liveCallsEnded.get();
        }

        private @Nullable Thread deliveryThread() {
            return deliveryThread;
        }

        private @Nullable Boolean interruptedAfterTheAction() {
            return interruptedAfterTheAction;
        }

        // Forgets the status recorded so far and makes the next delivery start with the thread interrupted
        private void interruptTheNextDelivery() {
            interruptedAfterTheAction = null;
            interruptBeforeTheNextDelivery = true;
        }
    }
}
