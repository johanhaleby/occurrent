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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * What a blocking resume at a position stores for a subscription whose stored position is a catch-up's, so that a
 * restart and a later pause and resume start from what is stored.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupResumeStoresARestartablePositionTest {

    private static final String ID = "subscription";
    private static final Checkpoint LATER_TOKEN = new StringBasedCheckpoint("later-token");

    private final CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private InMemorySubscriptionModel restartedInMemorySubscriptionModel;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private InMemoryEventStore eventStore;
    private InMemoryCheckpointStorage storage;
    private LiveStartLosingSubscriptionModel liveModel;
    private ReadingNoPresentPosition liveModelReadingNoPresentPosition;
    private StreamCatchupSubscriptionModel catchup;
    private StreamCatchupSubscriptionModel restarted;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        restartedInMemorySubscriptionModel = new InMemorySubscriptionModel();
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        LocalDateTime time = LocalDateTime.now();
        List.of(nameDefined(time, "event1"), nameDefined(time, "event2"), nameDefined(time, "event3")).forEach(this::write);
        storage = new InMemoryCheckpointStorage();
    }

    @AfterEach
    void shutdown() {
        if (catchup != null) {
            catchup.shutdown();
        }
        if (restarted != null) {
            restarted.shutdown();
        }
        inMemorySubscriptionModel.shutdown();
        restartedInMemorySubscriptionModel.shutdown();
    }

    @Test
    void a_restart_with_the_default_start_starts_and_a_pause_and_resume_works_after_a_resume_at_a_global_position_over_a_stored_catch_up_position() {
        // Given
        Checkpoint storedBefore = heldTakeoverReplayWithACatchupPositionStored();

        // When
        catchup.resumeSubscription(ID, StartAt.checkpoint(GlobalCheckpoint.of(5)));

        // Then
        assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore);
        assertARestartStartsAndAPauseAndResumeWorks();
    }

    @Test
    void a_restart_with_the_default_start_starts_and_a_pause_and_resume_works_after_a_resume_at_a_time_over_a_stored_catch_up_position() {
        // Given
        heldTakeoverReplayWithACatchupPositionStored();
        Checkpoint resumedAt = TimeBasedCheckpoint.from(OffsetDateTime.now().minusHours(1));

        // When
        catchup.resumeSubscription(ID, StartAt.checkpoint(resumedAt));

        // Then
        Checkpoint stored = storage.read(ID);
        assertThat(CatchupCheckpoints.isMarkedTime(stored)).as("stored position is a marked time").isTrue();
        CatchupCheckpoints.Time storedTime = (CatchupCheckpoints.Time) CatchupCheckpoints.parse(stored);
        CatchupCheckpoints.Time resumedTime = (CatchupCheckpoints.Time) CatchupCheckpoints.parse(resumedAt);
        assertAll(
                () -> assertThat(storedTime.time()).as("stored time").isEqualTo(resumedTime.time()),
                () -> assertThat(storedTime.liveFrom()).as("live start stored with the time").isNotNull()
        );
        assertARestartStartsAndAPauseAndResumeWorks();
    }

    @Test
    void throws_an_IllegalStateException_naming_the_time_and_stores_nothing_when_a_resume_at_a_time_without_a_live_start_over_a_stored_catch_up_position_finds_no_present_position() {
        // Given
        Checkpoint storedBefore = heldTakeoverReplayWithACatchupPositionStored(ReadingNoPresentPosition::new);
        liveModelReadingNoPresentPosition.answeringNothingForTheCurrentPosition();
        Checkpoint resumedAt = TimeBasedCheckpoint.from(OffsetDateTime.now().minusHours(1));

        // When
        Throwable thrown = catchThrowable(() -> catchup.resumeSubscription(ID, StartAt.checkpoint(resumedAt)));

        // Then
        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ID)
                .hasMessageContaining(resumedAt.asString())
                .hasMessageNotContaining("StartAt.now()");
        assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore);
    }

    @Test
    void leaves_the_stored_position_when_a_resume_is_at_a_token_the_live_model_does_not_restart_at() {
        // Given
        Checkpoint storedBefore = heldTakeoverReplayWithACatchupPositionStored();
        liveModel.notRestartingAt(LATER_TOKEN);

        // When
        catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN));

        // Then
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore),
                () -> await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(liveModel.openedAt).as("the live starts the live model opened").containsExactly(LATER_TOKEN))
        );
    }

    @Test
    void stores_a_token_the_live_model_restarts_at_over_a_stored_catch_up_position() {
        // Given
        heldTakeoverReplayWithACatchupPositionStored();

        // When
        catchup.resumeSubscription(ID, StartAt.checkpoint(LATER_TOKEN));

        // Then
        assertAll(
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(LATER_TOKEN),
                () -> await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(liveModel.openedAt).as("the live starts the live model opened").containsExactly(LATER_TOKEN))
        );
    }

    @Test
    void stores_the_position_a_dynamic_start_answers_the_catch_up_and_resumes_the_live_model_at_it_when_the_function_answers_differently_each_time_it_is_asked() {
        // Given
        heldTakeoverReplayWithACatchupPositionStored();
        List<Class<?>> askedFor = new CopyOnWriteArrayList<>();
        StartAt dynamic = StartAt.dynamic(context -> {
            askedFor.add(context.subscriptionModelType());
            return StartAt.checkpoint(new StringBasedCheckpoint("answer-" + askedFor.size()));
        });

        // When
        catchup.resumeSubscription(ID, dynamic);

        // Then
        long askedForTheCatchup = askedFor.stream().filter(StreamCatchupSubscriptionModel.class::equals).count();
        assertAll(
                () -> assertThat(askedForTheCatchup).as("times the function was asked for the catch-up's context").isEqualTo(1),
                () -> await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(liveModel.openedAt).as("the live starts the live model opened, the position stored").containsExactly(storage.read(ID)))
        );
    }

    @Test
    void resumes_the_live_model_at_the_position_a_dynamic_start_answers_it_and_leaves_the_stored_position_when_the_function_answers_the_catch_up_nothing() {
        // Given
        Checkpoint storedBefore = heldTakeoverReplayWithACatchupPositionStored();
        Checkpoint answeredTheLiveModel = new StringBasedCheckpoint("answered-the-live-model");
        StartAt dynamic = StartAt.dynamic(context -> StreamCatchupSubscriptionModel.class.equals(context.subscriptionModelType()) ? null : StartAt.checkpoint(answeredTheLiveModel));

        // When
        catchup.resumeSubscription(ID, dynamic);

        // Then
        assertAll(
                () -> await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(liveModel.openedAt).as("the live starts the live model opened, the position the function answered it").containsExactly(answeredTheLiveModel)),
                () -> assertThat(storage.read(ID)).as("stored position").isEqualTo(storedBefore)
        );
    }

    // A takeover replay from the beginning whose live start the live model loses at each open, held once the catch-up
    // made 4 replays, with the catch-up position it stored. The live model keeps every open after this returns
    private Checkpoint heldTakeoverReplayWithACatchupPositionStored() {
        return heldTakeoverReplayWithACatchupPositionStored(Function.<CheckpointAwareSubscriptionModel>identity()::apply);
    }

    // As above, with the catch-up over what wrap makes of the live model
    private <M extends CheckpointAwareSubscriptionModel> Checkpoint heldTakeoverReplayWithACatchupPositionStored(Function<LiveStartLosingSubscriptionModel, M> wrap) {
        liveModel = new LiveStartLosingSubscriptionModel(inMemorySubscriptionModel, true, false, () -> {
        }).losingEveryOpen();
        M wrapped = wrap.apply(liveModel);
        if (wrapped instanceof ReadingNoPresentPosition reading) {
            liveModelReadingNoPresentPosition = reading;
        }
        catchup = new StreamCatchupSubscriptionModel(wrapped, eventStore, config());
        HeldPausedReports reports = new HeldPausedReports();
        catchup.addStartRefusalListener(reports);
        catchup.subscribe(ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), action());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(reports.heldPaused).as("held paused reports").hasSize(1));
        Checkpoint stored = storage.read(ID);
        assertThat(stored).as("position the takeover replay stored").isNotNull().matches(GlobalCheckpoint::isGlobalCheckpoint, "a catch-up position");
        liveModel.keepingEveryOpen();
        return stored;
    }

    // A new model over the same storage and event store, as a restart makes
    private void assertARestartStartsAndAPauseAndResumeWorks() {
        catchup.shutdown();
        LiveStartLosingSubscriptionModel restartedLiveModel = new LiveStartLosingSubscriptionModel(restartedInMemorySubscriptionModel, true, false, () -> {
        }).neverLosingAnOpen();
        restarted = new StreamCatchupSubscriptionModel(restartedLiveModel, eventStore, config());
        AtomicBoolean started = new AtomicBoolean();
        Throwable subscribeThrew = catchThrowable(() -> started.set(restarted.subscribe(ID, StartAt.subscriptionModelDefault(), action()).waitUntilStarted(Duration.ofSeconds(10))));
        assertThat(subscribeThrew).as("thrown by the subscribe of the restart").isNull();
        assertThat(started).as("restart started").isTrue();
        await().untilAsserted(() -> assertThat(restartedLiveModel.isRunning(ID)).as("handed over to the live model").isTrue());

        restarted.pauseSubscription(ID);
        Throwable resumeThrew = catchThrowable(() -> restarted.resumeSubscription(ID));

        assertThat(resumeThrew).as("thrown by the resume after the pause").isNull();
        assertThat(restartedLiveModel.isRunning(ID)).as("running after the resume").isTrue();
    }

    private CatchupSubscriptionModelConfig config() {
        return new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
    }

    private Consumer<CloudEvent> action() {
        return cloudEvent -> received.add(cloudEventConverter.toDomainEvent(cloudEvent));
    }

    // The live model, which answers nothing when asked for its current position once told to
    private static final class ReadingNoPresentPosition implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {
        private final LiveStartLosingSubscriptionModel delegate;
        private volatile boolean answersNothingForTheCurrentPosition;

        private ReadingNoPresentPosition(LiveStartLosingSubscriptionModel delegate) {
            this.delegate = delegate;
        }

        void answeringNothingForTheCurrentPosition() {
            answersNothingForTheCurrentPosition = true;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return answersNothingForTheCurrentPosition ? null : delegate.globalCheckpoint();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return delegate.resumeSubscription(subscriptionId, startAt);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void addHistoryLossListener(HistoryLossListener listener) {
            delegate.addHistoryLossListener(listener);
        }

        @Override
        public void removeHistoryLossListener(HistoryLossListener listener) {
            delegate.removeHistoryLossListener(listener);
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return delegate.canResumeFrom(checkpoint);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return delegate.restartsAt(checkpoint);
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
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }
    }

    private static final class HeldPausedReports implements StartRefusalListener {
        private final List<CatchupCannotResumeException> heldPaused = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
        }

        @Override
        public void handleHeldPaused(String subscriptionId, CatchupCannotResumeException refusal) {
            heldPaused.add(refusal);
        }
    }

    private static NameDefined nameDefined(LocalDateTime time, String name) {
        return new NameDefined(UUID.randomUUID().toString(), time, "name", name);
    }

    private void write(DomainEvent event) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event));
        eventStore.write(event.eventId(), cloudEvents);
    }
}
