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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The protected methods of a catch-up model that read the wrapped model on the replay thread, called by a subclass
 * from some other thread, still reach the wrapped model, with a stillWanted that always answers true.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupProtectedMethodsOffTheReplayThreadTest {

    private static final String ID = "subscription";
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("live-start");

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private RecordingLiveModel live;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        live = new RecordingLiveModel(inMemorySubscriptionModel);
        catchup = new StreamCatchupSubscriptionModel(live, new InMemoryEventStore(inMemorySubscriptionModel), new CatchupSubscriptionModelConfig(100));
    }

    @AfterEach
    void shut_down() {
        catchup.shutdown();
        inMemorySubscriptionModel.shutdown();
    }

    @Test
    void captureLiveResumeCheckpoint_called_off_a_replay_thread_asks_the_wrapped_model_for_its_global_checkpoint_with_a_stillWanted_that_answers_true() {
        // Given
        AtomicReference<Checkpoint> captured = new AtomicReference<>();

        // When
        assertThatCode(() -> captured.set(catchup.captureLiveResumeCheckpoint(null))).as("capturing the live resume checkpoint").doesNotThrowAnyException();

        // Then
        assertAll(
                () -> assertThat(captured.get()).as("captured").isEqualTo(LIVE_START),
                () -> assertThat(live.globalCheckpointStillWanted).as("the stillWanted the wrapped model got for globalCheckpoint").hasSize(1).allSatisfy(stillWanted -> assertThat(stillWanted.getAsBoolean()).as("what it answers").isTrue())
        );
    }

    @Test
    void positionReplayStart_called_off_a_replay_thread_asks_the_wrapped_model_whether_it_can_resume_from_the_stored_live_start_with_a_stillWanted_that_answers_true() {
        // Given a position checkpoint that stores a live start
        Checkpoint start = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 0, 3, LIVE_START));
        AtomicReference<AbstractCatchupSubscriptionModel.PositionReplayStart> replayStart = new AtomicReference<>();

        // When
        assertThatCode(() -> replayStart.set(catchup.positionReplayStart(ID, start, null, null))).as("starting a position replay").doesNotThrowAnyException();

        // Then
        assertAll(
                () -> assertThat(replayStart.get()).as("replay start").isNotNull().extracting(AbstractCatchupSubscriptionModel.PositionReplayStart::liveFrom).as("live start").isEqualTo(LIVE_START),
                () -> assertThat(live.canResumeFromStillWanted).as("the stillWanted the wrapped model got for canResumeFrom").hasSize(1).allSatisfy(stillWanted -> assertThat(stillWanted.getAsBoolean()).as("what it answers").isTrue())
        );
    }

    @Test
    void timeReplayStart_called_off_a_replay_thread_asks_the_wrapped_model_whether_it_can_resume_from_the_stored_live_start_with_a_stillWanted_that_answers_true() {
        // Given a time checkpoint that stores a live start
        String beginningOfTime = TimeBasedCheckpoint.beginningOfTime().asString();
        Checkpoint start = new StringBasedCheckpoint(CatchupCheckpoints.format(beginningOfTime, beginningOfTime, LIVE_START));
        AtomicReference<AbstractCatchupSubscriptionModel.TimeReplayStart> replayStart = new AtomicReference<>();

        // When
        assertThatCode(() -> replayStart.set(catchup.timeReplayStart(ID, start, null, null))).as("starting a time replay").doesNotThrowAnyException();

        // Then
        assertAll(
                () -> assertThat(replayStart.get()).as("replay start").isNotNull().extracting(AbstractCatchupSubscriptionModel.TimeReplayStart::liveFrom).as("live start").isEqualTo(LIVE_START),
                () -> assertThat(live.canResumeFromStillWanted).as("the stillWanted the wrapped model got for canResumeFrom").hasSize(1).allSatisfy(stillWanted -> assertThat(stillWanted.getAsBoolean()).as("what it answers").isTrue())
        );
    }

    /**
     * A wrapped model that records the stillWanted it is given and answers as a model with history does.
     */
    private static final class RecordingLiveModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        final CopyOnWriteArrayList<BooleanSupplier> globalCheckpointStillWanted = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<BooleanSupplier> canResumeFromStillWanted = new CopyOnWriteArrayList<>();

        private RecordingLiveModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return LIVE_START;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint(BooleanSupplier stillWanted) {
            globalCheckpointStillWanted.add(stillWanted);
            return LIVE_START;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint, BooleanSupplier stillWanted) {
            canResumeFromStillWanted.add(stillWanted);
            return true;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
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
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
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
}
