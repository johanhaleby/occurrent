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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

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
            protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupAttempt attempt, Callable<Subscription> catchup) {
                return super.startCatchupAsync(subscriptionId, attempt, () -> {
                    runningMarkedBeforeCatchupRuns.set(runningCatchupSubscriptions.containsKey(subscriptionId));
                    return catchup.call();
                });
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
        assertThat(secondReplayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // Release the stale first attempt while the second is still in flight, mid-replay, unresolved.
        releaseFirstReplay.countDown();
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

    @Test
    void is_catching_up_rejects_a_null_subscription_id() {
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        StreamCatchupSubscriptionModel subscription = new StreamCatchupSubscriptionModel(subscriptionModel, eventStore, new CatchupSubscriptionModelConfig(100));

        Throwable thrown = catchThrowable(() -> subscription.isCatchingUp(null));

        assertThat(thrown).isInstanceOf(NullPointerException.class);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link CheckpointAwareSubscriptionModel} for
     * these tests, mirroring the catchup-subscription module's own test double: any position start is translated
     * to {@code now}, since the in-memory model only supports {@code now}/{@code default}.
     */
    private static final class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private final @Nullable Checkpoint checkpoint;

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
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            return delegate.subscribe(subscriptionId, filter, startAtToUse, action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return checkpoint;
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
