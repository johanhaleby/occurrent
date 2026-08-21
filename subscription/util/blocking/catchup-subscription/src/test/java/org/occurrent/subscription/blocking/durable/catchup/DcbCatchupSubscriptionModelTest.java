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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * Tests for {@link CatchupSubscriptionModel} in DCB mode (replay and resume by {@code position}, see ADR 20).
 * <p>
 * These use the in-memory event store and subscription model so the DCB-specific logic (position-windowed replay,
 * position resume, the query post-filter and the multi-window paging) is exercised deterministically without a
 * database. The in-memory subscription model is not position aware, so a small {@link CheckpointAwareInMemorySubscriptionModel}
 * test double adapts it: it translates the concrete resume position the catch-up hands over into {@code StartAt.now()}
 * (the in-memory model only supports now and default) and reports a stub global position. The faithful change-stream
 * resume across the catch-up to live seam is exercised against a real MongoDB change stream by
 * {@code DcbCatchupSubscriptionModelMongoTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbCatchupSubscriptionModelTest {

    private InMemorySubscriptionModel inMemorySubscriptionModel;
    private CheckpointAwareInMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private LocalDateTime time;

    @BeforeEach
    void create_instances() {
        inMemorySubscriptionModel = new InMemorySubscriptionModel();
        subscriptionModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel);
        eventStore = new InMemoryEventStore(inMemorySubscriptionModel);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        time = LocalDateTime.now();
    }

    @AfterEach
    void shutdown() {
        inMemorySubscriptionModel.shutdown();
    }

    // The same contract the stream model reports, through the dispatcher a DCB projection actually subscribes on.
    // Both answer true while the history that was already there is being read, and isReplayingHistory turns false for
    // the events appended since.
    @Test
    void reports_the_history_read_and_the_reconciliation_as_different_parts_of_one_catch_up() {
        appendTagged("name:1", nameDefined("history"));

        CopyOnWriteArrayList<String> phasePerDelivery = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            phasePerDelivery.add(subscription.isCatchingUp("subscription") + "/" + subscription.isReplayingHistory("subscription"));
            // Appended from inside the history read, so it cannot be part of it and the reconciliation is what
            // delivers it.
            if (phasePerDelivery.size() == 1) {
                appendTagged("name:1", nameDefined("appendedDuringTheHistoryRead"));
            }
        }).waitUntilStarted();

        await().untilAsserted(() -> assertThat(phasePerDelivery).containsExactly("true/true", "true/false"));
        assertThat(subscription.isCatchingUp("subscription")).isFalse();
        assertThat(subscription.isReplayingHistory("subscription")).isFalse();
    }

    @Test
    void replays_matching_dcb_events_from_the_beginning_of_the_sequence_in_position_order() {
        NameDefined name1 = nameDefined("name1");
        NameDefined name2 = nameDefined("name2");
        NameDefined name3 = nameDefined("name3");
        appendTagged("name:1", name1);
        appendTagged("other:1", nameDefined("ignored"));
        appendTagged("name:1", name2);
        appendTagged("name:1", name3);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        AtomicBoolean replayedOnVirtualThread = new AtomicBoolean(false);
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            replayedOnVirtualThread.set(Thread.currentThread().isVirtual());
            received.add(cloudEventConverter.toDomainEvent(cloudEvent));
        }).waitUntilStarted();

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(name1, name2, name3);
            assertThat(replayedOnVirtualThread).isTrue();
        });
    }

    @Test
    void delivers_events_written_during_and_after_catchup_through_the_live_handover_without_duplicates() {
        NameDefined historic1 = nameDefined("historic1");
        NameDefined historic2 = nameDefined("historic2");
        appendTagged("name:1", historic1);
        appendTagged("name:1", historic2);

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();
        await().untilAsserted(() -> assertThat(received).containsExactly(historic1, historic2));

        NameDefined live1 = nameDefined("live1");
        NameDefined live2 = nameDefined("live2");
        appendTagged("name:1", live1);
        appendTagged("other:1", nameDefined("ignoredLive"));
        appendTagged("name:1", live2);

        await().untilAsserted(() -> {
            assertThat(received).containsExactly(historic1, historic2, live1, live2);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void resumes_replay_after_a_supplied_dcb_position_and_skips_earlier_events() {
        NameDefined position1 = nameDefined("position1");
        NameDefined position2 = nameDefined("position2");
        NameDefined position3 = nameDefined("position3");
        appendTagged("name:1", position1); // position 1
        appendTagged("name:1", position2); // position 2
        appendTagged("name:1", position3); // position 3

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(2)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position3));
    }

    @Test
    void resumes_replay_from_a_dcb_position_read_back_from_storage() {
        appendTagged("name:1", nameDefined("position1")); // position 1
        NameDefined position2 = nameDefined("position2");
        appendTagged("name:1", position2);                // position 2

        CheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save("subscription", GlobalCheckpoint.of(1));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")),
                new CatchupSubscriptionModelConfig(useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));

        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactly(position2));
    }

    @Test
    void live_only_subscription_applies_the_dcb_query_post_filter() {
        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        // Default start with nothing stored subscribes live (no replay), mirroring the stream path.
        subscription.subscribe("subscription", StartAt.subscriptionModelDefault(), toDomainEvents(received)).waitUntilStarted();

        NameDefined matching = nameDefined("matching");
        appendTagged("name:1", matching);
        appendTagged("other:1", nameDefined("nonMatching"));

        await().untilAsserted(() -> assertThat(received).containsExactly(matching));
    }

    @Test
    void replays_across_multiple_position_windows() {
        List<NameDefined> events = List.of(nameDefined("e1"), nameDefined("e2"), nameDefined("e3"), nameDefined("e4"), nameDefined("e5"));
        events.forEach(event -> appendTagged("name:1", event));

        CopyOnWriteArrayList<DomainEvent> received = new CopyOnWriteArrayList<>();
        // A window of 2 positions forces the replay to page across several windows to cover all five events.
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")),
                new CatchupSubscriptionModelConfig(100).dcbCatchupPositionWindowSize(2));

        subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), toDomainEvents(received)).waitUntilStarted();

        await().untilAsserted(() -> assertThat(received).containsExactlyElementsOf(events));
    }

    @Test
    void position_catchup_fails_loudly_instead_of_silently_resuming_at_now_when_the_delegate_reports_no_resume_token() {
        appendTagged("name:1", nameDefined("position1"));

        CheckpointAwareSubscriptionModel nullCheckpointSubscriptionModel = new CheckpointAwareInMemorySubscriptionModel(inMemorySubscriptionModel) {
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                return null;
            }
        };
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(nullCheckpointSubscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));

        Subscription started = subscription.subscribe("subscription", StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
        });

        assertThat(started).isInstanceOf(CatchupSubscription.class);
        Future<Subscription> delegatedSubscription = ((CatchupSubscription) started).delegatedSubscription();
        assertThatThrownBy(() -> delegatedSubscription.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .cause().hasMessageContaining("no resume token");
    }

    /**
     * Before per-attempt identity (issue #737, finding 4), the running-catch-up marker was keyed only by
     * {@code subscriptionId}, with no way to tell a cancelled attempt's own entry from a later attempt's. A
     * cancelled replay whose virtual thread had not yet noticed would finish after a resubscribe for the same id had
     * already started a second replay, and blindly remove that second replay's marker instead of its own. See the
     * identical test on {@code StreamCatchupSubscriptionModelTest} for the blocking stream side of the same fix.
     */
    @Test
    void a_cancelled_replays_late_completion_does_not_disturb_a_later_attempts_bookkeeping_for_the_same_id() throws InterruptedException {
        appendTagged("name:1", nameDefined("event1"));
        CatchupSubscriptionModel subscription = new CatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")));
        String subscriptionId = "subscription";

        CountDownLatch firstReplayReached = new CountDownLatch(1);
        CountDownLatch releaseFirstReplay = new CountDownLatch(1);
        Subscription first = subscription.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            firstReplayReached.countDown();
            awaitLatch(releaseFirstReplay);
        });
        assertThat(firstReplayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // Cancel while the first attempt is still blocked inside its replay, then resubscribe the same id before
        // that blocked thread has had any chance to notice the cancellation.
        subscription.cancelSubscription(subscriptionId);

        CountDownLatch secondReplayReached = new CountDownLatch(1);
        CountDownLatch releaseSecondReplay = new CountDownLatch(1);
        Subscription second = subscription.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
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

    /**
     * Issue #827 (deferred from PR 823's per-attempt identity fix, #737), the DCB side of the same gap the blocking
     * stream catch-up has. The identity check made the ownership decision itself atomic, but not what followed it.
     * A cancellation landing after this attempt decided it was still current, but before the delegate subscribe
     * below actually ran, found nothing left in the map to flag, and the delegate did not know the id yet either,
     * so the cancellation was silently lost and the delegate went live anyway. Closing the gap needs a lock
     * spanning the whole handover, not just the decision.
     */
    @Test
    void a_cancellation_racing_the_final_delegate_subscribe_is_not_lost() throws InterruptedException {
        appendTagged("name:1", nameDefined("event1"));
        String subscriptionId = "subscription";
        CountDownLatch reachedFinishing = new CountDownLatch(1);
        CountDownLatch releaseFinishing = new CountDownLatch(1);
        DcbCatchupSubscriptionModel subscription = new DcbCatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(100)) {
            @Override
            protected boolean endReplayIfStillCurrent(String subscriptionId) {
                boolean stillCurrent = super.endReplayIfStillCurrent(subscriptionId);
                reachedFinishing.countDown();
                awaitLatch(releaseFinishing);
                return stillCurrent;
            }
        };

        subscription.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
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
     * Issue #827, the checkpoint-delete manifestation of the same gap on the DCB side. A finishing attempt's
     * temporary checkpoint delete runs after it already relinquished ownership of the id. Unguarded, a fresh
     * attempt for the same id can register and save its own position in that gap, and the stale delete then
     * removes the fresh attempt's position instead of its own. Closing the gap needs holding the same lock across
     * the delete and gating a fresh attempt's registration on it too.
     */
    @Test
    void a_finishing_attempts_late_checkpoint_delete_does_not_clobber_a_fresh_attempts_saved_position() throws InterruptedException {
        appendTagged("name:1", nameDefined("event1"));
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
        // overridden method too, for its own identity decision, and must not be blocked by it as well.
        AtomicBoolean firstCallToEndReplayIfStillCurrent = new AtomicBoolean(true);
        DcbCatchupSubscriptionModel subscription = new DcbCatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")), config) {
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

        // Resolves to a real DCB position for the catch-up's own replay, but to null for the delegate, so the
        // finishing tail's "delegate must not run" branch deletes the temporary catch-up position once done.
        StartAt neverDelegates = StartAt.dynamic(context ->
                context.hasSubscriptionModelType(CheckpointAwareInMemorySubscriptionModel.class) ? null : StartAt.checkpoint(GlobalCheckpoint.of(0)));

        subscription.subscribe(subscriptionId, neverDelegates, cloudEvent -> {
        });
        assertThat(reachedFinishing.await(5, TimeUnit.SECONDS)).isTrue();

        // A fresh, normally-delegating attempt for the same id, started on its own thread while the first is
        // blocked right after deciding it is still current but before its own delete runs.
        Thread freshAttempt = new Thread(() -> {
            try {
                subscription.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
                }).waitUntilStarted();
            } catch (RuntimeException ignored) {
                // The stale attempt above also subscribes the delegate unconditionally once its own handover
                // decides it is still current, even though its own StartAt resolved to null for the delegate, so
                // the two attempts' delegate subscribe calls can then collide on the in-memory model's duplicate-id
                // check. Immaterial here, this test only cares whether the fresh attempt's own checkpoint save
                // (asserted below) survives, and that save runs before this call, not after it.
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

        assertThat(freshSaved.await(5, TimeUnit.SECONDS))
                .as("the fresh attempt must have saved its own position by the time both attempts have finished")
                .isTrue();
        assertThat(storage.read(subscriptionId))
                .as("a finishing attempt's late, now-stale checkpoint delete must not remove a fresh attempt's own "
                        + "saved position for the same id")
                .isNotNull();
    }

    /**
     * Copilot review on PR #839 (issue #827), a fourth finding on top of the two already fixed above, the DCB side
     * of the same gap. Serializing only calls that go through {@code startCatchupAsync} missed {@code subscribe}'s
     * own direct-to-live branches, which hand the id straight to the live delegate without ever starting a
     * catch-up. A same-id catch-up still replaying, unaware a live subscribe has already claimed its id, would
     * still subscribe the delegate itself once its replay finishes, racing the delegate subscribe the direct live
     * subscribe already made.
     */
    @Test
    void a_live_subscribe_for_the_same_id_supersedes_a_still_replaying_catchups_own_delegate_subscribe() throws Exception {
        appendTagged("name:1", nameDefined("event1"));
        String subscriptionId = "subscription";
        CountDownLatch replayReached = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);

        DcbCatchupSubscriptionModel subscription = new DcbCatchupSubscriptionModel(subscriptionModel, eventStore, DcbCriteria.tags(Tag.parse("name:1")), new CatchupSubscriptionModelConfig(100));

        Subscription staleAttempt = subscription.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), cloudEvent -> {
            replayReached.countDown();
            awaitLatch(releaseReplay);
        });
        assertThat(replayReached.await(5, TimeUnit.SECONDS)).isTrue();

        // A live subscribe for the same id, bypassing catch-up entirely since StartAt.now() is not a DCB catch-up
        // position, while the catch-up above is still blocked mid-replay, well before it reaches its own handover
        // lock.
        subscription.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
        }).waitUntilStarted();

        releaseReplay.countDown();

        Future<Subscription> staleDelegatedSubscription = ((CatchupSubscription) staleAttempt).delegatedSubscription();
        assertThat(staleDelegatedSubscription.get(5, TimeUnit.SECONDS))
                .as("a live subscribe claiming an id while a same-id catch-up is still replaying must cancel that "
                        + "catch-up's finishing tail instead of leaving it to also subscribe the delegate once its "
                        + "replay catches up, colliding with the live subscribe that already claimed the same id")
                .isInstanceOf(CancelledSubscription.class);
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

    private void appendTagged(String tag, DomainEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(events)).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse(tag))))
                .toList();
        eventStore.append(cloudEvents);
    }

    /**
     * Adapts the (non position aware) {@link InMemorySubscriptionModel} to {@link CheckpointAwareSubscriptionModel} for
     * these tests. The catch-up hands over to the live phase with a concrete checkpoint, but the in-memory
     * model only supports {@code now}/{@code default}, so any position start is translated to {@code now}. The stub
     * global position is enough for the catch-up to take its normal handover path.
     */
    private static class CheckpointAwareInMemorySubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;

        private CheckpointAwareInMemorySubscriptionModel(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            StartAt startAtToUse = resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
            return delegate.subscribe(subscriptionId, filter, startAtToUse, action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
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
