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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DurableSubscriptionModel#resumeSubscription(String)} against a recording fake rather than a real Mongo
 * model, so each of the four decisions it makes (reposition from the checkpoint, fall back for no checkpoint, fall
 * back for a non-repositionable delegate, and the opt-out guard) is isolated from a change stream and provable on
 * its own. {@link CompetingConsumerLeaseRegainResumesFromCheckpointTest} proves the same mechanism end to end over
 * real Mongo.
 * <p>
 * {@code subscribe}, {@code cancelSubscription} and {@code resumeSubscription} for one id are mutually exclusive
 * under {@code subscriptionIdLock}, so the concurrency tests here assert that a call blocks while another for the
 * same id is still in flight, and that it sees the correct, settled state once released, rather than asserting
 * against a specific unsynchronized interleaving.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeRepositioningTest {

    private static final String SUBSCRIPTION_ID = "subscription";

    @Test
    void resume_reopens_from_the_stored_checkpoint_rather_than_the_delegates_own_position() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        Checkpoint stored = new StringBasedCheckpoint("stored-checkpoint");
        storage.save(SUBSCRIPTION_ID, stored);
        RecordingRepositionableSubscriptionModel delegate = new RecordingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.plainResumeCalled).as("a stored checkpoint must win over the delegate's own tracked position").isFalse();
        assertThat(delegate.repositionedTo).isInstanceOf(StartAt.StartAtCheckpoint.class);
        assertThat(((StartAt.StartAtCheckpoint) delegate.repositionedTo).checkpoint).isEqualTo(stored);
    }

    @Test
    void falls_back_to_the_delegates_own_resume_when_no_checkpoint_is_stored() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RecordingRepositionableSubscriptionModel delegate = new RecordingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.plainResumeCalled)
                .as("nothing is stored yet, so the delegate's own tracked position is the only position there is, "
                        + "never StartAt.subscriptionModelDefault() which would resolve to the present and drop the paused window")
                .isTrue();
        assertThat(delegate.repositionedTo).isNull();
    }

    @Test
    void falls_back_to_the_delegates_own_resume_when_the_delegate_is_not_repositionable() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.plainResumeCalled)
                .as("a checkpoint is stored, but the delegate cannot be resumed at an explicit position, so its own resume is the only option")
                .isTrue();
    }

    @Test
    void a_subscription_that_opted_out_of_checkpoint_management_resumes_without_reading_the_checkpoint_even_when_one_exists() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RecordingRepositionableSubscriptionModel delegate = new RecordingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        // Opts this subscription id out of DurableSubscriptionModel's checkpoint management, the same StartAt shape
        // CompetingConsumerSubscriptionModel's own class-level javadoc uses to opt a subscription out of competing
        // consumption.
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();
        // Left behind by something else, or by an earlier subscription with the same id that was checkpoint managed.
        // Either way, this model was never asked to manage this subscription id, so the checkpoint is not its to read.
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.plainResumeCalled).isTrue();
        assertThat(delegate.repositionedTo).as("an opted-out subscription must not be repositioned from storage").isNull();
    }

    /**
     * Issue #737, finding 2. The opt-out marker used to be cleared only on a failed delegate subscribe (the #690
     * guarantee above); nothing cleared it on the managed path, so a stale marker left by an earlier opt-out call
     * for this id survived into a later, managed call for the same id, still same id resubscribe without an
     * intervening cancel, e.g. a stateful {@code StartAt.dynamic} that answers differently across calls.
     */
    @Test
    void a_later_subscribe_that_resolves_managed_clears_the_opt_out_marker_an_earlier_call_left_behind() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RecordingRepositionableSubscriptionModel delegate = new RecordingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();

        // Same id, no intervening cancel, but this call resolves managed rather than opting out.
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        }).waitUntilStarted();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("this id is managed now, so the stale opt-out marker must not make resumeSubscription skip "
                        + "repositioning from the stored checkpoint")
                .isInstanceOf(StartAt.StartAtCheckpoint.class);
    }

    /**
     * Copilot review on PR #823 (issue #737, finding 2). The managed path's marker removal ran before the delegate
     * subscribe call, so a managed subscribe the delegate refused as a duplicate against a still-active, opted-out
     * subscription for the same id still lost that active subscription's marker, even though nothing about it
     * actually changed.
     */
    @Test
    void a_failed_managed_subscribe_against_an_active_opted_out_id_does_not_disturb_its_marker() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DuplicateRejectingRepositionableSubscriptionModel delegate = new DuplicateRejectingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();

        // Same id, still active and opted out. This call resolves managed, and the delegate refuses it as a
        // duplicate of the still-registered opted-out subscription.
        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        })).isInstanceOf(RuntimeException.class);

        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("the failed managed subscribe must not have cleared the still-active opted-out subscription's "
                        + "marker, or its next resume gets wrongly repositioned from storage")
                .isNull();
        assertThat(delegate.plainResumeCalled).isTrue();
    }

    @Test
    void a_delegate_subscribe_that_throws_leaves_no_opt_out_marker_behind_for_a_later_resubscribe() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ThrowingOnSubscribeRepositionableSubscriptionModel delegate = new ThrowingOnSubscribeRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        })).isInstanceOf(RuntimeException.class);

        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("the failed subscribe attempt must not have opted this id out of checkpoint management, so a "
                        + "same-JVM resubscribe still repositions from the stored checkpoint (the #690 guarantee)")
                .isInstanceOf(StartAt.StartAtCheckpoint.class);
    }

    @Test
    void a_delegate_subscribe_that_throws_an_error_also_leaves_no_opt_out_marker_behind() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ErrorThrowingOnSubscribeRepositionableSubscriptionModel delegate = new ErrorThrowingOnSubscribeRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        })).isInstanceOf(Error.class);

        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("an Error out of the delegate's subscribe must not leave the marker behind either, or it never "
                        + "clears and every later resubscribe with this id is treated as opted out forever")
                .isInstanceOf(StartAt.StartAtCheckpoint.class);
    }

    @Test
    void a_failed_duplicate_opt_out_subscribe_does_not_disturb_the_first_ones_marker() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DuplicateRejectingRepositionableSubscriptionModel delegate = new DuplicateRejectingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        })).isInstanceOf(RuntimeException.class);

        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("the second call's failure must not disturb the first, still-active subscription's marker, "
                        + "or its next resume gets wrongly repositioned from storage")
                .isNull();
        assertThat(delegate.plainResumeCalled).isTrue();
    }

    @Test
    void a_duplicate_opt_out_subscribe_against_an_already_registered_id_never_marks_it_opted_out() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DuplicateRejectingRepositionableSubscriptionModel delegate = new DuplicateRejectingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        // A real, existing, checkpoint-managed subscription for this id, nothing to do with opting out.
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        });

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        })).isInstanceOf(RuntimeException.class);

        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.repositionedTo)
                .as("a duplicate opt-out subscribe against an id the wrapped model already has registered must "
                        + "never mark that id opted out, or its real, checkpoint-managed subscription stops being "
                        + "repositioned from storage")
                .isInstanceOf(StartAt.StartAtCheckpoint.class);
    }

    @Test
    void a_resume_blocks_behind_an_in_flight_subscribe_and_then_sees_its_settled_marker() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingRepositionableSubscriptionModel delegate = new PausingRepositionableSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        // Left behind by an earlier checkpoint-managed subscription with the same id, and must never reposition
        // the opted-out subscribe call below, whichever of the two calls the lock lets through first.
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            Future<Subscription> resumeFuture = pool.submit(() -> model.resumeSubscription(SUBSCRIPTION_ID));

            assertThatThrownBy(() -> resumeFuture.get(200, TimeUnit.MILLISECONDS))
                    .as("resumeSubscription must wait for this id's lock rather than deciding from a state the "
                            + "in-flight subscribe call could still change")
                    .isInstanceOf(TimeoutException.class);

            releaseSubscribe.countDown();
            subscribeFuture.get(10, TimeUnit.SECONDS);
            resumeFuture.get(10, TimeUnit.SECONDS);

            assertThat(delegate.plainResumeCalled)
                    .as("once the subscribe above has settled, resume must see the marker it left behind")
                    .isTrue();
            assertThat(delegate.repositionedTo)
                    .as("a subscription that opted out must never be repositioned from a stored checkpoint")
                    .isNull();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_cancel_blocks_behind_an_in_flight_subscribe_and_then_removes_its_settled_marker() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingRepositionableSubscriptionModel delegate = new PausingRepositionableSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            Future<?> cancelFuture = pool.submit(() -> model.cancelSubscription(SUBSCRIPTION_ID));

            assertThatThrownBy(() -> cancelFuture.get(200, TimeUnit.MILLISECONDS))
                    .as("cancelSubscription must wait for this id's lock rather than racing the still in-flight subscribe")
                    .isInstanceOf(TimeoutException.class);

            releaseSubscribe.countDown();
            subscribeFuture.get(10, TimeUnit.SECONDS);
            cancelFuture.get(10, TimeUnit.SECONDS);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("the cancel that waited out the subscribe must have removed its marker, so a later, "
                            + "checkpoint-managed subscription for this id is not left looking opted out")
                    .isInstanceOf(StartAt.StartAtCheckpoint.class);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_subscribe_blocks_behind_an_in_flight_cancel_and_then_registers_the_freed_id() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        UnregisteringThenPausingCancelRepositionableSubscriptionModel delegate = new UnregisteringThenPausingCancelRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();

        CountDownLatch unregisteredAndPausing = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        delegate.unregisteredAndPausing = unregisteredAndPausing;
        delegate.holdReturnUntil = releaseCancel;

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> cancelFuture = pool.submit(() -> model.cancelSubscription(SUBSCRIPTION_ID));
            assertThat(unregisteredAndPausing.await(10, TimeUnit.SECONDS)).isTrue();

            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));

            assertThatThrownBy(() -> subscribeFuture.get(200, TimeUnit.MILLISECONDS))
                    .as("a fresh subscribe for this id must wait for the in-flight cancel's lock rather than "
                            + "racing the delegate transition it is still making")
                    .isInstanceOf(TimeoutException.class);

            releaseCancel.countDown();
            cancelFuture.get(10, TimeUnit.SECONDS);
            subscribeFuture.get(10, TimeUnit.SECONDS);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("the fresh subscribe that waited out the cancel must have its own marker, not be treated "
                            + "as checkpoint managed")
                    .isNull();
            assertThat(delegate.plainResumeCalled).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_checkpoint_managed_subscribe_blocks_behind_an_in_flight_cancel_too_and_then_writes_its_checkpoint_cleanly() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        UnregisteringThenPausingCancelRepositionableSubscriptionModel delegate = new UnregisteringThenPausingCancelRepositionableSubscriptionModel();
        delegate.globalCheckpoint = new StringBasedCheckpoint("global-checkpoint");
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
        }).waitUntilStarted();

        CountDownLatch unregisteredAndPausing = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        delegate.unregisteredAndPausing = unregisteredAndPausing;
        delegate.holdReturnUntil = releaseCancel;

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> cancelFuture = pool.submit(() -> model.cancelSubscription(SUBSCRIPTION_ID));
            assertThat(unregisteredAndPausing.await(10, TimeUnit.SECONDS)).isTrue();

            // A checkpoint-managed subscribe (not opt-out) reusing this id, racing the same in-flight cancel. Its
            // own generateStartAtPositionFrom writes this id's first checkpoint, which a concurrent cancel's
            // delete for the same id must not race.
            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> {
            }));

            assertThatThrownBy(() -> subscribeFuture.get(200, TimeUnit.MILLISECONDS))
                    .as("a checkpoint-managed subscribe for this id must wait for the in-flight cancel's lock too, "
                            + "not only an opt-out one")
                    .isInstanceOf(TimeoutException.class);

            releaseCancel.countDown();
            cancelFuture.get(10, TimeUnit.SECONDS);
            subscribeFuture.get(10, TimeUnit.SECONDS);

            assertThat(storage.read(SUBSCRIPTION_ID))
                    .as("the checkpoint-managed subscribe's own checkpoint write must survive, since the cancel it "
                            + "waited out had already deleted whatever was there before this subscribe could write")
                    .isEqualTo(delegate.globalCheckpoint);

            model.resumeSubscription(SUBSCRIPTION_ID);
            assertThat(delegate.repositionedTo).isInstanceOf(StartAt.StartAtCheckpoint.class);
            assertThat(((StartAt.StartAtCheckpoint) delegate.repositionedTo).checkpoint)
                    .as("the freshly written checkpoint must still be there for a later resume to reposition from")
                    .isEqualTo(delegate.globalCheckpoint);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Records whether {@link #resumeSubscription(String)} was called, and answers something for every other
     * {@link CheckpointAwareSubscriptionModel} member, since {@link DurableSubscriptionModel} requires a whole one
     * to wrap even though these tests only exercise its resume path. Tracks {@code subscribe}/{@code
     * cancelSubscription} in {@code registeredIds} for real, since some fakes below need to answer duplicate or
     * cancel questions honestly rather than by a fixed stub.
     */
    private static class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        boolean plainResumeCalled = false;
        @Nullable Checkpoint globalCheckpoint = null;
        private final Set<String> registeredIds = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            registeredIds.add(subscriptionId);
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return registeredIds.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return registeredIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            plainResumeCalled = true;
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            registeredIds.remove(subscriptionId);
        }

        static Subscription dummySubscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }

    /**
     * The same fake, additionally repositionable, recording what it was asked to reposition to.
     */
    private static class RecordingRepositionableSubscriptionModel extends RecordingSubscriptionModel implements RepositionableSubscriptions {
        @Nullable StartAt repositionedTo = null;

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            repositionedTo = startAt;
            return dummySubscription(subscriptionId);
        }
    }

    /**
     * The same repositionable fake, but {@code subscribe} throws instead of returning a {@link Subscription},
     * standing in for a delegate that rejects a subscription outright.
     */
    private static class ThrowingOnSubscribeRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new RuntimeException("delegate refused the subscription");
        }
    }

    /**
     * The same repositionable fake, but {@code subscribe} throws an {@link Error} rather than a
     * {@link RuntimeException}, standing in for something like an {@code OutOfMemoryError} escaping the delegate.
     */
    private static class ErrorThrowingOnSubscribeRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throw new Error("delegate crashed");
        }
    }

    /**
     * The same repositionable fake, but a second {@code subscribe} call for an id it already has throws, standing
     * in for a delegate refusing a duplicate subscription id such as {@code DuplicateSubscriptionIdException}.
     */
    private static class DuplicateRejectingRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (!subscribed.add(subscriptionId)) {
                throw new RuntimeException("duplicate subscription id " + subscriptionId);
            }
            return super.subscribe(subscriptionId, filter, startAt, action);
        }
    }

    /**
     * The same repositionable fake, but {@code subscribe} signals {@code subscribeEntered} and then blocks on
     * {@code holdSubscribeUntil}, standing in for a delegate call slow enough for a concurrent
     * {@code resumeSubscription} or {@code cancelSubscription} for the same id to have to wait for it.
     */
    private static class PausingRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Nullable CountDownLatch subscribeEntered;
        @Nullable CountDownLatch holdSubscribeUntil;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (subscribeEntered != null) {
                subscribeEntered.countDown();
            }
            if (holdSubscribeUntil != null) {
                try {
                    assertThat(holdSubscribeUntil.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.subscribe(subscriptionId, filter, startAt, action);
        }
    }

    /**
     * The same repositionable fake, but {@code cancelSubscription} unregisters the id first, then signals
     * {@code unregisteredAndPausing} and blocks on {@code holdReturnUntil} before returning, standing in for a
     * delegate call slow enough for a concurrent {@code subscribe} for the same id to have to wait for it.
     */
    private static class UnregisteringThenPausingCancelRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Nullable CountDownLatch unregisteredAndPausing;
        @Nullable CountDownLatch holdReturnUntil;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            // A real delegate resolves a dynamic StartAt's supplier while subscribing, which is what actually
            // performs a first checkpoint write for the "default" case. This fake mirrors that instead of leaving
            // the supplier uninvoked, the way a dumb stub would.
            startAt.get(new StartAt.SubscriptionModelContext(RecordingSubscriptionModel.class));
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            super.cancelSubscription(subscriptionId);
            if (unregisteredAndPausing != null) {
                unregisteredAndPausing.countDown();
            }
            if (holdReturnUntil != null) {
                try {
                    assertThat(holdReturnUntil.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
