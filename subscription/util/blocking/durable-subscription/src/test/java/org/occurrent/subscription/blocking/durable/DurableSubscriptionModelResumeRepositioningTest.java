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
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DurableSubscriptionModel#resumeSubscription(String)} against a recording fake rather than a real Mongo
 * model, so each of the four decisions it makes (reposition from the checkpoint, fall back for no checkpoint, fall
 * back for a non-repositionable delegate, and the opt-out guard) is isolated from a change stream and provable on
 * its own. {@link CompetingConsumerLeaseRegainResumesFromCheckpointTest} proves the same mechanism end to end over
 * real Mongo.
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
    void a_truly_concurrent_duplicate_subscribe_only_releases_its_own_share_of_a_shared_counter() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ConcurrentDuplicateRepositionableSubscriptionModel delegate = new ConcurrentDuplicateRepositionableSubscriptionModel();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        delegate.firstCallEntered = firstEntered;
        delegate.holdFirstCallUntil = releaseFirst;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> firstAttempt = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(firstEntered.await(10, TimeUnit.SECONDS)).isTrue();

            // The second attempt reaches and is accepted by the delegate while the first is still paused inside
            // its own delegate call, so both attempts' increments land on the one counter they share before either
            // is settled, neither having taken the early-return branch since neither is known to the delegate yet.
            model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }).waitUntilStarted();

            releaseFirst.countDown();
            assertThatThrownBy(() -> firstAttempt.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(RuntimeException.class);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("the first attempt's failure must release only its own share of the counter both attempts "
                            + "shared, not the second attempt's share, which is the one the delegate accepted")
                    .isNull();
            assertThat(delegate.plainResumeCalled).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_resume_racing_a_still_running_subscribe_sees_the_opt_out_marker_before_the_delegate_returns() throws InterruptedException {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingRepositionableSubscriptionModel delegate = new PausingRepositionableSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        // Left behind by an earlier checkpoint-managed subscription with the same id, and must never reposition
        // the opted-out subscribe call below, whether resumeSubscription lands before or after it returns.
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.execute(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.plainResumeCalled)
                    .as("the marker must already be visible while the delegate's own subscribe call is still in "
                            + "flight, so resumeSubscription forwards to the delegate's own resume rather than "
                            + "treating this id as checkpoint managed")
                    .isTrue();
            assertThat(delegate.repositionedTo)
                    .as("a subscription that opted out must never be repositioned from a stored checkpoint, "
                            + "concurrent resume or not")
                    .isNull();
        } finally {
            releaseSubscribe.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void a_cancel_racing_a_still_running_subscribe_does_not_erase_the_marker_once_that_subscribe_succeeds() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingRepositionableSubscriptionModel delegate = new PausingRepositionableSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            // The delegate here does not yet know this id, so this cancel is a no-op on it, but it must not erase
            // the opt-out marker that the still in-flight subscribe above is entitled to once it succeeds.
            model.cancelSubscription(SUBSCRIPTION_ID);

            releaseSubscribe.countDown();
            subscribeFuture.get(10, TimeUnit.SECONDS);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("a subscribe call that finished after a concurrent cancel must still have its opt-out "
                            + "marker, or its next resume gets wrongly repositioned from storage")
                    .isNull();
            assertThat(delegate.plainResumeCalled).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_stale_attempts_failure_does_not_touch_a_later_generations_counter_for_the_same_id() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingThenThrowingOnFirstSubscribeRepositionableSubscriptionModel delegate = new PausingThenThrowingOnFirstSubscribeRepositionableSubscriptionModel();
        CountDownLatch insideSubscribe = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.subscribeEntered = insideSubscribe;
        delegate.holdSubscribeUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> staleAttempt = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(insideSubscribe.await(10, TimeUnit.SECONDS)).isTrue();

            // Cancelling the stale attempt's marker while it is still in flight, then a fresh subscribe for the
            // same id installs a genuinely different counter object, the same id but a later generation.
            model.cancelSubscription(SUBSCRIPTION_ID);
            model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }).waitUntilStarted();

            releaseSubscribe.countDown();
            assertThatThrownBy(() -> staleAttempt.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(RuntimeException.class);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("the stale attempt's failure must release only its own generation's share, never the later "
                            + "generation's counter that a fresh subscribe for the same id installed in between")
                    .isNull();
            assertThat(delegate.plainResumeCalled).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_duplicate_opt_out_subscribe_against_an_already_registered_id_never_marks_it_opted_out() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        PausingBeforeRejectingDuplicateRepositionableSubscriptionModel delegate = new PausingBeforeRejectingDuplicateRepositionableSubscriptionModel();
        CountDownLatch aboutToReject = new CountDownLatch(1);
        CountDownLatch releaseRejection = new CountDownLatch(1);
        delegate.aboutToReject = aboutToReject;
        delegate.holdRejectionUntil = releaseRejection;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        // A real, existing, checkpoint-managed subscription for this id, nothing to do with opting out.
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), event -> {
        });
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> duplicateAttempt = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(aboutToReject.await(10, TimeUnit.SECONDS)).isTrue();

            // The duplicate opt-out attempt above is paused right before the delegate rejects it. A concurrent
            // resume for the id's real, checkpoint-managed subscription must not be fooled by it in that window.
            model.resumeSubscription(SUBSCRIPTION_ID);

            releaseRejection.countDown();
            assertThatThrownBy(() -> duplicateAttempt.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(RuntimeException.class);

            assertThat(delegate.repositionedTo)
                    .as("a duplicate opt-out subscribe against an id the wrapped model already has registered must "
                            + "never mark that id opted out, even while the duplicate attempt is still in flight, "
                            + "or its real, checkpoint-managed subscription stops being repositioned from storage")
                    .isInstanceOf(StartAt.StartAtCheckpoint.class);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_cancel_after_the_delegate_has_already_accepted_the_subscription_leaves_no_marker_to_reinstate() throws Exception {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RegisteringThenPausingRepositionableSubscriptionModel delegate = new RegisteringThenPausingRepositionableSubscriptionModel();
        CountDownLatch registered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        delegate.registeredAndPausing = registered;
        delegate.holdReturnUntil = releaseSubscribe;
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> subscribeFuture = pool.submit(() -> model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }));
            assertThat(registered.await(10, TimeUnit.SECONDS)).isTrue();

            // The delegate has already accepted this registration by this point, so this cancel is real, not a
            // no-op, and it must be the one that wins once the subscribe call above returns.
            model.cancelSubscription(SUBSCRIPTION_ID);

            releaseSubscribe.countDown();
            subscribeFuture.get(10, TimeUnit.SECONDS);

            storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));
            model.resumeSubscription(SUBSCRIPTION_ID);

            assertThat(delegate.repositionedTo)
                    .as("a cancel that lands after the delegate already accepted the subscription must win, so a "
                            + "later, checkpoint-managed subscription for the same id is not left with a stale "
                            + "opt-out marker and skips repositioning")
                    .isInstanceOf(StartAt.StartAtCheckpoint.class);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void a_resume_that_already_read_the_checkpoint_does_not_reposition_once_a_concurrent_opt_out_subscribe_is_accepted() throws Exception {
        PausingOnReadCheckpointStorage storage = new PausingOnReadCheckpointStorage();
        CountDownLatch readingCheckpoint = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        storage.aboutToReturn = readingCheckpoint;
        storage.holdReturnUntil = releaseRead;
        RecordingRepositionableSubscriptionModel delegate = new RecordingRepositionableSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);
        StartAt optOut = StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(DurableSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault());
        // Left behind by an earlier, checkpoint-managed subscription with the same id, now cancelled and never
        // cleaned up in this fake, standing in for the delegate reporting the id as no longer known.
        storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stored-checkpoint"));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Subscription> resumeFuture = pool.submit(() -> model.resumeSubscription(SUBSCRIPTION_ID));
            assertThat(readingCheckpoint.await(10, TimeUnit.SECONDS)).isTrue();

            // The opt-out subscribe below is accepted while the resume above is still reading the checkpoint it
            // will otherwise reposition from.
            model.subscribe(SUBSCRIPTION_ID, null, optOut, event -> {
            }).waitUntilStarted();

            releaseRead.countDown();
            resumeFuture.get(10, TimeUnit.SECONDS);

            assertThat(delegate.repositionedTo)
                    .as("a resume must not act on a reposition decision it made before the checkpoint read "
                            + "returned, once a concurrent opt-out subscribe was accepted while it was reading")
                    .isNull();
            assertThat(delegate.plainResumeCalled).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Records whether {@link #resumeSubscription(String)} was called, and answers something for every other
     * {@link CheckpointAwareSubscriptionModel} member, since {@link DurableSubscriptionModel} requires a whole one
     * to wrap even though these tests only exercise its resume path. Tracks {@code subscribe}/{@code
     * cancelSubscription} in {@code registeredIds} for real, since {@link DurableSubscriptionModel} now asks
     * {@code isRunning}/{@code isPaused} to decide whether an id is already known before counting an opt-out
     * attempt in, so a fake that answered those unconditionally would defeat that check for every id, known or not.
     */
    private static class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        boolean plainResumeCalled = false;
        private final Set<String> registeredIds = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            registeredIds.add(subscriptionId);
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
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
     * The same repositionable fake, but the first {@code subscribe} call signals {@code firstCallEntered} and
     * blocks on {@code holdFirstCallUntil} before checking for a duplicate, standing in for two attempts racing the
     * delegate for the same id before either is known to it, so both land on the one counter this model's own
     * preflight check has already made them share.
     */
    private static class ConcurrentDuplicateRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        private final AtomicInteger callCount = new AtomicInteger();
        @Nullable CountDownLatch firstCallEntered;
        @Nullable CountDownLatch holdFirstCallUntil;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (callCount.incrementAndGet() == 1) {
                if (firstCallEntered != null) {
                    firstCallEntered.countDown();
                }
                if (holdFirstCallUntil != null) {
                    try {
                        assertThat(holdFirstCallUntil.await(10, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            if (!subscribed.add(subscriptionId)) {
                throw new RuntimeException("duplicate subscription id " + subscriptionId);
            }
            return super.subscribe(subscriptionId, filter, startAt, action);
        }
    }

    /**
     * The same duplicate-rejecting fake, but a rejection signals {@code aboutToReject} and blocks on
     * {@code holdRejectionUntil} before throwing, standing in for a delegate whose duplicate-id check takes long
     * enough for a concurrent resume to land while the rejection is still in flight.
     */
    private static class PausingBeforeRejectingDuplicateRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
        @Nullable CountDownLatch aboutToReject;
        @Nullable CountDownLatch holdRejectionUntil;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (subscribed.add(subscriptionId)) {
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
            if (aboutToReject != null) {
                aboutToReject.countDown();
            }
            if (holdRejectionUntil != null) {
                try {
                    assertThat(holdRejectionUntil.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            throw new RuntimeException("duplicate subscription id " + subscriptionId);
        }
    }

    /**
     * The same repositionable fake, but {@code subscribe} signals {@code subscribeEntered} and then blocks on
     * {@code holdSubscribeUntil}, standing in for a delegate whose subscribe call is still in flight when a
     * concurrent {@code resumeSubscription} lands.
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
     * The same repositionable fake, but {@code subscribe} registers the id first, then signals
     * {@code registeredAndPausing} and blocks on {@code holdReturnUntil} before returning, standing in for a
     * delegate that has already accepted a registration but has not yet handed the {@link Subscription} back to
     * the caller.
     */
    private static class RegisteringThenPausingRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Nullable CountDownLatch registeredAndPausing;
        @Nullable CountDownLatch holdReturnUntil;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
            if (registeredAndPausing != null) {
                registeredAndPausing.countDown();
            }
            if (holdReturnUntil != null) {
                try {
                    assertThat(holdReturnUntil.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return subscription;
        }
    }

    /**
     * The same repositionable fake, but only the first {@code subscribe} call pauses on
     * {@code subscribeEntered}/{@code holdSubscribeUntil} and then throws once released, standing in for a stale
     * attempt that fails after being unblocked. Every later call for any id succeeds immediately, standing in for
     * a fresh subscribe landing while the stale one is still in flight.
     */
    private static class PausingThenThrowingOnFirstSubscribeRepositionableSubscriptionModel extends RecordingRepositionableSubscriptionModel {
        @Nullable CountDownLatch subscribeEntered;
        @Nullable CountDownLatch holdSubscribeUntil;
        private final AtomicInteger callCount = new AtomicInteger();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (callCount.incrementAndGet() != 1) {
                return super.subscribe(subscriptionId, filter, startAt, action);
            }
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
            throw new RuntimeException("stale attempt failed after being unblocked");
        }
    }

    /**
     * Wraps a real {@link InMemoryCheckpointStorage}, but {@code read} signals {@code aboutToReturn} and blocks on
     * {@code holdReturnUntil} before returning, standing in for a checkpoint read slow enough for a concurrent
     * opt-out subscribe to be accepted while it is still in flight.
     */
    private static class PausingOnReadCheckpointStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        @Nullable CountDownLatch aboutToReturn;
        @Nullable CountDownLatch holdReturnUntil;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint checkpoint = delegate.read(subscriptionId);
            if (aboutToReturn != null) {
                aboutToReturn.countDown();
            }
            if (holdReturnUntil != null) {
                try {
                    assertThat(holdReturnUntil.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return checkpoint;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }
    }
}
