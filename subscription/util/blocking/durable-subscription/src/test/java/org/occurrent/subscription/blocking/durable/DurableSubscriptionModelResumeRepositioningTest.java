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
import java.util.concurrent.TimeUnit;
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
    void a_failed_duplicate_subscribe_does_not_erase_the_marker_of_an_already_active_subscription_with_the_same_id() {
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
                .as("the second call's failure must release only its own share of the marker, not the first, "
                        + "still-active subscription's, or the first subscription's next resume gets wrongly "
                        + "repositioned from storage")
                .isNull();
        assertThat(delegate.plainResumeCalled).isTrue();
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

    /**
     * Records whether {@link #resumeSubscription(String)} was called, and answers something for every other
     * {@link CheckpointAwareSubscriptionModel} member, since {@link DurableSubscriptionModel} requires a whole one
     * to wrap even though these tests only exercise its resume path.
     */
    private static class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        boolean plainResumeCalled = false;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return true;
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
}
