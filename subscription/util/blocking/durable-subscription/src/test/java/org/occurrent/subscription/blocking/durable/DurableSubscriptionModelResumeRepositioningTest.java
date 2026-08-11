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
}
