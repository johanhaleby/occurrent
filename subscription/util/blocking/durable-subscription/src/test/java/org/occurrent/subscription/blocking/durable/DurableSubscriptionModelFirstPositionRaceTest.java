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
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code recordFirstPositionOrRefuse} reads storage, finds nothing, then writes what the wrapped model answers for
 * {@code globalCheckpoint()}. A checkpoint that lands in storage between those two calls, from another node
 * registering the same subscription id concurrently, must not be silently overwritten by this node's write, the
 * same guarantee {@code ManualStartSubscriptionModel} and {@code ReactorDurableSubscriptionModel} already give.
 * <p>
 * A hand-rolled storage rather than MongoDB, because this is about the order two calls reach storage in, which a
 * real database hides.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRaceTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_registration_that_loses_the_first_position_write_to_a_checkpoint_it_did_not_read_is_refused_rather_than_overwriting_it() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        // Another node's registration for the same subscription id wins the write in between this node's read
        // (which finds nothing) and its own write.
        storage.whenTheFirstReadFindsNothing = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("landed-during-registration"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .as("the position stored is not the one this registration read, so starting from it would skip whatever the other node's own subscription is about to see")
                .isInstanceOf(StartPositionAlreadyPinnedException.class);

        assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString())
                .as("the other node's checkpoint must survive this node's losing write untouched")
                .isEqualTo("landed-during-registration");
    }

    private static CloudEvent cloudEvent(String id) {
        return io.cloudevents.core.builder.CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").build();
    }

    /**
     * Delegates every call to a real {@link InMemoryCheckpointStorage}, except that the first {@link #read} for a
     * subscription id runs {@link #whenTheFirstReadFindsNothing} right after finding nothing, before this node's
     * own write reaches storage. That reproduces the interleaving a real race needs without a second thread.
     */
    private static final class RaceSimulatingCheckpointStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        @Nullable Runnable whenTheFirstReadFindsNothing;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint found = delegate.read(subscriptionId);
            if (found == null && whenTheFirstReadFindsNothing != null) {
                Runnable hook = whenTheFirstReadFindsNothing;
                whenTheFirstReadFindsNothing = null;
                hook.run();
            }
            return found;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, org.occurrent.subscription.CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public java.util.OptionalLong writeVersion(String subscriptionId) {
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

    /**
     * A feed with change-stream mechanics reduced to what this test needs: the model default means the end of what
     * has been published so far, and a subscription is registered but never delivered to.
     */
    private static final class InMemoryFeed implements CheckpointAwareSubscriptionModel {
        final Map<String, Boolean> subscriptions = new LinkedHashMap<>();
        boolean answersCurrentPosition = false;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.put(subscriptionId, true);
            return dummySubscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return answersCurrentPosition ? new StringBasedCheckpoint("0") : null;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
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
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static Subscription dummySubscription(String subscriptionId) {
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
}
