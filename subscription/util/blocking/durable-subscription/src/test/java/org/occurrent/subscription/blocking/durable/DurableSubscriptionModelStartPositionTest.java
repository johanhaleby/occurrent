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
import org.occurrent.subscription.api.blocking.ManualStartSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This model reads a stored checkpoint for a subscription registered with the model default, so it is the layer
 * {@link ManualStartSubscriptionModel} records a registration's start position from. What the caller's position
 * resolves to under this class is what decides where the subscription starts, which is what
 * {@link org.occurrent.subscription.api.blocking.SubscriptionModelWrapper#decidesWhereTheSubscriptionStarts()}
 * answering {@code true} says, and answering {@code false} here would leave a first run starting from the moment it
 * is started rather than from where it was registered.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStartPositionTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_registration_naming_this_model_is_recorded_from_where_it_was_registered() {
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel();
        feed.globalCheckpoint = new StringBasedCheckpoint("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        ManualStartSubscriptionModel manualStart = ManualStartSubscriptionModel.stoppedByDefault(durable, feed, storage);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());

        manualStart.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        assertThat(storage.read(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    @Test
    void starting_a_registration_later_keeps_the_position_it_was_registered_at() {
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel();
        feed.globalCheckpoint = new StringBasedCheckpoint("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);
        ManualStartSubscriptionModel manualStart = ManualStartSubscriptionModel.stoppedByDefault(durable, feed, storage);
        StartAt startAt = StartAt.dynamic(context -> context.hasSubscriptionModelType(DurableSubscriptionModel.class)
                ? StartAt.subscriptionModelDefault() : StartAt.now());
        manualStart.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> {
        });

        // Whatever the feed has moved on to by the time the subscription is started is what would have been stored
        // without the write at registration.
        feed.globalCheckpoint = new StringBasedCheckpoint("at-start");
        manualStart.start(true);

        assertThat(storage.read(SUBSCRIPTION_ID).asString()).isEqualTo("at-registration");
    }

    // Answers something for every CheckpointAwareSubscriptionModel member, since DurableSubscriptionModel wraps a
    // whole one, and reports the position these tests hand it.
    private static final class RecordingSubscriptionModel implements CheckpointAwareSubscriptionModel {
        @Nullable Checkpoint globalCheckpoint = null;
        private final Set<String> registeredIds = ConcurrentHashMap.newKeySet();

        @Override
        public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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
            return false;
        }

        @Override
        public SubscriptionHandle resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            registeredIds.remove(subscriptionId);
        }

        private static SubscriptionHandle dummySubscription(String subscriptionId) {
            return new SubscriptionHandle() {
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
