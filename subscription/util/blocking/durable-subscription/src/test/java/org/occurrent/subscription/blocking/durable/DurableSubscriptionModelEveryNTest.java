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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.util.predicate.EveryN;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link EveryN} counts the events of each subscription on its own, even when one instance is configured for every
 * subscription a {@link DurableSubscriptionModel} runs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelEveryNTest {

    @Test
    void every_second_event_of_each_subscription_is_checkpointed_when_two_subscriptions_take_turns() {
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ManuallyDeliveringSubscriptionModel wrapped = new ManuallyDeliveringSubscriptionModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(EveryN.every(2)));
        model.subscribe("A", null, StartAt.now(), __ -> {
        });
        model.subscribe("B", null, StartAt.now(), __ -> {
        });

        for (int i = 1; i <= 4; i++) {
            wrapped.deliver("A", "a" + i);
            wrapped.deliver("B", "b" + i);
        }

        assertThat(storage.read("A")).as("A saved after its 2nd and 4th event").extracting(Checkpoint::asString).isEqualTo("a4");
        assertThat(storage.read("B")).as("B saved after its 2nd and 4th event").extracting(Checkpoint::asString).isEqualTo("b4");
    }

    /**
     * Keeps the action of every subscription and hands it one event per call to {@link #deliver(String, String)}, with
     * the given checkpoint.
     */
    private static final class ManuallyDeliveringSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();

        void deliver(String subscriptionId, String checkpoint) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId(checkpoint)
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType("Created")
                    .build();
            actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint(checkpoint)));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
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

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global");
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
            return actions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
        }
    }
}
