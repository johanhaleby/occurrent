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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;

import java.time.Duration;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A single-subscription {@link CheckpointAwareSubscriptionModel} standing in for the event store's own subscription
 * channel, so a forwarder can be wrapped in a real {@link org.occurrent.subscription.blocking.durable.DurableSubscriptionModel}
 * and exercised without a broker, an event store, or an executor: {@link #publish(CloudEvent)} calls the registered
 * action synchronously, on the calling thread, so a test sees the checkpoint move (or not) before it makes its
 * assertions. {@link #globalCheckpoint()} answers {@link GlobalCheckpoint#of} {@code 0}, the position a real
 * wrapped model reports for an empty stream, so {@code DurableSubscriptionModel} records that as the first
 * position instead of refusing to start.
 */
class FakeCheckpointAwareSubscriptionModel implements CheckpointAwareSubscriptionModel {

    private @Nullable String subscriptionId;
    private @Nullable SubscriptionFilter filter;
    private StartAt startAt = StartAt.now();
    private @Nullable Consumer<CloudEvent> action;

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        this.subscriptionId = requireNonNull(subscriptionId);
        this.filter = filter;
        this.startAt = requireNonNull(startAt);
        this.action = requireNonNull(action);
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

    /**
     * Hand {@code cloudEvent} to whatever action the last {@code subscribe} call registered, on the calling thread.
     */
    void publish(CloudEvent cloudEvent) {
        requireNonNull(action, "Nothing has subscribed yet").accept(cloudEvent);
    }

    @Nullable
    String lastSubscriptionId() {
        return subscriptionId;
    }

    @Nullable
    SubscriptionFilter lastFilter() {
        return filter;
    }

    StartAt lastStartAt() {
        return startAt;
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
        return false;
    }

    @Override
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        this.action = null;
    }

    @Override
    public Checkpoint globalCheckpoint() {
        return GlobalCheckpoint.of(0);
    }
}
