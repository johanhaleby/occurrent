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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A wrapped model a test reads from by hand, so it decides when a read happens and what it returns, and on which
 * thread.
 */
final class QuietPositionReportingModel implements CheckpointAwareSubscriptionModel, QuietPositionReportingSubscriptions {
    final List<QuietPositionListener> listeners = new CopyOnWriteArrayList<>();
    final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
    final Map<String, StartAt> startAts = new ConcurrentHashMap<>();
    // Whether subscribe evaluates the StartAt it got before it returns, as a model that opens its feed there does
    volatile boolean evaluatesStartAtInSubscribe;

    // Evaluates the StartAt the latest subscribe of the id got, as a model does each time it opens its feed
    @Nullable StartAt evaluateStartAt(String subscriptionId) {
        return startAts.get(subscriptionId).get(new StartAt.SubscriptionModelContext(QuietPositionReportingModel.class));
    }

    // A read that returned no event. True when a listener wanted the quiet position
    boolean readNothing(String subscriptionId, Checkpoint quietPosition) {
        Consumer<Checkpoint> saver = beforeReading(subscriptionId);
        if (saver == null) {
            return false;
        }
        saver.accept(quietPosition);
        return true;
    }

    @Nullable Consumer<Checkpoint> beforeReading(String subscriptionId) {
        return listeners.isEmpty() ? null : listeners.getFirst().beforeReading(subscriptionId);
    }

    void deliver(String subscriptionId, Checkpoint position) {
        CloudEvent cloudEvent = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("type").build();
        actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(cloudEvent, position));
    }

    @Override
    public void addQuietPositionListener(QuietPositionListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeQuietPositionListener(QuietPositionListener listener) {
        listeners.remove(listener);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        actions.put(subscriptionId, action);
        startAts.put(subscriptionId, startAt);
        if (evaluatesStartAtInSubscribe) {
            evaluateStartAt(subscriptionId);
        }
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

    // What globalCheckpoint() answers
    volatile @Nullable Checkpoint globalCheckpoint;

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        return globalCheckpoint;
    }

    @Override
    public void shutdown() {
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
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        actions.remove(subscriptionId);
    }
}
