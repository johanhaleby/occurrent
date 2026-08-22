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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A wrapped model that manages named subscriptions of its own, which is what puts the durable model on the path that
 * awaits the start position inside {@code subscribe} rather than driving the cold primitive itself. Reads the position
 * through a {@link RecordingSubscriptionModel}, so the same knobs decide what a read answers on either path.
 */
final class NamedRecordingSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {

    final List<String> subscribedIds = new CopyOnWriteArrayList<>();
    final List<StartAt> startedAt = new CopyOnWriteArrayList<>();
    final RecordingSubscriptionModel feed;
    /**
     * A stopped named model parks a registration and opens its feed when it is started, which is what makes a start
     * position of {@code now} mean "wherever the feed has reached by then" rather than "where this registered".
     */
    boolean running = true;

    NamedRecordingSubscriptionModel(String globalCheckpoint) {
        this.feed = new RecordingSubscriptionModel(globalCheckpoint);
    }

    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        return feed.subscribe(filter, startAt);
    }

    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        return feed.globalCheckpoint();
    }

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                  Function<CloudEvent, Mono<Void>> action) {
        subscribedIds.add(subscriptionId);
        // What the durable model hands a named model is the whole of what decides where the subscription begins on
        // this path, since this model resolves nothing further.
        startedAt.add(startAt);
        return new SubscriptionHandle() {
            @Override
            public String id() {
                return subscriptionId;
            }

            @Override
            public Mono<Void> waitUntilStarted() {
                return Mono.empty();
            }
        };
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
    }

    @Override
    public void stop() {
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return running && subscribedIds.contains(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return !running && subscribedIds.contains(subscriptionId);
    }

    @Override
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
    }
}
