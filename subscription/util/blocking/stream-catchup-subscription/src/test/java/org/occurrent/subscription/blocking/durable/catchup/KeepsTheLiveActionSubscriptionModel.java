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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A live delegate that keeps the action it was given, also once the subscription is cancelled, as a delegate that has a
 * call under way does, and calls it when told to, recording what the call threw. Public, since a test in another package
 * subclasses a catch-up model over it.
 */
public final class KeepsTheLiveActionSubscriptionModel implements CheckpointAwareSubscriptionModel {
    private final Map<String, Consumer<CloudEvent>> kept = new ConcurrentHashMap<>();
    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final List<Throwable> thrownByTheAction = new CopyOnWriteArrayList<>();
    private final AtomicReference<Runnable> onShutdown = new AtomicReference<>(() -> {
    });
    private final AtomicBoolean failTheNextCancel = new AtomicBoolean();
    private final AtomicReference<Consumer<Consumer<CloudEvent>>> onSubscribe = new AtomicReference<>(action -> {
    });

    public void whenItShutsDown(Runnable action) {
        onShutdown.set(action);
    }

    // The next cancel removes the subscription as a live delegate that held it paused does, and throws
    public void failTheNextCancel() {
        failTheNextCancel.set(true);
    }

    // Runs once the action is kept and before the subscription counts as running, with the action the subscribe was given.
    // What it throws is what the subscribe throws
    public void whenSubscribing(Consumer<Consumer<CloudEvent>> hook) {
        onSubscribe.set(hook);
    }

    public void callTheKeptAction(String subscriptionId) {
        CloudEvent event = CloudEventBuilder.v1().withId("live1").withSource(URI.create("urn:test")).withType("Tested").build();
        try {
            kept.get(subscriptionId).accept(event);
        } catch (Throwable e) {
            thrownByTheAction.add(e);
        }
    }

    public List<Throwable> thrownByTheAction() {
        return thrownByTheAction;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        kept.put(subscriptionId, action);
        onSubscribe.getAndSet(ignored -> {
        }).accept(action);
        running.add(subscriptionId);
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
        return new StringBasedCheckpoint("live start read now");
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return true;
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
        return running.contains(subscriptionId);
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
        running.remove(subscriptionId);
        if (failTheNextCancel.getAndSet(false)) {
            throw new IllegalStateException("The live delegate failed to cancel " + subscriptionId);
        }
    }

    @Override
    public void shutdown() {
        onShutdown.get().run();
    }
}
