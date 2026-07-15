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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.SynchronousEventDispatcher;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A register-only subscription model whose handlers are invoked <strong>synchronously</strong>, in-process, on
 * the thread that supplies the events, rather than asynchronously off a change stream.
 * <p>
 * It exists to be driven by the application service: after a successful write, the application service hands
 * the just-written cloud events to {@link #accept(List)}, which routes each event to the registered handlers
 * whose {@link SubscriptionFilter} matches, invoking them in registration order on the calling thread. A
 * handler exception propagates to the caller (so, under a transaction, it rolls the write back).
 * <p>
 * Unlike the asynchronous {@code SubscriptionModel}s, this model has no lifecycle, start position, checkpoint,
 * catch-up, or replay: it only ever reacts to events fed to it here and now. It therefore implements only
 * {@link Subscribable}, not the full {@code SubscriptionModelLifeCycle}. {@link StartAt} is accepted for
 * interface compatibility with the subscription DSLs but ignored, since "where to start" is meaningless for
 * synchronous, at-write-time dispatch.
 */
@NullMarked
public class SynchronousSubscriptionModel implements Subscribable, SynchronousEventDispatcher, Consumer<List<CloudEvent>> {

    private record Registration(String id, Predicate<CloudEvent> matcher, Consumer<CloudEvent> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!subscriptionIds.add(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already registered");
        }
        registrations.add(new Registration(subscriptionId, SubscriptionFilterMatcher.matcherFor(filter), action));
        return new SynchronousSubscription(subscriptionId);
    }

    /**
     * Dispatch the supplied cloud events to every matching registered handler, synchronously, on the calling
     * thread, in registration order. Called by the application service with the events it just wrote.
     *
     * @param writtenCloudEvents The newly written cloud events.
     */
    @Override
    public void dispatch(List<CloudEvent> writtenCloudEvents) {
        Objects.requireNonNull(writtenCloudEvents, "writtenCloudEvents cannot be null");
        for (CloudEvent cloudEvent : writtenCloudEvents) {
            for (Registration registration : registrations) {
                if (registration.matcher().test(cloudEvent)) {
                    registration.action().accept(cloudEvent);
                }
            }
        }
    }

    /**
     * Alias for {@link #dispatch(List)} so the model can also be used directly as a
     * {@code Consumer<List<CloudEvent>>} listener (for example as an in-memory event-store write listener).
     */
    @Override
    public void accept(List<CloudEvent> cloudEvents) {
        dispatch(cloudEvents);
    }

    @Override
    public boolean hasSubscriptions() {
        return !registrations.isEmpty();
    }

    private record SynchronousSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            // Synchronous subscriptions are always "started": there is no background thread to wait for.
            return true;
        }
    }
}
