/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.function.Consumer;

/**
 * Common interface for blocking subscription models. The purpose of a subscription is to read events from an event store
 * and react to these events.
 * <p>
 * A subscription may be used to create read models (such as views, projections, sagas, snapshots etc) or
 * forward the event to another piece of infrastructure such as a message bus or other eventing infrastructure.
 * <p>
 * A blocking subscription model also you to create and manage subscriptions that'll use blocking IO.
 */
@NullMarked
public interface SubscriptionModel extends Subscribable, SubscriptionModelLifeCycle {

    /**
     * Subscribes as {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)} does, but holds the subscription
     * paused whether or not this model is running, as a stopped model holds a subscription made while it is stopped.
     * It starts from the position such a subscription starts from, and delivers nothing until
     * {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)} resumes it.
     * <p>
     * The default implementation throws {@link UnsupportedOperationException}. Subscribing when {@link #isRunning()}
     * returns {@code false} would not hold the subscription paused if another thread starts this model in between, and
     * pausing a subscription after subscribing it could deliver an event before the pause.
     *
     * @throws UnsupportedOperationException if this model cannot hold a new subscription paused
     */
    default Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        throw new UnsupportedOperationException(getClass().getName() + " cannot hold subscription " + subscriptionId + " paused.");
    }
}