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

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;

import static java.util.Objects.requireNonNull;

/**
 * Drives stored events out of the event store into a {@link CloudEventSink}, one durable subscription at a time.
 * Takes a {@link DurableSubscriptionModel} rather than a bare subscription model so the checkpoint-after-success
 * guarantee is part of the type. {@code DurableSubscriptionModel} already persists the checkpoint only once its
 * action returns, and the action here is a call to the sink, so a sink that throws leaves the checkpoint where it
 * was and the event is published again on the next run. That makes publication at least once, which is the
 * guarantee to document rather than to try to improve.
 */
public class CloudEventForwarder {

    private final DurableSubscriptionModel subscriptionModel;
    private final CloudEventSink sink;

    public CloudEventForwarder(DurableSubscriptionModel subscriptionModel, CloudEventSink sink) {
        this.subscriptionModel = requireNonNull(subscriptionModel, DurableSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.sink = requireNonNull(sink, CloudEventSink.class.getSimpleName() + " cannot be null");
    }

    /**
     * Start forwarding at the subscription model's default start position, with no filter.
     */
    public Subscription forward(String subscriptionId) {
        return subscriptionModel.subscribe(subscriptionId, sink::publish);
    }

    /**
     * Start forwarding at {@code startAt}, with no filter.
     */
    public Subscription forward(String subscriptionId, StartAt startAt) {
        return subscriptionModel.subscribe(subscriptionId, startAt, sink::publish);
    }

    /**
     * Start forwarding only events matching {@code filter}, at the subscription model's default start position.
     */
    public Subscription forward(String subscriptionId, @Nullable SubscriptionFilter filter) {
        return subscriptionModel.subscribe(subscriptionId, filter, sink::publish);
    }

    /**
     * Start forwarding only events matching {@code filter}, at {@code startAt}.
     */
    public Subscription forward(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt) {
        return subscriptionModel.subscribe(subscriptionId, filter, startAt, sink::publish);
    }
}
