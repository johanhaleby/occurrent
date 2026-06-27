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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;

import java.util.function.Consumer;

/**
 * A typed view of a {@link SubscriptionModel} for Dynamic Consistency Boundary (DCB) subscriptions. It accepts a
 * {@link DcbQuery} and a {@link DcbStartAt}, so a DCB subscription cannot be handed a stream {@code Filter} or a
 * time-based start position. It is a facade over the shared subscription model, not a separate lifecycle: obtain one
 * with {@link #from(SubscriptionModel)} and the typed calls are translated to the underlying model.
 * <p>
 * This is the lower-level, CloudEvent-level typed view. Most application code does not use it directly: use the
 * {@code @DcbSubscription} annotation for a persistent framework-managed subscription, or the {@code DcbSubscriptions}
 * DSL for an ephemeral per-connection one.
 */
@NullMarked
public interface DcbSubscriptionModel extends SubscriptionModelLifeCycle {

    /**
     * Subscribe to DCB events matching {@code query}, starting at {@code startAt}.
     */
    Subscription subscribe(String subscriptionId, DcbQuery query, DcbStartAt startAt, Consumer<CloudEvent> action);

    /**
     * Subscribe to DCB events matching {@code query} at the subscription model default position.
     */
    default Subscription subscribe(String subscriptionId, DcbQuery query, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, query, DcbStartAt.subscriptionModelDefault(), action);
    }

    /**
     * Subscribe to every DCB event at the subscription model default position.
     */
    default Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, DcbQuery.all(), action);
    }

    /**
     * Create a DCB view over an existing {@link SubscriptionModel}. The returned model translates DCB queries and start
     * positions into the filter and start position the delegate understands, and forwards all life-cycle calls to it.
     */
    static DcbSubscriptionModel from(SubscriptionModel delegate) {
        return new DcbSubscriptionModelAdapter(delegate);
    }
}
