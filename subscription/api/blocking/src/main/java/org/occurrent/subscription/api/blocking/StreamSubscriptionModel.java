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
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StartAt;

import java.util.function.Consumer;

/**
 * A typed view of a {@link SubscriptionModel} for stream subscriptions. It accepts an Occurrent {@link Filter} (not a
 * DCB query) and the standard {@link StartAt}. It is a facade over the shared subscription model: obtain one with
 * {@link #from(SubscriptionModel)} and the typed calls are translated to the underlying model. The start position stays
 * the generic {@link StartAt} here. The DCB-only start type {@link org.occurrent.subscription.DcbStartAt} is paired with
 * {@link DcbSubscriptionModel}, which is where the start position is also constrained to DCB.
 */
@NullMarked
public interface StreamSubscriptionModel extends SubscriptionModelLifeCycle {

    /**
     * Subscribe to events matching {@code filter}, starting at {@code startAt}.
     */
    Subscription subscribe(String subscriptionId, Filter filter, StartAt startAt, Consumer<CloudEvent> action);

    /**
     * Subscribe to events matching {@code filter} at the subscription model default position.
     */
    default Subscription subscribe(String subscriptionId, Filter filter, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
    }

    /**
     * Subscribe to every event at the subscription model default position.
     */
    default Subscription subscribe(String subscriptionId, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, Filter.all(), action);
    }

    /**
     * Create a stream view over an existing {@link SubscriptionModel}. The returned model wraps the {@link Filter} in an
     * {@code OccurrentSubscriptionFilter} for the delegate, and forwards all life-cycle calls to it.
     */
    static StreamSubscriptionModel from(SubscriptionModel delegate) {
        return new StreamSubscriptionModelAdapter(delegate);
    }
}
