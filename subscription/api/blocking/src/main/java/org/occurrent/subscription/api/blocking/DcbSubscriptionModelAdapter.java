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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link DcbSubscriptionModel} calls into the shared {@link SubscriptionModel}, building a
 * {@link DcbSubscriptionFilter} from the query and converting the {@link DcbStartAt} to a generic start position. All
 * life-cycle calls forward to the delegate (see {@link AbstractDelegatingSubscriptionModelAdapter}).
 */
@NullMarked
final class DcbSubscriptionModelAdapter extends AbstractDelegatingSubscriptionModelAdapter implements DcbSubscriptionModel {

    DcbSubscriptionModelAdapter(SubscriptionModel delegate) {
        super(delegate);
    }

    @Override
    public Subscription subscribe(String subscriptionId, DcbQuery query, DcbStartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        requireNonNull(action, "Subscription action cannot be null");
        // The DcbSubscriptionFilter is honored server-side for live delivery, but a DCB catch-up replays by the
        // model-level query, so an in-process check keeps the subscription scoped to its own query during catch-up too
        // (and stays correct for any backend that does not honor the filter).
        Consumer<CloudEvent> scopedToQuery = cloudEvent -> {
            if (DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query)) {
                action.accept(cloudEvent);
            }
        };
        return delegate.subscribe(subscriptionId, DcbSubscriptionFilter.filter(query), startAt.toStartAt(), scopedToQuery);
    }
}
