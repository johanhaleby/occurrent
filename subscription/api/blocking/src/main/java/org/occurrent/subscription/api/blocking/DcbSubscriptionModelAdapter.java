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
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link DcbSubscriptionModel} calls into the shared {@link SubscriptionModel}, building a
 * {@link DcbSubscriptionFilter} from the criteria and converting the {@link DcbStartAt} to a generic start position. All
 * life-cycle calls forward to the delegate (see {@link AbstractSubscriptionModelAdapter}).
 */
@NullMarked
final class DcbSubscriptionModelAdapter extends AbstractSubscriptionModelAdapter implements DcbSubscriptionModel {

    DcbSubscriptionModelAdapter(SubscriptionModel delegate) {
        super(delegate);
    }

    @Override
    public Subscription subscribe(String subscriptionId, DcbCriteria criteria, DcbStartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        requireNonNull(action, "Subscription action cannot be null");
        // The DcbSubscriptionFilter is honored server-side for live delivery, but a DCB catch-up replays by the
        // model-level criteria, so an in-process check keeps the subscription scoped to its own criteria during catch-up too
        // (and stays correct for any backend that does not honor the filter).
        Consumer<CloudEvent> scopedToCriteria = cloudEvent -> {
            // Scope to DCB-written events matching the criteria. The discriminator is isDcbEvent (the DCB tags extension),
            // not a positive position: with stream position on by default, stream events also carry a position, so a
            // "position > 0" guard would leak stream events into a DCB subscription.
            if (DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, criteria)) {
                action.accept(cloudEvent);
            }
        };
        return delegate.subscribe(subscriptionId, DcbSubscriptionFilter.filter(criteria), startAt.toStartAt(), scopedToCriteria);
    }
}
