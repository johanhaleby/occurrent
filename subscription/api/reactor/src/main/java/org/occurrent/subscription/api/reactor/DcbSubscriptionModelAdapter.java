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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link DcbSubscriptionModel} calls into the shared reactive {@link SubscriptionModel}, building a
 * {@link DcbSubscriptionFilter} from the query and converting the {@link DcbStartAt} to a generic start position.
 */
@NullMarked
final class DcbSubscriptionModelAdapter implements DcbSubscriptionModel {

    private final SubscriptionModel delegate;

    DcbSubscriptionModelAdapter(SubscriptionModel delegate) {
        this.delegate = requireNonNull(delegate, SubscriptionModel.class.getSimpleName() + " cannot be null");
    }

    @Override
    public Flux<CloudEvent> subscribe(DcbQuery query, DcbStartAt startAt) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        // The DcbSubscriptionFilter is honored server-side for live delivery. The in-process check keeps the
        // subscription scoped to its own query for any backend that does not honor the filter, matching the blocking
        // adapter.
        return delegate.subscribe(DcbSubscriptionFilter.filter(query), startAt.toStartAt())
                .filter(cloudEvent -> DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query));
    }
}
