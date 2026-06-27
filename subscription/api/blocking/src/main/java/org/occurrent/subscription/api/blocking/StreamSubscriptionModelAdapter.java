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
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.StartAt;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link StreamSubscriptionModel} calls into the shared {@link SubscriptionModel}, wrapping the
 * {@link Filter} in an {@link OccurrentSubscriptionFilter}. All life-cycle calls forward to the delegate (see
 * {@link AbstractDelegatingSubscriptionModelAdapter}).
 */
@NullMarked
final class StreamSubscriptionModelAdapter extends AbstractDelegatingSubscriptionModelAdapter implements StreamSubscriptionModel {

    StreamSubscriptionModelAdapter(SubscriptionModel delegate) {
        super(delegate);
    }

    @Override
    public Subscription subscribe(String subscriptionId, Filter filter, StartAt startAt, Consumer<CloudEvent> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        requireNonNull(action, "Subscription action cannot be null");
        return delegate.subscribe(subscriptionId, OccurrentSubscriptionFilter.filter(filter), startAt, action);
    }
}
