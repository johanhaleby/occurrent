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
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Translates {@link DcbSubscriptionModel} calls into the shared reactive {@link SubscriptionModel}, building a
 * {@link DcbSubscriptionFilter} from the query and converting the {@link DcbStartAt} to a generic start position.
 * <p>
 * The named, lifecycle-managed {@code subscribe} methods additionally require the {@code delegate} to implement
 * {@link Subscribable} and {@link SubscriptionModelLifeCycle}, since a bare {@link SubscriptionModel} has no notion
 * of a named subscription that can be cancelled by id. The composed durable subscription model and
 * {@code ReactorMongoSubscriptionModel} both satisfy this.
 */
@NullMarked
final class DcbSubscriptionModelAdapter implements DcbSubscriptionModel {

    private final SubscriptionModel delegate;

    DcbSubscriptionModelAdapter(SubscriptionModel delegate) {
        this.delegate = requireNonNull(delegate, SubscriptionModel.class.getSimpleName() + " cannot be null");
    }

    @Override
    public Flux<CloudEvent> subscribe(DcbCriteria query, DcbStartAt startAt) {
        requireNonNull(query, "Criteria cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        // The DcbSubscriptionFilter is honored server-side for live delivery. The in-process check keeps the
        // subscription scoped to its own query for any backend that does not honor the filter, matching the blocking
        // adapter.
        return delegate.subscribe(DcbSubscriptionFilter.filter(query), startAt.toStartAt())
                .filter(cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, query));
    }

    @Override
    public Subscription subscribe(String subscriptionId, DcbCriteria query, DcbStartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(query, "Criteria cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        requireNonNull(action, "Subscription action cannot be null");
        if (!(delegate instanceof Subscribable subscribable)) {
            throw new IllegalStateException("Named DCB subscriptions require the underlying " + SubscriptionModel.class.getSimpleName() +
                    " to also implement " + Subscribable.class.getSimpleName() + ", but " + delegate.getClass().getName() + " does not.");
        }
        // The DcbSubscriptionFilter is honored server-side for live delivery, but a DCB catch-up replays by the
        // model-level query, so an in-process check keeps the subscription scoped to its own query during catch-up too
        // (and stays correct for any backend that does not honor the filter), matching the blocking adapter.
        Function<CloudEvent, Mono<Void>> scopedToQuery = cloudEvent -> {
            if (DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, query)) {
                return action.apply(cloudEvent);
            }
            return Mono.empty();
        };
        return subscribable.subscribe(subscriptionId, DcbSubscriptionFilter.filter(query), startAt.toStartAt(), scopedToQuery);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        if (!(delegate instanceof SubscriptionModelLifeCycle lifeCycle)) {
            throw new IllegalStateException("Cancelling named DCB subscriptions requires the underlying " + SubscriptionModel.class.getSimpleName() +
                    " to also implement " + SubscriptionModelLifeCycle.class.getSimpleName() + ", but " + delegate.getClass().getName() + " does not.");
        }
        lifeCycle.cancelSubscription(subscriptionId);
    }
}
