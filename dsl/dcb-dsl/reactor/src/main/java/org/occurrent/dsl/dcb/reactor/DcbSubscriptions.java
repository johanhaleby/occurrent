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

package org.occurrent.dsl.dcb.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;

/**
 * Subscribes to live DCB-tagged events reactively without passing the {@link CloudEventConverter} on every call.
 * <p>
 * This wraps a reactive {@link SubscriptionModel} and a {@link CloudEventConverter}, mirroring how
 * {@link DcbDomainEventQueries} wraps its dependencies. Each {@code subscribe} returns a {@link Flux} that is the
 * subscription, so cancelling is disposing the {@link Flux}.
 * <p>
 * Delivery is live and the events are post-filtered by the {@link DcbQuery} server-side where the backend supports it.
 * A {@link DcbStartAt} that asks to replay history is passed through to the live subscription, which starts live,
 * because reactive DCB catch-up is not implemented yet.
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class DcbSubscriptions<E> {

    private final DcbSubscriptionModel subscriptionModel;
    private final CloudEventConverter<E> cloudEventConverter;

    public DcbSubscriptions(SubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        this.subscriptionModel = DcbSubscriptionModel.from(requireNonNull(subscriptionModel, SubscriptionModel.class.getSimpleName() + " cannot be null"));
        this.cloudEventConverter = requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
    }

    /**
     * Subscribes to live DCB events that match {@code query}, converting each to a domain event.
     */
    public Flux<E> subscribe(DcbQuery query) {
        return subscribe(query, DcbStartAt.subscriptionModelDefault());
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt}, converting each to a domain
     * event.
     */
    public Flux<E> subscribe(DcbQuery query, DcbStartAt startAt) {
        return subscriptionModel.subscribe(query, startAt).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, delivering each domain event together with its DCB
     * metadata.
     */
    public Flux<DcbEvent<E>> subscribeWithMetadata(DcbQuery query) {
        return subscribeWithMetadata(query, DcbStartAt.subscriptionModelDefault());
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt}, delivering each domain event
     * together with its DCB metadata.
     */
    public Flux<DcbEvent<E>> subscribeWithMetadata(DcbQuery query, DcbStartAt startAt) {
        return subscriptionModel.subscribe(query, startAt)
                .map(cloudEvent -> new DcbEvent<>(DcbEventMetadata.from(cloudEvent), cloudEventConverter.toDomainEvent(cloudEvent)));
    }
}
