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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Subscribes to live DCB-tagged events reactively without passing the {@link CloudEventConverter} on every call.
 * <p>
 * This wraps a reactive {@link SubscriptionModel} and a {@link CloudEventConverter}, mirroring how
 * {@link DcbDomainEventQueries} wraps its dependencies. Each {@code subscribe} returns a {@link Flux} that is the
 * subscription, so it is cancelled by cancelling the downstream subscription, for example disposing the
 * {@link reactor.core.Disposable} returned by {@code subscribe()}.
 * <p>
 * Delivery is live. Events are filtered by the {@link DcbQuery} server-side where the backend supports it, with an
 * in-process scoping filter as a correctness floor. A {@link DcbStartAt} is passed through to the underlying
 * {@link SubscriptionModel}, so whether a replay-oriented start such as {@link DcbStartAt#beginning()} replays history
 * depends on that model. The current reactive subscription models have no DCB catch-up, so such a start behaves like a
 * live start today.
 * <p>
 * The {@link #subscribe(String, DcbQuery, Function)} and {@link #subscribeWithMetadata(String, DcbQuery, BiFunction)}
 * methods below are the named, lifecycle-managed counterpart to the {@link Flux}-returning methods above, mirroring
 * {@link Subscribable}: they return a {@link Subscription} tracked by id, which can be cancelled with
 * {@link #cancel(String)}. Like {@link Subscribable}, they return without waiting for the subscription to start; call
 * {@link Subscription#waitUntilStarted()} on the returned subscription when you need it running before you continue.
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
                .map(cloudEvent -> new DcbEvent<>(DcbEventMetadata.from(EventMetadata.from(cloudEvent)), cloudEventConverter.toDomainEvent(cloudEvent)));
    }

    /**
     * Subscribes to live DCB events that match {@code query}, tracked by {@code subscriptionId}.
     */
    public Subscription subscribe(String subscriptionId, DcbQuery query, Function<E, Mono<Void>> fn) {
        return subscribe(subscriptionId, query, null, fn);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt}, tracked by
     * {@code subscriptionId}.
     */
    public Subscription subscribe(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, Function<E, Mono<Void>> fn) {
        requireNonNull(fn, "Subscription function cannot be null");
        return subscribeWithMetadata(subscriptionId, query, startAt, (metadata, event) -> fn.apply(event));
    }

    /**
     * Subscribes to live DCB events that match {@code query}, tracked by {@code subscriptionId}, exposing DCB metadata
     * to the callback.
     * <p>
     * This is a distinct method rather than an overload of {@link #subscribe} so that a method reference stays
     * unambiguous on the {@link Function} overloads.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, BiFunction<DcbEventMetadata, E, Mono<Void>> fn) {
        return subscribeWithMetadata(subscriptionId, query, null, fn);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt} and tracked by
     * {@code subscriptionId}, exposing DCB metadata to the callback. Returns without waiting for the subscription to
     * start.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, BiFunction<DcbEventMetadata, E, Mono<Void>> fn) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(fn, "Subscription function cannot be null");

        // The DcbSubscriptionModel scopes delivery to the query (server-side where the backend supports it, and an
        // in-process floor in the typed adapter otherwise), so this callback only converts and dispatches.
        Function<CloudEvent, Mono<Void>> action = cloudEvent -> {
            E event = cloudEventConverter.toDomainEvent(cloudEvent);
            return fn.apply(DcbEventMetadata.from(EventMetadata.from(cloudEvent)), event);
        };

        DcbStartAt startAtToUse = startAt == null ? DcbStartAt.subscriptionModelDefault() : startAt;
        return subscriptionModel.subscribe(subscriptionId, query, startAtToUse, action);
    }

    /**
     * Cancels and removes the subscription with the given id, stopping further delivery to its callback. Cancelling an
     * unknown or already cancelled subscription id is a no-op. This is the natural teardown for a per-connection
     * subscription, such as an SSE activity feed that subscribes when a client connects and cancels when it disconnects.
     */
    public void cancel(String subscriptionId) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        subscriptionModel.cancelSubscription(subscriptionId);
    }
}
