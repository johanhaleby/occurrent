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

package org.occurrent.dsl.dcb.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.blocking.DcbSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Subscribes to live DCB-tagged events without having to pass the {@link CloudEventConverter} on every call.
 * <p>
 * This wraps a {@link SubscriptionModel} and a {@link CloudEventConverter}, mirroring how {@link DcbDomainEventQueries}
 * wraps its dependencies. The subscriptions are live CloudEvent subscriptions that post-filter DCB-tagged events
 * by a {@link DcbQuery}. They are not DCB reads, so they provide no DCB append-condition or high-watermark
 * guarantees.
 * <p>
 * Like {@link Subscribable}, the {@code subscribe} methods return the {@link Subscription} without waiting for it to
 * start. Call {@link Subscription#waitUntilStarted()} on the returned subscription when you need it running before
 * you continue (for example to avoid missing the first events of a brand new subscription). Call {@link #cancel(String)}
 * to stop and remove a subscription, for example when a per-connection subscription is no longer needed.
 * <p>
 * This DSL is for ephemeral, per-connection subscriptions that you start and cancel by hand, such as a Server-Sent-Events
 * feed scoped to one request. For a persistent, framework-managed subscription, such as a read model that catches up
 * from history on startup, use the {@code @DcbSubscription} annotation instead.
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
     * Subscribes to live DCB events that match {@code query}.
     */
    public Subscription subscribe(String subscriptionId, DcbQuery query, Consumer<E> fn) {
        return subscribe(subscriptionId, query, null, fn);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt}.
     */
    public Subscription subscribe(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, Consumer<E> fn) {
        requireNonNull(fn, "Subscription function cannot be null");
        return subscribeWithMetadata(subscriptionId, query, startAt, (metadata, event) -> fn.accept(event));
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt}. When {@code waitUntilStarted}
     * is {@code true} this blocks until the subscription has started, and for a replaying DCB subscription that means
     * until catch-up has completed, mirroring the Kotlin DSL default.
     */
    public Subscription subscribe(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, boolean waitUntilStarted, Consumer<E> fn) {
        requireNonNull(fn, "Subscription function cannot be null");
        return subscribeWithMetadata(subscriptionId, query, startAt, waitUntilStarted, (metadata, event) -> fn.accept(event));
    }

    /**
     * Subscribes to live DCB events that match {@code query}, exposing DCB metadata to the callback.
     * <p>
     * This is a distinct method rather than an overload of {@link #subscribe} so that a method reference like
     * {@code list::add} stays unambiguous on the {@link Consumer} overloads.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, BiConsumer<DcbEventMetadata, E> fn) {
        return subscribeWithMetadata(subscriptionId, query, null, fn);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt} and exposing DCB metadata
     * to the callback. Returns without waiting for the subscription to start.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, BiConsumer<DcbEventMetadata, E> fn) {
        return subscribeWithMetadata(subscriptionId, query, startAt, false, fn);
    }

    /**
     * Subscribes to live DCB events that match {@code query}, starting at {@code startAt} and exposing DCB metadata
     * to the callback. When {@code waitUntilStarted} is {@code true} this blocks until the subscription has started,
     * and for a replaying DCB subscription that means until catch-up has completed, mirroring the Kotlin DSL default.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, @Nullable DcbStartAt startAt, boolean waitUntilStarted, BiConsumer<DcbEventMetadata, E> fn) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(fn, "Subscription function cannot be null");

        // The DcbSubscriptionModel scopes delivery to the query (server-side where the backend supports it, and an
        // in-process floor in the typed adapter otherwise), so this callback only converts and dispatches.
        Consumer<CloudEvent> consumer = cloudEvent -> {
            E event = cloudEventConverter.toDomainEvent(cloudEvent);
            fn.accept(DcbEventMetadata.from(EventMetadata.from(cloudEvent)), event);
        };

        DcbStartAt startAtToUse = startAt == null ? DcbStartAt.subscriptionModelDefault() : startAt;
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, query, startAtToUse, consumer);
        if (waitUntilStarted) {
            subscription.waitUntilStarted();
        }
        return subscription;
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
