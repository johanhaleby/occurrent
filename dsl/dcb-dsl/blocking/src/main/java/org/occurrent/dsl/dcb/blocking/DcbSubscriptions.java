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
import org.occurrent.dsl.subscription.blocking.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Subscribes to live DCB-tagged events without having to pass the {@link CloudEventConverter} on every call.
 * <p>
 * This wraps a {@link Subscribable} and a {@link CloudEventConverter}, mirroring how {@link DcbDomainEventQueries}
 * wraps its dependencies. The subscriptions are live CloudEvent subscriptions that post-filter DCB-tagged events
 * by a {@link DcbQuery}. They are not DCB reads, so they provide no DCB append-condition or high-watermark
 * guarantees.
 * <p>
 * Like {@link Subscribable}, the {@code subscribe} methods return the {@link Subscription} without waiting for it to
 * start. Call {@link Subscription#waitUntilStarted()} on the returned subscription when you need it running before
 * you continue (for example to avoid missing the first events of a brand new subscription).
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class DcbSubscriptions<E> {

    private final Subscribable subscribable;
    private final CloudEventConverter<E> cloudEventConverter;

    public DcbSubscriptions(Subscribable subscribable, CloudEventConverter<E> cloudEventConverter) {
        this.subscribable = requireNonNull(subscribable, Subscribable.class.getSimpleName() + " cannot be null");
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
    public Subscription subscribe(String subscriptionId, DcbQuery query, @Nullable StartAt startAt, Consumer<E> fn) {
        requireNonNull(fn, "Subscription function cannot be null");
        return subscribeWithMetadata(subscriptionId, query, startAt, (metadata, event) -> fn.accept(event));
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
     * to the callback.
     */
    public Subscription subscribeWithMetadata(String subscriptionId, DcbQuery query, @Nullable StartAt startAt, BiConsumer<DcbEventMetadata, E> fn) {
        requireNonNull(subscriptionId, "Subscription id cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(fn, "Subscription function cannot be null");

        Consumer<CloudEvent> consumer = cloudEvent -> {
            if (DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query)) {
                E event = cloudEventConverter.toDomainEvent(cloudEvent);
                fn.accept(DcbEventMetadata.from(toEventMetadata(cloudEvent)), event);
            }
        };

        OccurrentSubscriptionFilter filter = OccurrentSubscriptionFilter.filter(Filter.all());
        return startAt == null
                ? subscribable.subscribe(subscriptionId, filter, consumer)
                : subscribable.subscribe(subscriptionId, filter, startAt, consumer);
    }

    private static EventMetadata toEventMetadata(CloudEvent cloudEvent) {
        Map<String, Object> data = new HashMap<>();
        for (String extensionName : cloudEvent.getExtensionNames()) {
            data.put(extensionName, cloudEvent.getExtension(extensionName));
        }
        return new EventMetadata(data);
    }
}
