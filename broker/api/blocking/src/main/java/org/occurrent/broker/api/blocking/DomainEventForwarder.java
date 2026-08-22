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

package org.occurrent.broker.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Drives stored events out of the event store into a {@link DomainEventSink}, decoding each {@link CloudEvent}
 * once with a {@link CloudEventConverter} rather than leaving the sink to convert back and forth. Only pair this
 * with a {@link DomainEventSink} an application implements at the domain level. A shipped domain sink is built
 * from a {@link CloudEventSink} and converts a domain event back to a {@link CloudEvent} to publish it, so pairing
 * one of those with this forwarder decodes a stored event and immediately re-encodes it, which loses the id,
 * source, subject and time the store recorded and gains nothing a plain {@link CloudEventForwarder} would not
 * already do without converting at all.
 * <p>
 * The stream identity survives this path because the Occurrent extensions travel as {@link EventMetadata} rather
 * than being derived. The id, source, subject, time and, unless the sink's own encoding matches what the store
 * holds, the data do not.
 * <p>
 * Publication is at least once the same way {@link CloudEventForwarder} publication is, provided the
 * {@link DomainEventSink} holds to its own contract and does not return until it has confirmed delivery. That
 * guarantee also holds only for the {@link #forward(String)} and {@link #forward(String, SubscriptionFilter)}
 * overloads, which leave {@code startAt} at the subscription model's default and so read the checkpoint on every
 * start, restart included.
 * The two overloads that take an explicit {@link StartAt} hand that decision to the caller instead, and
 * {@code DurableSubscriptionModel} only consults the checkpoint for its own default position, so a literal position
 * such as {@link StartAt#now()} restarts from there again after a crash rather than resuming, and whatever failed
 * to publish just before the crash is skipped. Pass an explicit {@code startAt} only when the caller tracks its own
 * resume position durably, or accept that restarting loses that guarantee.
 */
public class DomainEventForwarder<E> {

    private final DurableSubscriptionModel subscriptionModel;
    private final CloudEventConverter<E> converter;
    private final DomainEventSink<E> sink;

    public DomainEventForwarder(DurableSubscriptionModel subscriptionModel, CloudEventConverter<E> converter, DomainEventSink<E> sink) {
        this.subscriptionModel = requireNonNull(subscriptionModel, DurableSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.converter = requireNonNull(converter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        this.sink = requireNonNull(sink, DomainEventSink.class.getSimpleName() + " cannot be null");
    }

    /**
     * Start forwarding at the subscription model's default start position, with no filter.
     */
    public Subscription forward(String subscriptionId) {
        return subscriptionModel.subscribe(subscriptionId, decodeAndPublish());
    }

    /**
     * Start forwarding at {@code startAt}, with no filter. See the class javadoc for what an explicit
     * {@code startAt} costs on a restart.
     */
    public Subscription forward(String subscriptionId, StartAt startAt) {
        return subscriptionModel.subscribe(subscriptionId, startAt, decodeAndPublish());
    }

    /**
     * Start forwarding only events matching {@code filter}, at the subscription model's default start position.
     */
    public Subscription forward(String subscriptionId, @Nullable SubscriptionFilter filter) {
        return subscriptionModel.subscribe(subscriptionId, filter, decodeAndPublish());
    }

    /**
     * Start forwarding only events matching {@code filter}, at {@code startAt}. See the class javadoc for what an
     * explicit {@code startAt} costs on a restart.
     */
    public Subscription forward(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt) {
        return subscriptionModel.subscribe(subscriptionId, filter, startAt, decodeAndPublish());
    }

    private Consumer<CloudEvent> decodeAndPublish() {
        return cloudEvent -> {
            E domainEvent = converter.toDomainEvent(cloudEvent);
            EventMetadata metadata = EventMetadata.from(cloudEvent);
            sink.publish(metadata, domainEvent);
        };
    }
}
