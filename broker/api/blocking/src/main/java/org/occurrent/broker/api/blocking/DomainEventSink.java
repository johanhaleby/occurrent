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

import org.occurrent.cloudevents.EventMetadata;

/**
 * Publishes a domain event of type {@code E} to a broker, for an application whose own message converter already
 * produces domain events and would otherwise convert to a {@link io.cloudevents.CloudEvent} and back for nothing.
 * A shipped implementation is built from a {@link CloudEventSink} and a converter and delegates rather than
 * talking to the broker client itself, but this stays a plain interface with no such requirement, so an
 * application is free to implement it directly.
 * <p>
 * The same rule {@link CloudEventSink} states applies here. A {@code publish} call is not by itself an
 * at-least-once guarantee, so an implementation must not return until it has confirmed the broker took the
 * message, and must throw rather than report success when that cannot be established. {@link DomainEventForwarder}
 * relies on exactly that to hold its own at-least-once guarantee.
 *
 * @param <E> The domain event type this sink publishes.
 */
public interface DomainEventSink<E> {

    /**
     * Publish a domain event that has never been through the event store, so it carries no stream identity. A
     * consumer that reads the resulting message sees an {@link EventMetadata} with no {@code streamid},
     * {@code streamversion}, {@code position} or {@code appendid}, since a never-stored event has none of those,
     * but with every other extension the converter set, since {@link EventMetadata}'s own contract is to carry
     * whatever extension the event actually has, not only Occurrent's own four.
     */
    void publish(E domainEvent);

    /**
     * Publish several domain events. The default publishes one at a time, and an implementation that can publish
     * several more efficiently overrides this.
     * <p>
     * Publishing one at a time means waiting for the broker to acknowledge each event before starting the next, so
     * a call that throws part way has already published the events before the one it threw on. That is what makes
     * this an at-least-once building block rather than something to make faster. A caller that retries the whole
     * {@code Iterable} republishes those, which is a duplicate and never a loss. An override that batches has to
     * hold the same property, so it cannot report success until the broker has taken every event it was given.
     * <p>
     * The default sends each event through {@code publish(E)}, so none of the resulting messages has a stream
     * identity. There is no {@code Iterable} form of {@code publish(EventMetadata, E)}, since every event would need
     * its own metadata.
     */
    default void publish(Iterable<E> domainEvents) {
        for (E domainEvent : domainEvents) {
            publish(domainEvent);
        }
    }

    /**
     * Publish a domain event read from a stored {@link io.cloudevents.CloudEvent}, stamping {@code metadata} onto
     * the resulting message so a consumer can rebuild it with {@link EventMetadata}. This is what
     * {@link DomainEventForwarder} calls, and it is the overload to use whenever the domain event's stream
     * identity is known rather than fabricated.
     */
    void publish(EventMetadata metadata, E domainEvent);
}
