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
 *
 * @param <E> The domain event type this sink publishes.
 */
public interface DomainEventSink<E> {

    /**
     * Publish a domain event that has never been through the event store, so it carries no stream identity. A
     * consumer that reads the resulting message sees an {@link EventMetadata#empty() empty EventMetadata}.
     */
    void publish(E domainEvent);

    /**
     * Publish several domain events. The default publishes one at a time, and an implementation that can publish
     * several more efficiently overrides this.
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
