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

package org.occurrent.dsl.saga;

import org.occurrent.dsl.subscription.EventMetadata;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The closed input alphabet of a {@link Saga}: either a domain event, or one of the saga's own timers firing. A saga is a
 * single state machine over this union, so it has exactly one {@link Saga#evolve(Object, SagaInput)} and one
 * {@link Saga#react(Object, SagaInput)}, rather than a separate channel for timeouts.
 *
 * @param <E> the domain event type
 */
public sealed interface SagaInput<E> {

    // Metadata-less delivery marker, used by the event-only convenience factory. A metadata-less input carries an
    // EventMetadata with no extensions, so its typed accessors have nothing to return; a reaction that wants metadata must
    // be delivered through a runner that captures it. (The subscription DSL exposes EventMetadata.from(cloudEvent); there
    // is no EventMetadata.empty() on this base commit, so we build an empty one directly.)
    EventMetadata NO_METADATA = new EventMetadata(Map.of());

    /**
     * A domain event delivered to the saga, together with its delivery {@link EventMetadata} (stream id and version,
     * global position, and any CloudEvent extensions). A runner that has a CloudEvent builds this with
     * {@link EventMetadata#from}; the event-only {@link #event(Object)} factory carries {@link #NO_METADATA}.
     */
    record Event<E>(E event, EventMetadata metadata) implements SagaInput<E> {
        public Event {
            requireNonNull(event, "event cannot be null");
            requireNonNull(metadata, "metadata cannot be null");
        }
    }

    /**
     * A saga timer that has fired. {@code E} is phantom here, it only exists so the union is well-typed against the event
     * variant.
     */
    record Timeout<E>(SagaTimeout timeout) implements SagaInput<E> {
        public Timeout {
            requireNonNull(timeout, "timeout cannot be null");
        }
    }

    /** Wraps a domain event as a saga input, with no delivery metadata (see {@link #NO_METADATA}). */
    static <E> SagaInput<E> event(E event) {
        return new Event<>(event, NO_METADATA);
    }

    /** Wraps a domain event and its delivery metadata as a saga input. */
    static <E> SagaInput<E> event(E event, EventMetadata metadata) {
        return new Event<>(event, metadata);
    }

    /** Wraps a fired timer as a saga input. */
    static <E> SagaInput<E> timeout(SagaTimeout timeout) {
        return new Timeout<>(timeout);
    }
}
