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

package org.occurrent.dsl.projection.internal;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.filter.Filter;
import org.occurrent.filter.internal.EventTypeExpansion;

import static java.util.Objects.requireNonNull;

/**
 * Derives the plain {@link Filter} a {@link Projection} selects on. Shared by the blocking and reactor runners so the
 * selector logic lives in one place, independent of the subscription stack.
 */
@NullMarked
public final class ProjectionFilters {

    private ProjectionFilters() {
    }

    /**
     * The plain {@link Filter} a projection selects on. Its explicit {@link Projection#filter() filter} wins if set,
     * otherwise this derives a type filter over its {@link Projection#eventTypes() handled event types}, expanded
     * through {@link EventTypeExpansion#deriveFilter} the same way the saga DSL and the subscription annotations
     * already do, so a handler registered on a sealed supertype asks for every concrete type it permits. Empty means
     * all events. A registered type whose concrete types cannot all be found is refused, naming the type and the
     * remedy.
     */
    @SuppressWarnings("unchecked")
    public static <E> Filter filterFor(CloudEventConverter<E> cloudEventConverter, Projection<?, E, ?> projection) {
        requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        requireNonNull(projection, "projection cannot be null");
        Filter explicit = projection.filter();
        if (explicit != null) {
            return explicit;
        }
        return EventTypeExpansion.deriveFilter(projection.eventTypes(),
                type -> cloudEventConverter.getCloudEventType((Class<? extends E>) type),
                ProjectionFilters::cannotDeriveFilterFor);
    }

    private static IllegalArgumentException cannotDeriveFilterFor(Class<?> eventType) {
        if (eventType.isArray()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a registered event type, since this expansion does not support an array. Register the concrete event types instead.");
        }
        if (eventType.isPrimitive()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a registered event type, since no event is ever an instance of a primitive type. Register the concrete event types instead.");
        }
        return new IllegalArgumentException("the concrete event types dispatch would accept for " + eventType.getName()
                + " cannot all be enumerated, so a filter derived from it would miss some of them. Register the concrete "
                + "event types instead, make " + eventType.getSimpleName() + " and every level below it final or sealed, "
                + "or set an explicit filter(...), which is used instead of deriving one and is the way out when a "
                + "CloudEventTypeMapper of your own maps the whole hierarchy onto a single CloudEvent type string.");
    }
}
