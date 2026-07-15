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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

import java.util.List;

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
     * The plain {@link Filter} a projection selects on: its explicit {@link Projection#filter() filter} if set,
     * otherwise a type filter over its {@link Projection#eventTypes() handled event types}, resolved to CloudEvent type
     * strings through {@code cloudEventConverter}. An empty handled-type set means "all events". It also honours the
     * descriptor's explicit filter, which the subscription DSL's {@code filterFromEventTypes} does not.
     * <p>
     * The type-to-filter mapping is a small, deliberate re-implementation of that {@code filterFromEventTypes}. Reusing
     * it would make {@code projection-dsl-common} depend on {@code subscription-dsl-common} for a handful of lines and
     * mean adapting a {@code Set<Class<?>>} to its Kotlin {@code Array<KClass>} parameter, so the copy is the cheaper
     * side of the tradeoff. Keep this module independent of the subscription stack rather than collapsing the two.
     */
    public static <E> Filter filterFor(CloudEventConverter<E> cloudEventConverter, Projection<?, E, ?> projection) {
        requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        requireNonNull(projection, "projection cannot be null");
        Filter explicit = projection.filter();
        if (explicit != null) {
            return explicit;
        }
        List<Condition<String>> typeConditions = projection.eventTypes().stream()
                .map(type -> Condition.eq(cloudEventConverter.getCloudEventType(type)))
                .toList();
        return switch (typeConditions.size()) {
            case 0 -> Filter.all();
            case 1 -> Filter.type(typeConditions.getFirst());
            default -> Filter.type(Condition.or(typeConditions));
        };
    }
}
