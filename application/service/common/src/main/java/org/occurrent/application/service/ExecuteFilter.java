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

package org.occurrent.application.service;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.filter.internal.EventTypeExpansion;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An application-service-level stream read filter that resolves domain event classes to CloudEvent types
 * through a {@link CloudEventTypeGetter} at execution time.
 * <p>
 * This type exists to keep {@link StreamReadFilter} independent from application service concerns while
 * still allowing fluent filters based on domain event classes such as {@code type(MyEvent.class)}. It is shared
 * by both the blocking and the reactive application services.
 *
 * @param <E> The application service event type.
 */
@FunctionalInterface
@NullMarked
public interface ExecuteFilter<E> {

    /**
     * Resolve this filter into a concrete {@link StreamReadFilter} using the supplied {@link CloudEventTypeGetter}.
     *
     * @param cloudEventTypeGetter Resolves a domain event class to a CloudEvent type.
     * @return A concrete stream read filter.
     */
    StreamReadFilter resolve(CloudEventTypeGetter<? super E> cloudEventTypeGetter);

    /**
     * Adapt an already constructed {@link StreamReadFilter} to an {@link ExecuteFilter}.
     */
    static <E> ExecuteFilter<E> from(StreamReadFilter filter) {
        Objects.requireNonNull(filter, "filter cannot be null");
        return __ -> filter;
    }

    /**
     * Create a filter that includes events of the supplied domain event type.
     * <p>
     * {@code eventType} is expanded the way every other type-filter derivation in the library expands a declared
     * type, through {@link EventTypeExpansion}: a sealed type expands to the concrete types it permits, all the way
     * down, and a type whose concrete types cannot all be found is refused rather than turned into a filter that
     * would miss some of them.
     */
    static <E> ExecuteFilter<E> type(Class<? extends E> eventType) {
        Objects.requireNonNull(eventType, "eventType cannot be null");
        return includeTypes(eventType);
    }

    /**
     * Create a filter that includes events whose CloudEvent type matches any of the supplied domain event types.
     * <p>
     * Each declared type is expanded the way {@link #type(Class)} expands one.
     */
    @SafeVarargs
    static <E> ExecuteFilter<E> includeTypes(Class<? extends E> first, Class<? extends E>... more) {
        Set<Class<? extends E>> declaredTypes = declaredTypes(first, more);
        return cloudEventTypeGetter -> {
            List<String> types = resolveCloudEventTypes(EventTypeExpansion.expand(declaredTypes, ExecuteFilter::cannotFilterOn), cloudEventTypeGetter);
            return StreamReadFilter.type(types.size() == 1 ? Condition.eq(types.getFirst()) : Condition.in(types));
        };
    }

    /**
     * Create a filter that excludes events whose CloudEvent type matches any of the supplied domain event types.
     * <p>
     * A declared type is WIDENED to every concrete type {@link EventTypeExpansion#expandWhatCanBeFound}'s downward
     * walk can find rather than refused when it cannot all be enumerated, which is the opposite of what
     * {@link #type(Class)} and {@link #includeTypes} do, and on purpose. Excluding a supertype has to exclude
     * everything under it, or the exclusion silently lets the excluded family of events through. Widening never
     * excludes fewer of the concrete types that walk finds than a complete expansion would, so it is never the
     * wrong direction to fail in. It does not promise the exclusion excludes something. A concrete class declared
     * directly that is itself neither final nor sealed contributes itself, the same as before this method went
     * through {@link EventTypeExpansion}, since no downward walk can find a subclass stored under its own name, but
     * the exclusion is not empty. <strong>An interface or an abstract class whose hierarchy reopens before the walk
     * finds anything concrete is different: it contributes nothing at all, so the resulting filter excludes zero
     * real events while looking like a working exclusion.</strong> {@code excludeTypes(SensitiveEvent.class)} on a
     * sealed {@code SensitiveEvent} that permits a non-sealed abstract class, with nothing concrete found above
     * that level, silently keeps every event of that family in the read. Seal the hierarchy, or declare the
     * concrete types directly, to get a working exclusion for that shape. An array and a primitive are refused, for
     * two different reasons kept apart the way {@link EventTypeExpansion#expandWhatCanBeFound} keeps them apart: no
     * event is ever an instance of a primitive class, while an array is refused for consistency with {@link #type} and
     * {@link #includeTypes}, not because excluding one is impossible.
     */
    @SafeVarargs
    static <E> ExecuteFilter<E> excludeTypes(Class<? extends E> first, Class<? extends E>... more) {
        Set<Class<? extends E>> declaredTypes = declaredTypes(first, more);
        return cloudEventTypeGetter -> StreamReadFilter.type(Condition.not(Condition.in(resolveCloudEventTypes(
                EventTypeExpansion.expandWhatCanBeFound(declaredTypes, ExecuteFilter::cannotExcludeArrayOrPrimitive), cloudEventTypeGetter))));
    }

    @SafeVarargs
    private static <E> Set<Class<? extends E>> declaredTypes(Class<? extends E> first, Class<? extends E>... more) {
        Objects.requireNonNull(first, "first event type cannot be null");
        Objects.requireNonNull(more, "additional event types cannot be null");
        Set<Class<? extends E>> declared = new LinkedHashSet<>();
        declared.add(first);
        for (Class<? extends E> eventType : more) {
            declared.add(Objects.requireNonNull(eventType, "eventType cannot be null"));
        }
        return declared;
    }

    private static <E> List<String> resolveCloudEventTypes(Set<Class<? extends E>> eventTypes, CloudEventTypeGetter<? super E> cloudEventTypeGetter) {
        Objects.requireNonNull(cloudEventTypeGetter, "cloudEventTypeGetter cannot be null");
        return eventTypes.stream().map(cloudEventTypeGetter::getCloudEventType).distinct().toList();
    }

    private static IllegalArgumentException cannotFilterOn(Class<?> eventType) {
        if (eventType.isArray()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be filtered on by type, since this expansion does not support an array. Filter on the concrete event types instead.");
        }
        if (eventType.isPrimitive()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be filtered on by type, since no event is ever an instance of a primitive type. Filter on the concrete event types instead.");
        }
        return new IllegalArgumentException("the concrete event types dispatch would accept for " + eventType.getName()
                + " cannot all be enumerated, so a filter derived from it would miss some of them. Filter on the "
                + "concrete event types instead, make " + eventType.getSimpleName() + " and every level below it "
                + "final or sealed, or build the StreamReadFilter yourself with ExecuteFilter.from(..), which is the "
                + "way out when a CloudEventTypeMapper of your own maps the whole hierarchy onto a single CloudEvent "
                + "type string.");
    }

    private static IllegalArgumentException cannotExcludeArrayOrPrimitive(Class<?> eventType) {
        if (eventType.isArray()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be excluded by type, since this expansion does not support an array. Exclude the concrete event types instead.");
        }
        return new IllegalArgumentException(eventType.getTypeName()
                + " cannot be excluded by type, since no event is ever an instance of a primitive type. Exclude the concrete event types instead.");
    }
}
