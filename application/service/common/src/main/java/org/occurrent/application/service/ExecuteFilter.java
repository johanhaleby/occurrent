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
     * type, through {@link EventTypeExpansion}. A sealed type expands to the concrete types it permits, all the way
     * down, an enum expands to the classes of its constants, and a type whose concrete types cannot all be found is
     * refused rather than turned into a filter that would miss some of them. The finding is done by the walk
     * described on {@link #excludeTypes}, so both directions mean the same walk when they say a type can or cannot
     * be found.
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
     * A declared type is WIDENED to every concrete type {@link EventTypeExpansion#expandWhatCanBeFound} can find
     * rather than refused when it cannot find them all, which is the opposite of what {@link #type(Class)} and
     * {@link #includeTypes} do, and on purpose. Excluding a supertype has to exclude everything under it, or the
     * exclusion silently lets the excluded family of events through.
     * <p>
     * <strong>The walk doing the finding follows a hierarchy that closes itself.</strong> It starts at the declared
     * type, follows a {@code permits} clause through {@link Class#getPermittedSubclasses}, expands an enum through
     * its constants, and stops at the first level that is neither sealed nor an enum. It reads no classpath and
     * consults no index of subtypes, so a subclass declared outside a {@code permits} clause is beyond it. For a declared type {@code T} and a {@link CloudEventTypeGetter}
     * {@code g}, the filter excludes the CloudEvent types {@code g} returns for {@code T} itself and for every
     * concrete type that walk reaches below {@code T}. Growing that set only takes events out of the read, so a
     * type the walk cannot reach means an event the caller wanted out stays in, rather than the reverse. Being
     * incomplete is harmless in that one direction, which is a smaller claim than the exclusion doing what its
     * name promises.
     * <p>
     * {@code expandWhatCanBeFound} names this method as one of the two callers it is safe for. A missed type would
     * narrow what an inclusive filter reads, which is what that method's scope warns against, while here it only
     * narrows what gets excluded. The other safe caller derives no filter at all.
     * <p>
     * A concrete class declared directly that is itself neither final nor sealed contributes itself, the same as
     * before this method went through {@link EventTypeExpansion}, since no downward walk can find a subclass
     * stored under its own name, but the exclusion is not empty.
     * <p>
     * <strong>An interface or an abstract class whose hierarchy reopens before the walk finds anything concrete is
     * different. It contributes its own declared name and nothing else, and how much that excludes is decided by
     * {@code g} rather than by the walk.</strong> Under a getter that maps each type to its own class name, which
     * is what {@code ReflectionCloudEventTypeMapper} does in both its qualified and its simple form, no stored
     * event is written under the declared type's own name, so the filter excludes zero real events while looking like a
     * working exclusion.
     * {@code excludeTypes(SensitiveEvent.class)} on a sealed {@code SensitiveEvent} that permits a non-sealed
     * abstract class, with nothing concrete found above that level, silently keeps every event of that family in
     * the read. Under a getter of your own that maps a whole hierarchy onto one CloudEvent type string, the same
     * declaration excludes the whole family instead, because that one string is what the concrete events are
     * stored under. Seal the hierarchy, or declare the concrete types directly, for an exclusion that does not
     * depend on which of those two your getter is.
     * <p>
     * An array and a primitive are refused, for two different reasons kept apart the way
     * {@link EventTypeExpansion#expandWhatCanBeFound} keeps them apart. No event is ever an instance of a
     * primitive class, while an array is refused for consistency with {@link #type} and {@link #includeTypes},
     * not because excluding one is impossible.
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
                    + " cannot be filtered on by type, since this expansion does not support an array. An array class is "
                    + "already concrete, so there is no narrower type to name, and it is refused for consistency with the "
                    + "other declared shapes rather than because nothing can be an instance of one. Build the "
                    + "StreamReadFilter yourself with ExecuteFilter.from(..) if you do mean to filter on an array type.");
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
                    + " cannot be excluded by type, since this expansion does not support an array. An array class is "
                    + "already concrete, so there is no narrower type to name, and it is refused for consistency with "
                    + "type and includeTypes rather than because excluding one is impossible. Build the StreamReadFilter "
                    + "yourself with ExecuteFilter.from(..) if you do mean to exclude an array type.");
        }
        return new IllegalArgumentException(eventType.getTypeName()
                + " cannot be excluded by type, since no event is ever an instance of a primitive type. Exclude the concrete event types instead.");
    }
}
