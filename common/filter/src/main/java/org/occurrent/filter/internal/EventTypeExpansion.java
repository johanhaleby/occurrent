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

package org.occurrent.filter.internal;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Works out which event types a type filter has to name when a caller declares one type.
 * <p>
 * <strong>The rule this exists to enforce. The derived filter must name every event type that dispatch would accept.</strong>
 * Dispatch accepts an event by assignability, through {@code isInstance} and a handler lookup that walks superclasses and
 * interfaces, so a declared supertype accepts every concrete subtype. A filter that names fewer types than that loses
 * events with nothing to show for it. So a sealed type expands to the concrete types it permits, all the way down, and a
 * declared type whose concrete types cannot all be found is refused rather than turned into a filter that would miss
 * some of them.
 * <p>
 * One case is exempt on purpose. A non-sealed concrete class declared directly is accepted, and its subclasses are not
 * found, so dispatch accepts events the filter does not name. Refusing it would refuse every saga and subscription that
 * declares a class which is not final, which is behaviour that shipped. Events written as records or Kotlin data classes
 * are final already, so the exemption is narrow in practice.
 * <p>
 * Shared by the saga DSL and the subscription annotations, which each derive a type filter from declared event types and
 * each used to walk the hierarchy themselves. The caller formats and throws, because a saga and a subscription have
 * different things to say about the type they were given.
 * <p>
 * The rule above governs {@link #expand} and {@link #concreteTypesOf}, the two entry points a derived filter is built
 * from. {@link #expandWhatCanBeFound} walks the same hierarchy for a caller that was handed an explicit filter and so
 * derives none, and it still refuses an array or a primitive. Nothing here relaxes the rule, because a caller with no
 * derived filter has no filter for it to be true of.
 */
public final class EventTypeExpansion {

    private EventTypeExpansion() {
    }

    /**
     * The declared types plus the concrete types each of them covers, each declared type followed by its own expansion.
     * Iteration order follows the declared types, so a filter and any message naming one of them come out the same on
     * every run.
     *
     * @param cannotExpand builds the exception to throw for a type that cannot be turned into a filter
     */
    public static <E> Set<Class<? extends E>> expand(Set<Class<? extends E>> declaredTypes,
                                                     Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredTypes, "declaredTypes cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> expanded = new LinkedHashSet<>();
        for (Class<? extends E> declared : declaredTypes) {
            expanded.add(declared);
            expanded.addAll(concreteTypesOf(declared, cannotExpand));
        }
        return Collections.unmodifiableSet(expanded);
    }

    /**
     * The declared types plus every concrete type they cover that can be found, in the same order {@link #expand} uses.
     * A declared type whose concrete types cannot all be found contributes the ones that can, instead of being refused.
     * <p>
     * <strong>Only for a caller that is not deriving a filter.</strong> The rule at the top of this class is enforced by
     * {@link #expand}, and this method does not enforce it, so a filter built from what comes back here can miss event
     * types that dispatch would accept. It exists for a caller that has been given an explicit filter and so derives
     * none, and still wants to report which event types it handles. The saga DSL's {@code filter(Filter)} override is
     * the one such caller.
     * <p>
     * An array and a primitive are still refused, through {@code cannotExpand}, and for two different strengths of
     * reason worth keeping apart. A primitive can match nothing at all, since {@code int.class.isInstance(..)} is false
     * for every object, so a saga declaring one would build and then never start an instance. An array is refused for
     * consistency rather than impossibility, because an object really can be an instance of an array type, and the
     * reason not to accept one here is that {@link #expand} and the subscription annotations both refuse a declared
     * array, so this path is not the place to become the single exception.
     * <p>
     * An interface or an abstract class is different from both, since a hierarchy whose concrete types cannot all be
     * found is exactly the case this method exists to be lenient about.
     *
     * @param cannotExpand builds the exception to throw for an array or a primitive
     */
    public static <E> Set<Class<? extends E>> expandWhatCanBeFound(Set<Class<? extends E>> declaredTypes,
                                                                   Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredTypes, "declaredTypes cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> expanded = new LinkedHashSet<>();
        for (Class<? extends E> declared : declaredTypes) {
            if (declared.isArray() || declared.isPrimitive()) {
                throw cannotExpand.apply(declared);
            }
            expanded.add(declared);
            collect(declared, expanded, new HashSet<>());
        }
        return Collections.unmodifiableSet(expanded);
    }

    /**
     * The concrete event types {@code declaredType} covers, which is the type itself when it is stored under its own
     * name, and every concrete type it permits when it is sealed. Never empty.
     *
     * @param cannotExpand builds the exception to throw for a type that cannot be turned into a filter
     */
    public static <E> List<Class<? extends E>> concreteTypesOf(Class<? extends E> declaredType,
                                                              Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredType, "declaredType cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> concrete = new LinkedHashSet<>();
        boolean foundAll = collect(declaredType, concrete, new HashSet<>());
        // A non-sealed concrete class declared directly is accepted even though its subclasses cannot be found, which is
        // how every saga declaring a non-final event class kept working. Being instantiable is not itself enough. A
        // sealed root says its subtypes are knowable, so an incomplete hierarchy under one is refused whether or not the
        // root can be stored.
        boolean declaredConcreteAndOpen = !declaredType.isSealed() && isStoredUnderItsOwnName(declaredType);
        if (!declaredConcreteAndOpen && (!foundAll || concrete.isEmpty())) {
            throw cannotExpand.apply(declaredType);
        }
        return List.copyOf(concrete);
    }

    // Returns false as soon as one level cannot be walked further, so a hierarchy reopened part way down counts as
    // incomplete rather than stopping quietly there. A class that is neither sealed nor final reopens it even when the
    // class itself is concrete, because its own subclasses are stored under their own names and cannot be found here.
    @SuppressWarnings("unchecked")
    private static <E> boolean collect(Class<? extends E> type, Set<Class<? extends E>> concrete, Set<Class<?>> visited) {
        if (!visited.add(type)) {
            return true;
        }
        boolean stored = isStoredUnderItsOwnName(type);
        if (stored) {
            concrete.add(type);
        }
        if (!type.isSealed()) {
            return stored && Modifier.isFinal(type.getModifiers());
        }
        boolean foundAll = true;
        for (Class<?> permitted : type.getPermittedSubclasses()) {
            foundAll &= collect((Class<? extends E>) permitted, concrete, visited);
        }
        return foundAll;
    }

    // An array is final and not an interface, so it has to be ruled out by name rather than by its modifiers.
    private static boolean isStoredUnderItsOwnName(Class<?> type) {
        return !type.isInterface() && !type.isArray() && !Modifier.isAbstract(type.getModifiers());
    }
}
