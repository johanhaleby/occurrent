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
 * Works out which event types a type filter has to name when a caller declares one type. A sealed type stands for the
 * concrete types it permits, all the way down, so a filter built from the declared type alone would silently miss the
 * events that are actually stored. A type that is never stored under its own name and whose concrete types cannot all be
 * found cannot be turned into a filter at all, and is reported back to the caller.
 * <p>
 * Shared by the saga DSL and the subscription annotations, which each derive a type filter from declared event types and
 * each used to walk the hierarchy themselves. The caller formats and throws, because a saga and a subscription have
 * different things to say about the type they were given.
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
        // A declared type that is stored under its own name is never refused, however little of the hierarchy below it
        // can be found, because it already names events that exist.
        if (!isStoredUnderItsOwnName(declaredType) && (!foundAll || concrete.isEmpty())) {
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
