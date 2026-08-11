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

package org.occurrent.dsl.saga.internal;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Turns the event types a saga declares into the types its subscription has to ask for. A sealed type is joined by every
 * concrete type it permits, all the way down, so a saga that declares a sealed supertype still receives the concrete
 * events that are actually stored. A declared type that is never stored under its own name and whose concrete subtypes
 * cannot all be found is refused instead, since a subscription built from it would match nothing.
 */
public final class EventTypeExpansion {

    private EventTypeExpansion() {
    }

    /**
     * The declared types plus the concrete types any sealed one among them permits, each declared type followed by what
     * it expanded into. Iteration order follows the declared types, so a message naming one of them and a filter built
     * from them both come out the same on every run.
     *
     * @throws IllegalStateException if a declared type is abstract and its concrete subtypes cannot all be found
     */
    public static <E> Set<Class<? extends E>> expand(Set<Class<? extends E>> declaredTypes) {
        requireNonNull(declaredTypes, "declaredTypes cannot be null");
        Set<Class<? extends E>> expanded = new LinkedHashSet<>();
        for (Class<? extends E> declared : declaredTypes) {
            expanded.add(declared);
            Set<Class<? extends E>> concrete = new LinkedHashSet<>();
            boolean foundAll = collectConcreteTypes(declared, concrete, new HashSet<>());
            if (!isStoredUnderItsOwnName(declared) && (!foundAll || concrete.isEmpty())) {
                throw new IllegalStateException(refusal(declared));
            }
            expanded.addAll(concrete);
        }
        return Collections.unmodifiableSet(expanded);
    }

    // Returns false as soon as one level cannot be walked further, so a sealed hierarchy with a plain abstract class
    // somewhere inside it counts as incomplete rather than stopping quietly at that class.
    @SuppressWarnings("unchecked")
    private static <E> boolean collectConcreteTypes(Class<? extends E> type, Set<Class<? extends E>> concrete, Set<Class<?>> visited) {
        if (!visited.add(type)) {
            return true;
        }
        boolean stored = isStoredUnderItsOwnName(type);
        if (stored) {
            concrete.add(type);
        }
        if (!type.isSealed()) {
            return stored;
        }
        boolean foundAll = true;
        for (Class<?> permitted : type.getPermittedSubclasses()) {
            foundAll &= collectConcreteTypes((Class<? extends E>) permitted, concrete, visited);
        }
        return foundAll;
    }

    private static boolean isStoredUnderItsOwnName(Class<?> type) {
        return !type.isInterface() && !Modifier.isAbstract(type.getModifiers());
    }

    private static String refusal(Class<?> declared) {
        return "event type " + declared.getName() + " is abstract and its concrete subtypes cannot all be found, so the "
                + "subscription derived from it would match no stored event and the saga would receive nothing. Declare "
                + "the concrete event types instead, or make every level of the hierarchy below " + declared.getSimpleName()
                + " sealed.";
    }
}
