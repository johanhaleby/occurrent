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

import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

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
 * events with nothing to show for it. So a sealed type expands to the concrete types it permits, all the way down, an
 * enum expands to the classes of its constants, and a declared type whose concrete types cannot all be found is refused
 * rather than turned into a filter that would miss some of them.
 * <p>
 * Nothing is exempt from that rule any more. Up to 0.33.0 a non-sealed concrete class declared directly was accepted
 * with only itself in the filter. Under every {@code CloudEventTypeMapper} Occurrent ships, which store a subclass
 * under its own name, a caller declaring {@code class OrderPlaced} and publishing a subclass of it never saw that
 * subclass and got no warning. A mapper of the caller's own that maps the whole hierarchy onto one CloudEvent type
 * string is the exception, and that caller was working. It is refused from 0.34.0 either way, and the caller makes the
 * class final, seals the hierarchy, declares the concrete types, or sets an explicit filter, which is the one the
 * collapsing mapper wants. Events written as records or Kotlin data classes are final already, so an ordinary
 * hierarchy of records needs nothing.
 * <p>
 * Shared by every DSL that derives a type filter from declared event types, each of which used to walk the hierarchy on
 * its own. The caller formats and throws, because a saga, a projection, a subscription, a query and a snapshot view all
 * have different things to say about the type they were given.
 * <p>
 * The rule above governs {@link #expand}, {@link #concreteTypesOf} and {@link #deriveFilter}, which is {@code expand}
 * carried the rest of the way to a {@link org.occurrent.filter.Filter}. {@link #expandWhatCanBeFound} walks the same
 * hierarchy for a caller that derives no filter at all, or one that derives an exclusive filter, and it still refuses an
 * array or a primitive. Nothing here relaxes the rule, since neither of those callers has an inclusive filter for it to
 * be true of.
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
     * The plain {@link Filter} a caller's declared event types derive: {@link #expand} them and match every CloudEvent
     * type the expansion names, one {@link Condition#eq} per type, combined with {@link Condition#or} when there is more
     * than one. No declared types derives {@link Filter#all()}.
     * <p>
     * This is the one step every caller that derives a filter from declared event types was hand-rolling its own copy
     * of, {@code expand} plus the branch on how many types it comes back with. A declared type whose concrete types
     * cannot all be found is refused here exactly as it is in {@code expand}, since that is where the refusal happens.
     *
     * @param cloudEventTypeOf the CloudEvent type string an expanded event type is stored under
     * @param cannotExpand     builds the exception to throw for a type that cannot be turned into a filter
     */
    public static <E> Filter deriveFilter(Set<Class<? extends E>> declaredTypes,
                                          Function<Class<?>, String> cloudEventTypeOf,
                                          Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredTypes, "declaredTypes cannot be null");
        requireNonNull(cloudEventTypeOf, "cloudEventTypeOf cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> expanded = expand(declaredTypes, cannotExpand);
        List<Condition<String>> typeConditions = expanded.stream()
                .map(type -> Condition.eq(cloudEventTypeOf.apply(type)))
                .toList();
        return switch (typeConditions.size()) {
            case 0 -> Filter.all();
            case 1 -> Filter.type(typeConditions.getFirst());
            default -> Filter.type(Condition.or(typeConditions));
        };
    }

    /**
     * The declared types plus every concrete type they cover that can be found, in the same order {@link #expand} uses.
     * A declared type whose concrete types cannot all be found contributes the ones that can, instead of being refused.
     * <p>
     * <strong>Never for deriving a filter that decides what gets read.</strong> The rule at the top of this class is
     * enforced by {@link #expand}, and this method does not enforce it, so an inclusive filter built from what comes
     * back here can miss event types that dispatch would accept.
     * <p>
     * Two callers are safe, for different reasons. The saga DSL's {@code replacementFilter(Filter)} has been given an
     * explicit filter and so derives none, and only wants to report which event types it handles.
     * {@code ExecuteFilter.excludeTypes} derives an exclusive filter, where a type this walk misses narrows what gets
     * excluded rather than what gets read, so an event the caller wanted out stays in rather than the reverse. Widening
     * is the direction this method is safe in, which is why the same incompleteness that would lose events in an
     * inclusive filter is tolerable in an exclusive one.
     * <p>
     * An array and a primitive are still refused, through {@code cannotExpand}, and for two different strengths of
     * reason worth keeping apart. A primitive can match nothing at all, since {@code int.class.isInstance(..)} is false
     * for every object, so a saga declaring one would build and then never start an instance. An array is refused for
     * consistency rather than impossibility, because an object really can be an instance of an array type, and the
     * reason not to accept one here is that {@link #expand} and the subscription annotations both refuse a declared
     * array, so this path is not the place to become the single exception.
     * <p>
     * An interface, an abstract class, and a concrete class that is not final are all different from both, since a
     * hierarchy whose concrete types cannot all be found is exactly the case this method exists to be lenient about.
     *
     * @param cannotExpand builds the exception to throw for an array or a primitive
     */
    public static <E> Set<Class<? extends E>> expandWhatCanBeFound(Set<Class<? extends E>> declaredTypes,
                                                                   Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredTypes, "declaredTypes cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> expanded = new LinkedHashSet<>();
        for (Class<? extends E> declared : declaredTypes) {
            refuseArrayOrPrimitive(declared, cannotExpand);
            expanded.add(declared);
            collect(declared, expanded, new HashSet<>());
        }
        return Collections.unmodifiableSet(expanded);
    }

    /**
     * Refuses an array or a primitive declared event type, and accepts anything else without walking it. The two
     * reasons differ in strength, and both are given on {@link #expandWhatCanBeFound}, which applies this to each
     * declared type.
     * <p>
     * This is here on its own for a caller that narrows nothing and so has no set to expand, and that still should not
     * accept a declaration nothing can ever match. {@code Saga.create} with an empty {@code eventTypes} is the one such
     * caller, since it derives {@code Filter.all()} and walks no hierarchy, yet a start type nothing can be an instance
     * of would build a saga that never creates an instance.
     */
    public static void refuseArrayOrPrimitive(Class<?> declaredType, Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredType, "declaredType cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        if (declaredType.isArray() || declaredType.isPrimitive()) {
            throw cannotExpand.apply(declaredType);
        }
    }

    /**
     * The concrete event types {@code declaredType} covers, which is the type itself when it is stored under its own
     * name, and every concrete type it permits when it is sealed. Never empty. A sealed class that can be instantiated
     * is both, so it keeps itself and gains what it permits.
     * <p>
     * An enum covers the class of each of its constants, which is the constant's own class when that constant has a
     * body and the enum class itself when it does not. An enum with no constants covers nothing and is refused, since
     * no event can ever be an instance of it.
     * <p>
     * A concrete class that is neither final nor sealed is refused, because anything extending it is stored under its
     * own name where no walk can reach it. That refusal is new in 0.34.0, and up to 0.33.0 such a type was accepted
     * with only itself in the filter.
     *
     * @param cannotExpand builds the exception to throw for a type that cannot be turned into a filter
     */
    public static <E> List<Class<? extends E>> concreteTypesOf(Class<? extends E> declaredType,
                                                              Function<Class<?>, RuntimeException> cannotExpand) {
        requireNonNull(declaredType, "declaredType cannot be null");
        requireNonNull(cannotExpand, "cannotExpand cannot be null");
        Set<Class<? extends E>> concrete = new LinkedHashSet<>();
        boolean foundAll = collect(declaredType, concrete, new HashSet<>());
        // Being instantiable is not itself enough. A concrete class that is neither final nor sealed can be extended
        // where nothing here can see it, so it is refused like any other level that reopens the hierarchy, whether it
        // was declared directly or reached from a sealed root above it.
        if (!foundAll || concrete.isEmpty()) {
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
        // An enum closes its own hierarchy the way a permits clause does, since neither Java nor Kotlin lets anything
        // outside the declaration extend an enum type. Its constants are therefore every class an instance can have,
        // which is what makes this exact rather than a guess, and it is read from the constants rather than from
        // getPermittedSubclasses because only javac seals this construct. Kotlin compiles an enum whose constants have
        // bodies as a plain class with no permits clause at all, so the walk used to stop there and refuse it.
        if (type.isEnum()) {
            for (Object constant : type.getEnumConstants()) {
                // A constant with a body has its own final class, one without is an instance of the enum class itself,
                // and either way the constant's own class is what the event is stored under. An enum with no constants
                // adds nothing and is refused above, since no event can ever be an instance of it.
                concrete.add((Class<? extends E>) constant.getClass());
            }
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
