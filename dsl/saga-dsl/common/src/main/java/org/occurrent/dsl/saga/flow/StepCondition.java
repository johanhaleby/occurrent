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

package org.occurrent.dsl.saga.flow;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * A condition a flow step's window of received events can satisfy, for {@link StepBuilder#on(StepCondition, Continuation)}.
 * A leaf ({@link AtLeast}) lifts an event-level match, a type and an optional predicate over a single event, to a
 * window-level count: "the window since this step was entered contains at least {@code n} events matching this leaf".
 * {@link AllOf} and {@link AnyOf} combine leaves and other composites into a tree.
 * <p>
 * A condition tree is monotone. Every leaf only ever asks whether enough matching events have arrived, never whether
 * one is absent or whether a count stays under a limit. That is what makes checking a tree fresh on every arriving
 * event correct rather than merely convenient. Once a leaf's count is reached, no later event can undo it, so there
 * is never a negative to look for. There is deliberately no {@code not()} and no way to match on an event's absence.
 * A step's {@code timeout} is what expresses "this did not happen in time".
 * <p>
 * That property depends on a leaf's predicate being a deterministic function of the event it is handed, and nothing else.
 * A predicate is run again whenever a leaf's count has to be derived from the step window rather than read off the count
 * the instance carries, and a replay runs it again from the start, so one that consults the clock, a random source, mutable
 * state, or a remote service can answer differently for the same event later. That breaks the "once true, always true"
 * property the design rests on and makes a replay diverge from the original run. Deciding an event on its own contents is
 * supported, and anything else is not. Put a time or lookup condition in the event that records the decision, or in a
 * {@code timeout}, instead. See {@link EventMatcher#predicate()}.
 * <p>
 * Within one {@code allOf}, two children that match the same event are refused when the tree is built, since such a
 * declaration reads as more than it requires. {@code anyOf} permits them, and the rule has stated limits, see
 * {@link #allOf(StepCondition, StepCondition[])}.
 * <p>
 * A tree is data, not a closure over the builder, so it can be built once, given a name, and reused across several
 * {@code on(...)} calls, or across steps: {@code var cancelled = anyOf(event(Cancelled.class), event(TimedOut.class));}.
 * Reuse is also why {@link StepBuilder#on(StepCondition, Continuation)} accepts {@code StepCondition<? extends E>}
 * rather than the invariant {@code StepCondition<E>}: building a tree from leaves of different concrete event types
 * infers {@code E} as their least upper bound, which an invariant parameter would refuse.
 * <p>
 * When an {@link AnyOf} fires, the reaction's {@code whenFulfilled} sees the window it fired on, the events received since
 * the step was entered and nothing earlier, and the event that tipped it over is always the last element of
 * {@link ReceivedEvents#asList()}, whichever alternative it matched. Which alternative that was is not reported, and asking
 * the window is not a reliable way to find out, because a leaf can be satisfied by an event that arrived long before the one
 * that tipped the tree over. A step that reacts differently per outcome writes one {@code on(...)} branch per outcome
 * instead, ordered, first satisfied one winning, which says in the declaration what an inference in the reaction can only
 * guess at.
 *
 * @param <E> the domain event type
 */
public sealed interface StepCondition<E> permits StepCondition.AtLeast, StepCondition.AllOf, StepCondition.AnyOf {

    /**
     * An event of {@code eventType}, matched at the event level, and, if {@code predicate} is set, one that also
     * satisfies it. Nested inside {@link StepCondition} because it is not itself a condition. Lifting it to a
     * window-level count is what {@link AtLeast} does.
     *
     * @param eventType   the event type to match
     * @param predicate   an additional test the event must pass, or {@code null} to match on type alone. It must be a
     *                    deterministic function of the event it is given, so the same event yields the same answer every time,
     *                    because it is run again whenever a count has to be derived from the step window and a replay runs it
     *                    again from the start. Reading the clock, a random source, mutable state, or a remote service from a
     *                    predicate is unsupported and can both fire the wrong branch and make a replay diverge
     * @param predicateId a name for {@code predicate} that stays the same across restarts and recompilations, so a saga can
     *                    keep this leaf's count in its state instead of counting the step's events again. A lambda is a new
     *                    object every time the class loads and two different lambdas never compare equal, so without a name
     *                    there is nothing to tell a redeploy that changed a predicate from one that did not. Optional, and
     *                    only a leaf that has one can have its count kept, see {@code FlowSaga.Builder.stepWindow}. Change
     *                    it whenever the predicate's meaning changes
     * @param <E>         the domain event type
     */
    record EventMatcher<E>(Class<? extends E> eventType, @Nullable Predicate<E> predicate, @Nullable String predicateId) {
        public EventMatcher {
            requireNonNull(eventType, "eventType cannot be null");
            if (predicateId != null) {
                if (predicate == null) {
                    throw new IllegalArgumentException("predicateId '" + predicateId + "' names a predicate, so it cannot be given without one");
                }
                if (predicateId.isBlank()) {
                    throw new IllegalArgumentException("predicateId cannot be blank");
                }
            }
        }

        /** As {@link #EventMatcher(Class, Predicate, String)} with no name for the predicate. */
        public EventMatcher(Class<? extends E> eventType, @Nullable Predicate<E> predicate) {
            this(eventType, predicate, null);
        }

        /**
         * Whether {@code other} counts exactly the same events as this matcher. The predicate's name has no part in it,
         * since a name says which predicate this is rather than which events it accepts.
         */
        public boolean matchesTheSameEvents(EventMatcher<E> other) {
            requireNonNull(other, "other cannot be null");
            return eventType.equals(other.eventType) && Objects.equals(predicate, other.predicate);
        }
    }

    /**
     * Fulfilled once the step's window (the events received since it was entered) contains at least {@code count} events
     * matching {@code matcher}.
     *
     * @param matcher what an event must match
     * @param count   how many matching events are required, at least one
     * @param <E>     the domain event type
     */
    record AtLeast<E>(EventMatcher<E> matcher, int count) implements StepCondition<E> {
        public AtLeast {
            requireNonNull(matcher, "matcher cannot be null");
            if (count < 1) {
                throw new IllegalArgumentException("count must be at least 1, was " + count);
            }
        }
    }

    /**
     * Fulfilled once every one of {@code conditions} is. Built by {@link #allOf}, which normalizes it, rather than
     * constructed directly in ordinary use. Two children that match the same event are refused, see
     * {@link #allOf(StepCondition, StepCondition[])}.
     *
     * @param conditions the conditions that must all be fulfilled, at least one, no two of them matching the same event
     * @param <E>        the domain event type
     */
    record AllOf<E>(List<StepCondition<E>> conditions) implements StepCondition<E> {
        public AllOf {
            conditions = validateChildren(conditions, "allOf");
            rejectSharedRequirements(conditions);
        }
    }

    /**
     * Fulfilled once any one of {@code conditions} is. Built by {@link #anyOf}, which normalizes it, rather than
     * constructed directly in ordinary use.
     *
     * @param conditions the alternatives, at least one
     * @param <E>        the domain event type
     */
    record AnyOf<E>(List<StepCondition<E>> conditions) implements StepCondition<E> {
        public AnyOf {
            conditions = validateChildren(conditions, "anyOf");
        }
    }

    /** A leaf matching one event of {@code eventType}, with no further predicate. Shorthand for {@code event(eventType, 1)}. */
    static <E, T extends E> StepCondition<E> event(Class<T> eventType) {
        return event(eventType, 1, null);
    }

    /** A leaf matching {@code count} events of {@code eventType}, with no further predicate. */
    static <E, T extends E> StepCondition<E> event(Class<T> eventType, int count) {
        return event(eventType, count, null);
    }

    /** A leaf matching one event of {@code eventType} that also satisfies {@code predicate}. */
    static <E, T extends E> StepCondition<E> event(Class<T> eventType, Predicate<T> predicate) {
        return event(eventType, 1, requireNonNull(predicate, "predicate cannot be null"));
    }

    /**
     * A leaf matching {@code count} events of {@code eventType} that also satisfy {@code predicate}: the general form
     * every other {@code event(...)} overload shorthands.
     */
    @SuppressWarnings("unchecked") // The predicate only ever runs on an event that eventType.isInstance already accepted.
    static <E, T extends E> StepCondition<E> event(Class<T> eventType, int count, @Nullable Predicate<T> predicate) {
        requireNonNull(eventType, "eventType cannot be null");
        return new AtLeast<>(new EventMatcher<>(eventType, (Predicate<E>) predicate), count);
    }

    /**
     * A leaf matching one event of {@code eventType} that also satisfies {@code predicate}, with {@code predicateId}
     * naming that predicate. See {@link #event(Class, int, String, Predicate)}.
     */
    static <E, T extends E> StepCondition<E> event(Class<T> eventType, String predicateId, Predicate<T> predicate) {
        return event(eventType, 1, predicateId, predicate);
    }

    /**
     * A leaf matching {@code count} events of {@code eventType} that also satisfy {@code predicate}, with
     * {@code predicateId} naming that predicate so a saga can keep this leaf's count in its state rather than counting the
     * step's events again on every delivery.
     * <p>
     * Naming a predicate is what makes {@code FlowSaga.Builder.stepWindow} usable on the step, since dropping a step's older
     * events means its counts have to survive a redeploy, and a lambda is a different object every time the class loads.
     * Give the name the predicate's meaning rather than its wording, {@code "isBig"} rather than {@code "amountOver1000"},
     * and <b>change it whenever the meaning changes</b>. Keeping the name while changing what the predicate accepts is the
     * one thing this cannot detect, and an instance parked in that step then keeps counting events it matched under the old
     * test. Two leaves may share a name only when they are the same predicate value.
     *
     * @param eventType   the event type to match
     * @param count       how many matching events are required, at least one
     * @param predicateId a name for {@code predicate}, stable across restarts and recompilations, neither blank nor null
     * @param predicate   the test the event must pass, deterministic in the event alone, see {@link EventMatcher#predicate()}
     */
    @SuppressWarnings("unchecked") // The predicate only ever runs on an event that eventType.isInstance already accepted.
    static <E, T extends E> StepCondition<E> event(Class<T> eventType, int count, String predicateId, Predicate<T> predicate) {
        requireNonNull(eventType, "eventType cannot be null");
        requireNonNull(predicateId, "predicateId cannot be null");
        requireNonNull(predicate, "predicate cannot be null");
        return new AtLeast<>(new EventMatcher<>(eventType, (Predicate<E>) predicate, predicateId), count);
    }

    /**
     * A condition fulfilled once every one of {@code first} plus {@code rest} is. A nested {@code allOf(...)} among the
     * arguments flattens into this one, and a single argument (after flattening) is returned as-is rather than wrapped
     * in a one-element {@link AllOf}, the same normalization {@link #anyOf} applies.
     * <p>
     * <b>What is refused.</b> After flattening, two children are refused with {@link IllegalArgumentException} when some
     * {@link EventMatcher} is reachable from both, comparing matchers by equality. That covers a leaf pair
     * ({@code allOf(A.class, A.class)} reads as two {@code A} and is fulfilled by one, and {@code allOf(event(A, 2), event(A, 3))}
     * reads as five and is fulfilled by three, since each leaf counts over the same window independently and nothing consumes
     * an event), two equal children, and a matcher shared through a composite
     * ({@code allOf(event(A), anyOf(event(A), event(B)))} is fulfilled by one {@code A}). Ask for one
     * {@code event(type, count)} leaf with the total instead, or restructure so no two children match the same event. A tree
     * is normally built where a saga is declared, so this fails at startup rather than on a delivery.
     * <p>
     * <b>What is not refused, and why.</b> Two matchers that are unequal but can still be satisfied by one event are allowed
     * through, so the check is not a proof that children are distinct:
     * <ul>
     *   <li>a supertype and a subtype of it, {@code allOf(event(BaseEvent.class), event(A.class))}, because refusing it would
     *       refuse a legitimate "one event of any kind plus one {@code A} specifically".</li>
     *   <li>two leaves over one type whose predicates are separately written lambdas, because distinct lambdas never compare
     *       equal and nothing can tell a duplicate from two genuinely different tests.</li>
     * </ul>
     * <p>
     * {@code anyOf} deliberately permits a repeated alternative, because it is fulfilled by exactly what it says it wants, so
     * nothing about it reads as stronger than it is, and a tree assembled from data can hold a harmless duplicate there.
     * Do not "fix" that asymmetry for symmetry's sake, see ADR 120.
     */
    @SafeVarargs
    static <E> StepCondition<E> allOf(StepCondition<? extends E> first, StepCondition<? extends E>... rest) {
        requireNonNull(rest, "rest cannot be null");
        return combine(widenAll(first, rest), true);
    }

    /** As {@link #allOf(StepCondition, StepCondition[])}, from a collection of alternatives. */
    static <E> StepCondition<E> allOf(Collection<? extends StepCondition<? extends E>> conditions) {
        return combine(widenAll(conditions), true);
    }

    /**
     * Shorthand for {@code allOf(event(first), event(rest[0]), ...)}: every named type must arrive at least once, with
     * no predicate on any of them.
     */
    @SafeVarargs
    static <E> StepCondition<E> allOf(Class<? extends E> first, Class<? extends E>... rest) {
        requireNonNull(rest, "rest cannot be null");
        return combine(eventLeaves(first, rest), true);
    }

    /**
     * A condition fulfilled once any one of {@code first} plus {@code rest} is. A nested {@code anyOf(...)} among the
     * arguments flattens into this one, and a single argument (after flattening) is returned as-is rather than wrapped
     * in a one-element {@link AnyOf}.
     */
    @SafeVarargs
    static <E> StepCondition<E> anyOf(StepCondition<? extends E> first, StepCondition<? extends E>... rest) {
        requireNonNull(rest, "rest cannot be null");
        return combine(widenAll(first, rest), false);
    }

    /** As {@link #anyOf(StepCondition, StepCondition[])}, from a collection of alternatives. */
    static <E> StepCondition<E> anyOf(Collection<? extends StepCondition<? extends E>> conditions) {
        return combine(widenAll(conditions), false);
    }

    /**
     * Shorthand for {@code anyOf(event(first), event(rest[0]), ...)}: any one of the named types arriving once is
     * enough, with no predicate on any of them.
     */
    @SafeVarargs
    static <E> StepCondition<E> anyOf(Class<? extends E> first, Class<? extends E>... rest) {
        requireNonNull(rest, "rest cannot be null");
        return combine(eventLeaves(first, rest), false);
    }

    private static <E> List<StepCondition<E>> validateChildren(List<StepCondition<E>> conditions, String name) {
        requireNonNull(conditions, "conditions cannot be null");
        conditions.forEach(condition -> requireNonNull(condition, "condition cannot be null"));
        List<StepCondition<E>> copy = List.copyOf(conditions);
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("a " + name + " condition needs at least one condition");
        }
        return copy;
    }

    // An allOf child that asks for what a sibling already asks for makes the declaration read as stronger than it is, so
    // refuse it where the tree is built rather than let a step complete earlier than it says. Runs from AllOf's constructor,
    // so it covers the allOf(...) factories, a directly constructed AllOf, and a nested allOf that flattening lifted into
    // this level alike. See the allOf javadoc for the rule, the anyOf asymmetry and the limits of the check.
    private static <E> void rejectSharedRequirements(List<StepCondition<E>> conditions) {
        List<List<EventMatcher<E>>> perChild = new ArrayList<>();
        for (StepCondition<E> condition : conditions) {
            List<EventMatcher<E>> matchers = new ArrayList<>();
            StepConditionWalk.forEachLeafMatcher(condition, matchers::add);
            perChild.add(matchers);
        }
        for (int i = 0; i < conditions.size(); i++) {
            for (int j = i + 1; j < conditions.size(); j++) {
                EventMatcher<E> shared = firstShared(perChild.get(i), perChild.get(j));
                if (shared != null) {
                    String type = shared.eventType().getSimpleName();
                    throw new IllegalArgumentException("allOf children " + i + " and " + j + " can both be satisfied by the same "
                            + type + " event, so the condition is fulfilled sooner than it reads. Two children that match the"
                            + " same event are one requirement, not two. Ask for a single event(" + type + ", count) leaf with"
                            + " the total count you want, or restructure so no two children match the same event");
                }
            }
        }
    }

    // The first matcher two children have in common, or null when they share none. Two matchers are compared on the events
    // they accept and not on the name a predicate may carry, since a name says which predicate a leaf holds while this rule
    // is about one event satisfying two children. Composite children are searched through, since an allOf child can be a
    // whole anyOf subtree and one event reaching a leaf inside it satisfies that child too.
    private static <E> @Nullable EventMatcher<E> firstShared(List<EventMatcher<E>> earlier, List<EventMatcher<E>> later) {
        for (EventMatcher<E> matcher : earlier) {
            for (EventMatcher<E> candidate : later) {
                if (matcher.matchesTheSameEvents(candidate)) {
                    return matcher;
                }
            }
        }
        return null;
    }

    // Flattens a same-kind child one level (a child is already normalized, so this collapses a whole same-kind chain in
    // practice) and collapses a single remaining child to itself rather than wrapping it, the DcbCriteria.anyOf precedent.
    // Declaration order is preserved throughout and no element is deduplicated (an allOf instead refuses a child that
    // duplicates a sibling's requirement, see rejectSharedRequirements).
    private static <E> StepCondition<E> combine(List<StepCondition<E>> conditions, boolean allOf) {
        List<StepCondition<E>> flattened = new ArrayList<>();
        for (StepCondition<E> condition : conditions) {
            if (allOf && condition instanceof AllOf<E> nested) {
                flattened.addAll(nested.conditions());
            } else if (!allOf && condition instanceof AnyOf<E> nested) {
                flattened.addAll(nested.conditions());
            } else {
                flattened.add(condition);
            }
        }
        if (flattened.isEmpty()) {
            throw new IllegalArgumentException("a " + (allOf ? "allOf" : "anyOf") + " condition needs at least one condition");
        }
        return flattened.size() == 1 ? flattened.get(0) : (allOf ? new AllOf<>(flattened) : new AnyOf<>(flattened));
    }

    @SuppressWarnings("unchecked") // Safe: a StepCondition only ever reads an event through eventType.isInstance, never writes one.
    private static <E> StepCondition<E> widen(StepCondition<? extends E> condition) {
        requireNonNull(condition, "condition cannot be null");
        return (StepCondition<E>) condition;
    }

    private static <E> List<StepCondition<E>> widenAll(StepCondition<? extends E> first, StepCondition<? extends E>[] rest) {
        requireNonNull(first, "first cannot be null");
        requireNonNull(rest, "rest cannot be null");
        List<StepCondition<? extends E>> all = new ArrayList<>(rest.length + 1);
        all.add(first);
        all.addAll(Arrays.asList(rest));
        return widenAll(all);
    }

    private static <E> List<StepCondition<E>> widenAll(Collection<? extends StepCondition<? extends E>> conditions) {
        requireNonNull(conditions, "conditions cannot be null");
        List<StepCondition<E>> widened = new ArrayList<>();
        for (StepCondition<? extends E> condition : conditions) {
            widened.add(widen(condition));
        }
        return widened;
    }

    private static <E> List<StepCondition<E>> eventLeaves(Class<? extends E> first, Class<? extends E>[] rest) {
        requireNonNull(first, "first cannot be null");
        List<StepCondition<E>> leaves = new ArrayList<>();
        StepCondition<E> firstLeaf = event(first);
        leaves.add(firstLeaf);
        for (Class<? extends E> type : rest) {
            StepCondition<E> leaf = event(requireNonNull(type, "type cannot be null"));
            leaves.add(leaf);
        }
        return leaves;
    }
}

/**
 * Visits every leaf's event type in a {@link StepCondition} tree. Package-visible, not a public visitor in v1: the
 * flow builder's event-type collection is the only walk that needs one so far. An interface member cannot be
 * package-private, which is why this lives as a separate top-level class in the same file rather than on
 * {@link StepCondition} itself, the same split {@code ReceivedEventsList} uses beside {@link ReceivedEvents}.
 */
final class StepConditionWalk {

    private StepConditionWalk() {
    }

    /** Calls {@code action} with every leaf's event type, in declaration order, duplicates included. */
    static <E> void forEachLeafEventType(StepCondition<E> condition, Consumer<Class<? extends E>> action) {
        forEachLeafMatcher(condition, matcher -> action.accept(matcher.eventType()));
    }

    /** Calls {@code action} with every leaf's matcher, in declaration order, duplicates included. */
    static <E> void forEachLeafMatcher(StepCondition<E> condition, Consumer<StepCondition.EventMatcher<E>> action) {
        switch (condition) {
            case StepCondition.AtLeast<E> atLeast -> action.accept(atLeast.matcher());
            case StepCondition.AllOf<E> allOf -> allOf.conditions().forEach(child -> forEachLeafMatcher(child, action));
            case StepCondition.AnyOf<E> anyOf -> anyOf.conditions().forEach(child -> forEachLeafMatcher(child, action));
        }
    }
}
