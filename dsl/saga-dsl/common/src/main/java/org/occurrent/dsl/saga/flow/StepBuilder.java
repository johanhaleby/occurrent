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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Configures one step of a {@link FlowSaga}, one or more {@code on(...)} branches, evaluated in declaration order with the
 * first satisfied one winning, or the deprecated {@code join(...)} sugar instead of {@code on(...)}, never both, plus at
 * most one {@code timeout(...)}. Violating either throws {@link IllegalStateException}.
 * <p>
 * A branch is either a classic type match, {@code on(Class, ...)}, firing on a matching arriving event, or a window
 * condition, {@code on(StepCondition, ...)}, firing once the events received since the step was entered satisfy a
 * {@link StepCondition} tree. The two mix freely in the same step's branch list. A mixed step is a single ordered list, not
 * two separate mechanisms layered on top of each other.
 *
 * @param <E> the domain event type
 * @param <C> the command type
 */
public final class StepBuilder<E, C> {
    private final String stepName;
    private final List<Branch<E, C>> branches = new ArrayList<>();
    private boolean joinDeclared = false;
    private @Nullable TimeoutSpec<E, C> timeout;

    StepBuilder(String stepName) {
        this.stepName = stepName;
    }

    /** Adds a branch: on an event of {@code type}, run {@code commands} and follow {@code then}. First matching branch wins. */
    public <T extends E> StepBuilder<E, C> on(Class<T> type, Continuation then, Function<T, List<C>> commands) {
        return on(type, (BiPredicate<T, ReceivedEvents<E>>) null, then, commands);
    }

    /** Adds a branch that issues no commands: on an event of {@code type}, just follow {@code then}. */
    public <T extends E> StepBuilder<E, C> on(Class<T> type, Continuation then) {
        return on(type, then, (T event) -> List.of());
    }

    /** Adds a guarded branch: it matches only when {@code onlyIf} is also true for the event and events received so far. */
    public <T extends E> StepBuilder<E, C> on(Class<T> type, @Nullable BiPredicate<T, ReceivedEvents<E>> onlyIf, Continuation then, Function<T, List<C>> commands) {
        requireNonNull(commands, "commands cannot be null");
        return on(type, onlyIf, then, (metadata, event) -> commands.apply(event));
    }

    /** Adds a guarded branch that issues no commands, following {@code then} only when {@code onlyIf} matches. */
    public <T extends E> StepBuilder<E, C> on(Class<T> type, @Nullable BiPredicate<T, ReceivedEvents<E>> onlyIf, Continuation then) {
        return on(type, onlyIf, then, (T event) -> List.of());
    }

    /**
     * Adds a branch whose commands also receive the triggering event's delivery {@link EventMetadata} (stream id and
     * version, global position, CloudEvent extensions). The metadata-first sibling of {@link #on(Class, Continuation, Function)}.
     */
    public <T extends E> StepBuilder<E, C> on(Class<T> type, Continuation then, BiFunction<EventMetadata, T, List<C>> commands) {
        return on(type, (BiPredicate<T, ReceivedEvents<E>>) null, then, commands);
    }

    /** Adds a guarded, metadata-carrying branch: the metadata-first sibling of {@link #on(Class, BiPredicate, Continuation, Function)}. */
    @SuppressWarnings("unchecked")
    public <T extends E> StepBuilder<E, C> on(Class<T> type, @Nullable BiPredicate<T, ReceivedEvents<E>> onlyIf, Continuation then, BiFunction<EventMetadata, T, List<C>> commands) {
        requireNonNull(type, "type cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(commands, "commands cannot be null");
        requireNoJoin();
        ArrivingEvent<E> trigger = new ArrivingEvent<>(type, (BiPredicate<E, ReceivedEvents<E>>) onlyIf);
        BranchReaction<E, C> reaction = (metadata, triggering, received) -> commands.apply(metadata, (T) triggering);
        branches.add(new Branch<>(trigger, reaction, then));
        return this;
    }

    /**
     * Adds a branch that follows {@code then} once the events received since the step was entered satisfy
     * {@code condition}. Issues no commands. {@code condition} takes {@code StepCondition<? extends E>}, not the
     * invariant {@code StepCondition<E>}, so a tree built by combining leaves of different concrete event types (its
     * inferred type is their least upper bound) is accepted without a cast, see {@link StepCondition}'s javadoc.
     * <p>
     * The window a condition counts over opens when the step is entered, and <em>every</em> transition into a step resets it,
     * a {@code transitionTo} naming the step it is already in included. So a self-loop restarts every window in this step,
     * and in a mixed step a classic branch that self-loops wipes a sibling condition's partial count, the same way
     * it already wipes a {@code join}'s. That is deliberate, since the step has been re-entered and its waiting starts over.
     */
    public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then) {
        return on(condition, then, received -> List.of());
    }

    /**
     * As {@link #on(StepCondition, Continuation)}, plus the commands to issue once {@code condition} is satisfied.
     * {@code whenFulfilled} reads {@link ReceivedEvents}, not the triggering event alone. The tipping event, whichever
     * leaf it matched, is always {@code received.asList().getLast()}.
     * <p>
     * {@code whenFulfilled} reads the same window the condition was evaluated over, the events received since this step was
     * entered, so a count it takes is the count that fulfilled the condition and an event left over from an earlier step
     * cannot be mistaken for one of this step's. {@code received.initiating()} reaches past the window as always. A guard and
     * a {@code timeout}'s {@code onExpiry} read the whole retained history instead, which is what makes a count spanning
     * several steps possible there.
     * <p>
     * The flow builder's {@code stepWindow} is the one thing that narrows this. It caps how many of the step's events the
     * instance keeps, so a condition still fires on the same event, and {@code whenFulfilled} then reads only the events
     * still kept, the tipping one among them.
     */
    @SuppressWarnings("unchecked") // Safe. A StepCondition only ever reads an event through eventType.isInstance, never writes one.
    public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then, Function<ReceivedEvents<E>, List<C>> whenFulfilled) {
        requireNonNull(condition, "condition cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(whenFulfilled, "whenFulfilled cannot be null");
        requireNoJoin();
        WindowCondition<E> trigger = new WindowCondition<>((StepCondition<E>) condition, false);
        BranchReaction<E, C> reaction = (metadata, triggering, received) -> whenFulfilled.apply(received);
        branches.add(new Branch<>(trigger, reaction, then));
        return this;
    }

    /**
     * Makes this a join step that waits until all {@code expecting} are met (counted since the step was entered), then
     * runs {@code whenFulfilled} and follows {@code then}.
     *
     * @deprecated in favor of {@code on(allOf(...))}, which this now lowers to internally. An expectation of {@code n}
     * events of a type becomes {@code event(type, n)}, and the whole list becomes one {@code allOf(...)} tree. Two
     * expectations naming the same type collapse to the higher of their counts, which is what such a join has always meant,
     * since each expectation is checked against the same window independently. Behavior is unchanged, only the way you write
     * it. See {@link StepCondition}.
     */
    @Deprecated
    public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then, Function<ReceivedEvents<E>, List<C>> whenFulfilled) {
        requireNonNull(expecting, "expecting cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(whenFulfilled, "whenFulfilled cannot be null");
        if (!branches.isEmpty()) {
            throw new IllegalStateException("step '" + stepName + "' has on(...) branches and cannot also be a join step");
        }
        if (joinDeclared) {
            throw new IllegalStateException("join(...) has already been set for step '" + stepName + "' and can only be set once");
        }
        if (expecting.isEmpty()) {
            throw new IllegalArgumentException("a join step needs at least one expectation");
        }
        joinDeclared = true;
        // Marked as lowered from join, so the reaction keeps reading the whole retained history. join shipped in 0.31.0 with
        // that contract and lowering it to a condition tree must not change what its callback sees.
        WindowCondition<E> trigger = new WindowCondition<>(StepCondition.allOf(toConditions(expecting)), true);
        BranchReaction<E, C> reaction = (metadata, triggering, received) -> whenFulfilled.apply(received);
        branches.add(new Branch<>(trigger, reaction, then));
        return this;
    }

    /**
     * As {@link #join(List, Continuation, Function)}, but issues no commands when fulfilled.
     *
     * @deprecated in favor of {@code on(allOf(...))}, see {@link #join(List, Continuation, Function)}.
     */
    @Deprecated
    public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then) {
        return join(expecting, then, events -> List.of());
    }

    // Collapse expectations naming the same type to the highest count asked for, before the tree is built. allOf refuses two
    // children that match the same events, and a join is allowed to carry them, because join(expect(A, 2), expect(A, 3))
    // already means "at least three A" and inheriting allOf's rejection would turn a working declaration in a shipped,
    // deprecated API into a startup failure on a patch upgrade. Declaration order follows each type's first appearance.
    private static <E> List<StepCondition<E>> toConditions(List<Expectation<E>> expecting) {
        Map<Class<? extends E>, Integer> highestCount = new LinkedHashMap<>();
        for (Expectation<E> expectation : expecting) {
            highestCount.merge(expectation.eventType(), expectation.count(), Math::max);
        }
        List<StepCondition<E>> conditions = new ArrayList<>();
        for (Map.Entry<Class<? extends E>, Integer> entry : highestCount.entrySet()) {
            conditions.add(StepCondition.event(entry.getKey(), entry.getValue()));
        }
        return conditions;
    }

    private void requireNoJoin() {
        if (joinDeclared) {
            throw new IllegalStateException("step '" + stepName + "' is a join step and cannot also have on(...) branches");
        }
    }

    /** Sets a relative timeout: if it fires before the step completes, run {@code onExpiry} and follow {@code then}. */
    public StepBuilder<E, C> timeout(Duration after, Continuation then, Function<ReceivedEvents<E>, List<C>> onExpiry) {
        requireNonNull(after, "after cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(onExpiry, "onExpiry cannot be null");
        setTimeout(new TimeoutSpec<>(after, null, onExpiry, then));
        return this;
    }

    /** As {@link #timeout(Duration, Continuation, Function)}, but issues no commands on expiry. */
    public StepBuilder<E, C> timeout(Duration after, Continuation then) {
        return timeout(after, then, events -> List.of());
    }

    /** Sets an absolute, data-derived timeout: {@code at} is computed from the events received so far when the step is entered. */
    public StepBuilder<E, C> timeout(Function<ReceivedEvents<E>, Instant> at, Continuation then, Function<ReceivedEvents<E>, List<C>> onExpiry) {
        requireNonNull(at, "at cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(onExpiry, "onExpiry cannot be null");
        setTimeout(new TimeoutSpec<>(null, at, onExpiry, then));
        return this;
    }

    /** As {@link #timeout(Function, Continuation, Function)}, but issues no commands on expiry. */
    public StepBuilder<E, C> timeout(Function<ReceivedEvents<E>, Instant> at, Continuation then) {
        return timeout(at, then, events -> List.of());
    }

    private void setTimeout(TimeoutSpec<E, C> spec) {
        if (timeout != null) {
            throw new IllegalStateException("timeout(...) has already been set for step '" + stepName + "' and can only be set once");
        }
        timeout = spec;
    }

    CompiledStep<E, C> compile() {
        if (branches.isEmpty()) {
            throw new IllegalStateException("step '" + stepName + "' needs at least one on(...) branch or a join(...)");
        }
        List<Branch<E, C>> compiled = List.copyOf(branches);
        return new CompiledStep<>(stepName, compiled, timeout, StepLeaves.of(stepName, compiled));
    }
}
