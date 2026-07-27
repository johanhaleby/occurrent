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
import org.occurrent.dsl.saga.flow.FlowSagaImpl.Branch;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.ChoiceBody;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.CompiledStep;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.JoinBody;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.StepBody;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.TimeoutSpec;
import org.occurrent.cloudevents.EventMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Configures one step of a {@link FlowSaga}: either a choice (one or more {@code on(...)} branches, first match wins) or a
 * single {@code join(...)}, optionally with a {@code timeout(...)}. A step cannot be both a choice and a join, and can
 * have at most one timeout. Violating either throws {@link IllegalStateException}.
 *
 * @param <E> the domain event type
 * @param <C> the command type
 */
public final class StepBuilder<E, C> {
    private final String stepName;
    private final List<Branch<E, C>> branches = new ArrayList<>();
    private @Nullable JoinBody<E, C> join;
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
        if (join != null) {
            throw new IllegalStateException("step '" + stepName + "' is a join step and cannot also have on(...) branches");
        }
        branches.add(new Branch<>(type, (BiPredicate<E, ReceivedEvents<E>>) onlyIf, (BiFunction<EventMetadata, E, List<C>>) commands, then));
        return this;
    }

    /** Makes this a join step: wait until all {@code expecting} are met (counted since the step was entered), then run {@code whenFulfilled} and follow {@code then}. */
    public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then, Function<ReceivedEvents<E>, List<C>> whenFulfilled) {
        requireNonNull(expecting, "expecting cannot be null");
        requireNonNull(then, "then cannot be null");
        requireNonNull(whenFulfilled, "whenFulfilled cannot be null");
        if (!branches.isEmpty()) {
            throw new IllegalStateException("step '" + stepName + "' has on(...) branches and cannot also be a join step");
        }
        if (join != null) {
            throw new IllegalStateException("join(...) has already been set for step '" + stepName + "' and can only be set once");
        }
        if (expecting.isEmpty()) {
            throw new IllegalArgumentException("a join step needs at least one expectation");
        }
        join = new JoinBody<>(List.copyOf(expecting), whenFulfilled, then);
        return this;
    }

    /** As {@link #join(List, Continuation, Function)}, but issues no commands when fulfilled. */
    public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then) {
        return join(expecting, then, events -> List.of());
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
        if (branches.isEmpty() && join == null) {
            throw new IllegalStateException("step '" + stepName + "' needs at least one on(...) branch or a join(...)");
        }
        StepBody<E, C> body = join != null ? join : new ChoiceBody<>(List.copyOf(branches));
        return new CompiledStep<>(stepName, body, timeout);
    }
}
