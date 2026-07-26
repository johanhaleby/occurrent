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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.Branch;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.ChoiceBody;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.CompiledStep;
import org.occurrent.dsl.saga.flow.FlowSagaImpl.JoinBody;
import org.occurrent.dsl.saga.internal.TypeDispatch;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Builds a flow saga: a linear, declarative process of steps, branches, joins and timeouts, which compiles down to the
 * core {@code Saga<E, FlowState<E>, C>} the executor runs. Use it for the common case where a process moves
 * through a small number of named steps. Drop to {@code Saga.builder(...)} for anything the flow model cannot express
 * (dynamic joins, accumulators, an event valid in every step).
 * <p>
 * Kotlin has an equivalent {@code saga { }} block, see {@code SagaFlowExtensions.kt}.
 */
public final class FlowSaga {

    private FlowSaga() {
    }

    /** Starts building a flow saga over event type {@code E} issuing command type {@code C}. */
    public static <E, C> Builder<E, C> builder() {
        return new Builder<>();
    }

    /**
     * Assembles a flow saga. Not thread-safe, configure it and call {@link #build()} once. {@code build()} validates the
     * whole step graph: {@code startsOn} is required, every step name is unique, every {@code transitionTo} target exists, and
     * every referenced event type has a correlation.
     *
     * @param <E> the domain event type
     * @param <C> the command type
     */
    public static final class Builder<E, C> {
        private @Nullable Class<? extends E> startType;
        private Function<E, List<C>> onStartCommands = event -> List.of();
        private final Map<Class<?>, Function<E, @Nullable String>> correlators = new LinkedHashMap<>();
        private @Nullable Function<E, @Nullable String> correlateAll;
        private final List<CompiledStep<E, C>> steps = new ArrayList<>();
        private final Set<String> stepNames = new LinkedHashSet<>();
        private int historyWindow = FlowSagaImpl.DEFAULT_HISTORY_WINDOW;

        private Builder() {
        }

        /**
         * Sets how many received events are retained behind the current step's entry, and so how far back a guard, a join
         * reaction, or a timeout reaction can read history through {@link ReceivedEvents}. The initiating event and the
         * current step's own events are always retained on top of this (a join must count over the current step's events),
         * so this only bounds the earlier history. The default is {@value FlowSagaImpl#DEFAULT_HISTORY_WINDOW}. Raise it for
         * a guard that counts far across a self-looping step (for example a retry cap higher than the default); lower it to
         * trim the persisted state of a long-running instance. Must be at least zero.
         */
        public Builder<E, C> historyWindow(int events) {
            if (events < 0) {
                throw new IllegalArgumentException("historyWindow cannot be negative");
            }
            this.historyWindow = events;
            return this;
        }

        /**
         * Declares the event that starts an instance. Required, can be set only once. Correlate it with
         * {@link #correlate} or {@link #correlateAll} like any other event type, {@code build()} fails if neither
         * covers it.
         */
        public <T extends E> Builder<E, C> startsOn(Class<T> type) {
            return startsOn(type, event -> List.of());
        }

        /**
         * As {@link #startsOn(Class)}, plus the commands to issue when the instance starts.
         */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, C> startsOn(Class<T> type, Function<T, List<C>> onStart) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(onStart, "onStart cannot be null");
            if (startType != null) {
                throw new IllegalStateException("startsOn(...) has already been set and can only be set once");
            }
            startType = type;
            onStartCommands = (Function<E, List<C>>) onStart;
            return this;
        }

        /** Registers how to correlate an event of {@code type} to a saga instance. Registering the same type twice throws. */
        @SuppressWarnings("unchecked")
        public <T extends E> Builder<E, C> correlate(Class<T> type, Function<T, String> correlatedBy) {
            requireNonNull(type, "type cannot be null");
            requireNonNull(correlatedBy, "correlatedBy cannot be null");
            if (correlators.containsKey(type)) {
                throw new IllegalStateException("correlate(...) has already been registered for " + type.getName());
            }
            correlators.put(type, (Function<E, @Nullable String>) correlatedBy);
            return this;
        }

        /**
         * Registers a fallback correlation used for any event type without its own {@link #correlate}. The common case
         * is a sealed event hierarchy exposing a shared id, for example {@code correlateAll(OrderEvent::orderId)}. Can
         * be set only once.
         */
        public Builder<E, C> correlateAll(Function<E, String> correlatedBy) {
            if (this.correlateAll != null) {
                throw new IllegalStateException("correlateAll(...) has already been set and can only be set once");
            }
            this.correlateAll = requireNonNull(correlatedBy, "correlatedBy cannot be null");
            return this;
        }

        /** Adds a step named {@code name}, configured by {@code block}. Step names must be unique and non-blank. */
        public Builder<E, C> step(String name, Consumer<StepBuilder<E, C>> block) {
            requireNonNull(name, "name cannot be null");
            requireNonNull(block, "block cannot be null");
            if (name.isBlank()) {
                throw new IllegalArgumentException("step name cannot be blank");
            }
            if (!stepNames.add(name)) {
                throw new IllegalStateException("step '" + name + "' has already been defined; step names must be unique");
            }
            StepBuilder<E, C> stepBuilder = new StepBuilder<>(name);
            block.accept(stepBuilder);
            steps.add(stepBuilder.compile());
            return this;
        }

        /** Builds and validates the flow saga. */
        public Saga<E, FlowState<E>, C> build() {
            if (startType == null) {
                throw new IllegalStateException("a flow saga needs startsOn(...), call it before build()");
            }
            if (steps.isEmpty()) {
                throw new IllegalStateException("a flow saga needs at least one step(...)");
            }

            Map<String, Integer> stepIndex = new LinkedHashMap<>();
            Map<String, CompiledStep<E, C>> stepsByName = new LinkedHashMap<>();
            for (int i = 0; i < steps.size(); i++) {
                CompiledStep<E, C> step = steps.get(i);
                stepIndex.put(step.name(), i);
                stepsByName.put(step.name(), step);
            }

            validateTransitionToTargets(stepsByName.keySet());
            Set<Class<? extends E>> eventTypes = collectEventTypes();
            validateCorrelationCoverage(eventTypes);

            return new FlowSagaImpl<>(startType, onStartCommands, List.copyOf(steps), stepIndex, stepsByName,
                    correlators, correlateAll, Set.of(startType), eventTypes, historyWindow);
        }

        private void validateTransitionToTargets(Set<String> stepNamesInGraph) {
            for (CompiledStep<E, C> step : steps) {
                for (Continuation continuation : continuationsOf(step)) {
                    if (continuation instanceof Continuation.TransitionTo transitionTo && !stepNamesInGraph.contains(transitionTo.stepName())) {
                        throw new IllegalStateException("step '" + step.name() + "' has transitionTo(\"" + transitionTo.stepName()
                                + "\") but no such step is defined");
                    }
                }
            }
        }

        private List<Continuation> continuationsOf(CompiledStep<E, C> step) {
            List<Continuation> continuations = new ArrayList<>();
            switch (step.body()) {
                case ChoiceBody<E, C> choice -> choice.branches().forEach(branch -> continuations.add(branch.then()));
                case JoinBody<E, C> join -> continuations.add(join.then());
            }
            if (step.timeout() != null) {
                continuations.add(step.timeout().then());
            }
            return continuations;
        }

        private Set<Class<? extends E>> collectEventTypes() {
            Set<Class<? extends E>> types = new LinkedHashSet<>();
            if (startType != null) {
                types.add(startType);
            }
            for (CompiledStep<E, C> step : steps) {
                switch (step.body()) {
                    case ChoiceBody<E, C> choice -> choice.branches().forEach(branch -> types.add(branch.eventType()));
                    case JoinBody<E, C> join -> join.expectations().forEach(expectation -> types.add(expectation.eventType()));
                }
            }
            return Set.copyOf(types);
        }

        private void validateCorrelationCoverage(Set<Class<? extends E>> eventTypes) {
            if (correlateAll != null) {
                // A single fallback correlates every event type, so per-type coverage is not required.
                return;
            }
            TypeDispatch<Function<E, @Nullable String>> coverage =
                    new TypeDispatch<>(correlators);
            for (Class<? extends E> type : eventTypes) {
                if (coverage.resolve(type) == null) {
                    throw new IllegalStateException("event type " + type.getName() + " is used by a step but has no correlation, "
                            + "register correlate(" + type.getSimpleName() + ".class, ...) or add a correlateAll(...) fallback");
                }
            }
        }
    }
}
