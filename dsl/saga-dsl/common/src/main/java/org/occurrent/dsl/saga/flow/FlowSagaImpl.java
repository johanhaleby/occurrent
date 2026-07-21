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
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.flow.FlowState.ActionKind;
import org.occurrent.dsl.saga.internal.TypeDispatch;
import org.occurrent.dsl.subscription.EventMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Compiles a flow definition (steps, branches, joins, timeouts) down to the machine-core {@link Saga} the executor runs.
 * The step name is the persisted position, a timer is named {@code "step:" + stepName}, and because a timer lives only in
 * the saga's own state envelope (there is exactly one per name), a re-armed timer replaces the previous one and no
 * separate fencing is needed here.
 */
final class FlowSagaImpl<E, C> implements Saga<E, FlowState<E>, C> {

    static final String TIMER_PREFIX = "step:";

    /**
     * Default number of received events kept behind the current step's entry, on top of the initiating event and the
     * current step's own events (which are always retained because a join counts over them). 100 comfortably covers a
     * retry loop or a guard that looks a few steps back, while keeping a long-running instance's state bounded.
     */
    static final int DEFAULT_HISTORY_WINDOW = 100;

    private final Class<? extends E> startType;
    private final Function<E, List<C>> onStartCommands;
    private final List<CompiledStep<E, C>> steps;
    private final Map<String, Integer> stepIndex;
    private final Map<String, CompiledStep<E, C>> stepsByName;
    private final TypeDispatch<Function<E, @Nullable String>> correlators;
    private final Set<Class<? extends E>> startEventTypes;
    private final Set<Class<? extends E>> eventTypes;
    // Carry-over: how many received events before the current step's entry are retained (and so visible to guards and
    // reactions). The current step's own events are always kept regardless, since a join must count over them.
    private final int historyWindow;

    FlowSagaImpl(Class<? extends E> startType,
                 Function<E, List<C>> onStartCommands,
                 List<CompiledStep<E, C>> steps,
                 Map<String, Integer> stepIndex,
                 Map<String, CompiledStep<E, C>> stepsByName,
                 Map<Class<?>, Function<E, @Nullable String>> correlators,
                 Set<Class<? extends E>> startEventTypes,
                 Set<Class<? extends E>> eventTypes,
                 int historyWindow) {
        this.startType = startType;
        this.onStartCommands = onStartCommands;
        this.steps = steps;
        this.stepIndex = stepIndex;
        this.stepsByName = stepsByName;
        this.correlators = new TypeDispatch<>(correlators);
        this.startEventTypes = startEventTypes;
        this.eventTypes = eventTypes;
        this.historyWindow = historyWindow;
    }

    @Override
    public FlowState<E> initialState() {
        return FlowState.initial();
    }

    @Override
    public @Nullable String sagaId(E event) {
        Function<E, @Nullable String> correlator = correlators.resolve(event.getClass());
        return correlator == null ? null : correlator.apply(event);
    }

    @Override
    public Set<Class<? extends E>> startEventTypes() {
        return startEventTypes;
    }

    @Override
    public Set<Class<? extends E>> eventTypes() {
        return eventTypes;
    }

    @Override
    public boolean isTerminal(FlowState<E> state) {
        return state.completed();
    }

    @Override
    public FlowState<E> evolve(FlowState<E> state, SagaInput<E> input) {
        return switch (input) {
            case SagaInput.Event<E> ev -> evolveOnEvent(state, ev.event());
            case SagaInput.Timeout<E> to -> evolveOnTimeout(state, to.timeout().timerName());
        };
    }

    private FlowState<E> evolveOnEvent(FlowState<E> state, E event) {
        if (!state.completed() && state.currentStep() == null) {
            // Instance creation: the start event enters the first step, its window opens after the start event itself. The
            // start event is received.get(0) and is always retained; the retained tail begins at absolute position 1.
            if (!startType.isInstance(event)) {
                return state;
            }
            String first = steps.get(0).name();
            // No react on the start event itself: onStart carries the instance-creation effects.
            return new FlowState<>(first, List.of(event), 1, 1, false, null, ActionKind.NONE, -1);
        }
        if (state.completed() || state.currentStep() == null) {
            return state;
        }
        List<E> received = append(state.received(), event);
        CompiledStep<E, C> step = stepsByName.get(state.currentStep());
        return switch (step.body()) {
            case ChoiceBody<E, C> choice -> {
                ReceivedEvents<E> receivedEvents = ReceivedEvents.of(received);
                for (int i = 0; i < choice.branches().size(); i++) {
                    Branch<E, C> branch = choice.branches().get(i);
                    if (branch.eventType().isInstance(event) && (branch.guard() == null || branch.guard().test(event, receivedEvents))) {
                        yield applyTransition(state, branch.then(), received, ActionKind.BRANCH, i);
                    }
                }
                yield withClearedBookkeeping(state, received);
            }
            case JoinBody<E, C> join -> {
                if (joinFulfilled(join.expectations(), received, joinWindowStart(state))) {
                    yield applyTransition(state, join.then(), received, ActionKind.JOIN, -1);
                }
                yield withClearedBookkeeping(state, received);
            }
        };
    }

    private FlowState<E> evolveOnTimeout(FlowState<E> state, String timerName) {
        if (state.completed() || state.currentStep() == null) {
            return withClearedBookkeeping(state, state.received());
        }
        String expected = TIMER_PREFIX + state.currentStep();
        if (!timerName.equals(expected)) {
            return withClearedBookkeeping(state, state.received());
        }
        CompiledStep<E, C> step = stepsByName.get(state.currentStep());
        if (step.timeout() == null) {
            return withClearedBookkeeping(state, state.received());
        }
        return applyTransition(state, step.timeout().then(), state.received(), ActionKind.TIMEOUT, -1);
    }

    // Stay in the current step with no transition: keep the given received log but reset the evolve-to-react bookkeeping,
    // so react (which routes on lastAction) does nothing rather than re-running a previous transition's reaction. Used
    // both when an event matches no branch / does not fulfil a join, and on an ignored timeout. windowStart and
    // stepEntryIndex are preserved: no transition happened, so the current step's window is unchanged and its accumulating
    // events must not be dropped (a join counts over them).
    private FlowState<E> withClearedBookkeeping(FlowState<E> state, List<E> received) {
        return new FlowState<>(state.currentStep(), received, state.windowStart(), state.stepEntryIndex(), state.completed(),
                state.currentStep(), ActionKind.NONE, -1);
    }

    // The relative index into the retained received list where the current step's join window begins. received.get(0) is
    // the pinned initiating event, so absolute position p maps to relative index p - windowStart + 1.
    private static int joinWindowStart(FlowState<?> state) {
        return state.stepEntryIndex() - state.windowStart() + 1;
    }

    private FlowState<E> applyTransition(FlowState<E> from, Continuation continuation, List<E> received, ActionKind kind, int branchIndex) {
        // The new step is entered after every event received so far, so its entry is the absolute event count. received holds
        // the initiating event (position 0) plus the tail starting at windowStart, so that count is windowStart plus the tail
        // length, i.e. windowStart + (received.size() - 1). When nothing has been dropped (windowStart == 1) this is exactly
        // received.size(), matching the pre-windowing behaviour.
        int newStepEntry = from.windowStart() + received.size() - 1;
        // Bound the retained history: drop received events older than historyWindow behind the step we are leaving. Anchoring
        // on the step we leave (not the one we enter) guarantees that step's own events survive for its reaction to read;
        // historyWindow adds earlier events on top for guards that look further back. windowStart only ever advances.
        int newWindowStart = Math.max(from.windowStart(), from.stepEntryIndex() - historyWindow);
        List<E> retained = retain(received, from.windowStart(), newWindowStart);
        String fromStep = from.currentStep();
        return switch (continuation) {
            case Continuation.Next ignored -> {
                int next = stepIndex.get(fromStep) + 1;
                if (next < steps.size()) {
                    yield new FlowState<>(steps.get(next).name(), retained, newWindowStart, newStepEntry, false, fromStep, kind, branchIndex);
                }
                yield new FlowState<>(null, retained, newWindowStart, newStepEntry, true, fromStep, kind, branchIndex);
            }
            case Continuation.GoTo goTo -> new FlowState<>(goTo.stepName(), retained, newWindowStart, newStepEntry, false, fromStep, kind, branchIndex);
            case Continuation.End ignored -> new FlowState<>(null, retained, newWindowStart, newStepEntry, true, fromStep, kind, branchIndex);
        };
    }

    // Drop the retained-tail events older than newWindowStart, keeping the pinned initiating event (received.get(0)). The
    // tail element at relative index 1 is at absolute position oldWindowStart, so advancing windowStart by n drops the
    // first n tail elements. newWindowStart >= oldWindowStart always, so this only ever shrinks the tail.
    private static <E> List<E> retain(List<E> received, int oldWindowStart, int newWindowStart) {
        int drop = newWindowStart - oldWindowStart;
        if (drop <= 0) {
            return received;
        }
        List<E> retained = new ArrayList<>(received.size() - drop);
        retained.add(received.get(0));
        retained.addAll(received.subList(1 + drop, received.size()));
        return retained;
    }

    @Override
    public List<SagaEffect<C>> onStart(FlowState<E> state, EventMetadata metadata, E startEvent) {
        List<SagaEffect<C>> effects = new ArrayList<>();
        for (C command : onStartCommands.apply(startEvent)) {
            effects.add(SagaEffect.issue(command));
        }
        armTimeoutIfAny(effects, state.currentStep(), ReceivedEvents.of(state.received()));
        return effects;
    }

    @Override
    public List<SagaEffect<C>> react(FlowState<E> state, SagaInput<E> input) {
        return switch (state.lastAction()) {
            case NONE -> List.of();
            case BRANCH -> reactToBranch(state, input);
            case JOIN -> reactToJoin(state);
            case TIMEOUT -> reactToTimeout(state);
        };
    }

    private List<SagaEffect<C>> reactToBranch(FlowState<E> state, SagaInput<E> input) {
        CompiledStep<E, C> from = stepsByName.get(state.previousStep());
        ChoiceBody<E, C> choice = (ChoiceBody<E, C>) from.body();
        Branch<E, C> branch = choice.branches().get(state.matchedBranchIndex());
        SagaInput.Event<E> triggering = (SagaInput.Event<E>) input;
        List<SagaEffect<C>> effects = issueAll(branch.commands().apply(triggering.metadata(), triggering.event()));
        retargetTimers(effects, state, false);
        return effects;
    }

    private List<SagaEffect<C>> reactToJoin(FlowState<E> state) {
        CompiledStep<E, C> from = stepsByName.get(state.previousStep());
        JoinBody<E, C> join = (JoinBody<E, C>) from.body();
        List<SagaEffect<C>> effects = issueAll(join.whenFulfilled().apply(ReceivedEvents.of(state.received())));
        retargetTimers(effects, state, false);
        return effects;
    }

    private List<SagaEffect<C>> reactToTimeout(FlowState<E> state) {
        CompiledStep<E, C> from = stepsByName.get(state.previousStep());
        TimeoutSpec<E, C> timeout = from.timeout();
        List<SagaEffect<C>> effects = issueAll(timeout.onExpiry().apply(ReceivedEvents.of(state.received())));
        retargetTimers(effects, state, true);
        return effects;
    }

    /**
     * Emit the timer effects for a transition: cancel the timer of the step we left (unless it just fired, since the
     * executor already consumed it and re-cancelling would only add a redundant no-op to the effect list), and arm the
     * timer of the step we entered. A self-loop cancels then re-arms the same timer, which the executor applies in order.
     */
    private void retargetTimers(List<SagaEffect<C>> effects, FlowState<E> state, boolean firedFromTimer) {
        String fromStep = state.previousStep();
        if (!firedFromTimer && fromStep != null) {
            CompiledStep<E, C> from = stepsByName.get(fromStep);
            if (from.timeout() != null) {
                effects.add(SagaEffect.cancelTimeout(TIMER_PREFIX + fromStep));
            }
        }
        armTimeoutIfAny(effects, state.currentStep(), ReceivedEvents.of(state.received()));
    }

    private void armTimeoutIfAny(List<SagaEffect<C>> effects, @Nullable String stepName, ReceivedEvents<E> received) {
        if (stepName == null) {
            return;
        }
        CompiledStep<E, C> step = stepsByName.get(stepName);
        TimeoutSpec<E, C> timeout = step.timeout();
        if (timeout == null) {
            return;
        }
        String timerName = TIMER_PREFIX + stepName;
        if (timeout.after() != null) {
            effects.add(SagaEffect.startTimeout(timerName, timeout.after()));
        } else {
            effects.add(SagaEffect.startTimeoutAt(timerName, timeout.at().apply(received)));
        }
    }

    private List<SagaEffect<C>> issueAll(List<C> commands) {
        List<SagaEffect<C>> effects = new ArrayList<>();
        for (C command : commands) {
            effects.add(SagaEffect.issue(command));
        }
        return effects;
    }

    private static boolean joinFulfilled(List<? extends Expectation<?>> expectations, List<?> received, int windowStart) {
        List<?> window = received.subList(windowStart, received.size());
        for (Expectation<?> expectation : expectations) {
            int count = 0;
            for (Object event : window) {
                if (expectation.eventType().isInstance(event)) {
                    count++;
                }
            }
            if (count < expectation.count()) {
                return false;
            }
        }
        return true;
    }

    private static <E> List<E> append(List<E> received, E event) {
        // FlowState's constructor makes the immutable copy, so build a single sized ArrayList here rather than copying twice.
        List<E> result = new ArrayList<>(received.size() + 1);
        result.addAll(received);
        result.add(event);
        return result;
    }

    // --- Compiled model (package-private) -----------------------------------------------------------------------------

    record CompiledStep<E, C>(String name, StepBody<E, C> body, @Nullable TimeoutSpec<E, C> timeout) {
    }

    sealed interface StepBody<E, C> permits ChoiceBody, JoinBody {
    }

    record ChoiceBody<E, C>(List<Branch<E, C>> branches) implements StepBody<E, C> {
    }

    record JoinBody<E, C>(List<Expectation<E>> expectations, Function<ReceivedEvents<E>, List<C>> whenFulfilled, Continuation then) implements StepBody<E, C> {
    }

    record Branch<E, C>(Class<? extends E> eventType, @Nullable BiPredicate<E, ReceivedEvents<E>> guard, BiFunction<EventMetadata, E, List<C>> commands, Continuation then) {
    }

    record TimeoutSpec<E, C>(@Nullable Duration after, @Nullable Function<ReceivedEvents<E>, Instant> at, Function<ReceivedEvents<E>, List<C>> onExpiry, Continuation then) {
    }
}
