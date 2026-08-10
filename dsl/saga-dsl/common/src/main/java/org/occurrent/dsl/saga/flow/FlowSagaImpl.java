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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.internal.TypeDispatch;

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
 * Compiles a flow definition (steps, branches, window conditions, timeouts) down to the core {@link Saga} the executor
 * runs. The step name is the persisted position, a timer is named {@code "step:" + stepName}, and because a timer lives
 * only in the saga's own state envelope (there is exactly one per name), a re-armed timer replaces the previous one and
 * no separate fencing is needed here.
 */
final class FlowSagaImpl<E, C> implements Saga<E, FlowState<E>, C> {

    static final String TIMER_PREFIX = "step:";

    /**
     * Default number of received events kept behind the current step's entry, on top of the initiating event and the
     * current step's own events (which are always retained because a window condition counts over them). 100 comfortably
     * covers a retry loop or a guard that looks a few steps back, while keeping a long-running instance's state bounded.
     */
    static final int DEFAULT_HISTORY_WINDOW = 100;

    private final Class<? extends E> startType;
    private final BiFunction<EventMetadata, E, List<C>> onStartCommands;
    private final List<CompiledStep<E, C>> steps;
    private final Map<String, Integer> stepIndex;
    private final Map<String, CompiledStep<E, C>> stepsByName;
    private final TypeDispatch<Function<E, @Nullable String>> correlators;
    private final @Nullable Function<E, @Nullable String> correlateAll;
    private final Set<Class<? extends E>> startEventTypes;
    private final Set<Class<? extends E>> eventTypes;
    // Carry-over: how many received events before the current step's entry are retained (and so visible to guards and
    // reactions). The current step's own events are always kept regardless, since a window condition must count over them.
    private final int historyWindow;

    FlowSagaImpl(Class<? extends E> startType,
                 BiFunction<EventMetadata, E, List<C>> onStartCommands,
                 List<CompiledStep<E, C>> steps,
                 Map<String, Integer> stepIndex,
                 Map<String, CompiledStep<E, C>> stepsByName,
                 Map<Class<?>, Function<E, @Nullable String>> correlators,
                 @Nullable Function<E, @Nullable String> correlateAll,
                 Set<Class<? extends E>> startEventTypes,
                 Set<Class<? extends E>> eventTypes,
                 int historyWindow) {
        this.startType = startType;
        this.onStartCommands = onStartCommands;
        this.steps = steps;
        this.stepIndex = stepIndex;
        this.stepsByName = stepsByName;
        this.correlators = new TypeDispatch<>(correlators);
        this.correlateAll = correlateAll;
        this.startEventTypes = startEventTypes;
        this.eventTypes = eventTypes;
        this.historyWindow = historyWindow;
    }

    @Override
    public FlowState<E> initialState() {
        return FlowStateImpl.initial();
    }

    @Override
    public @Nullable String sagaId(E event) {
        Function<E, @Nullable String> correlator = correlators.resolve(event.getClass());
        if (correlator != null) {
            return correlator.apply(event);
        }
        return correlateAll == null ? null : correlateAll.apply(event);
    }

    @Override
    public Set<Class<? extends E>> startEventTypes() {
        return startEventTypes;
    }

    @Override
    public Set<Class<? extends E>> eventTypes() {
        return eventTypes;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public boolean isTerminal(FlowState<E> state) {
        return state.completed();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public FlowState<E> evolve(FlowState<E> state, SagaInput<E> input) {
        // The executor only ever hands back a state this saga produced, so the concrete type is always FlowStateImpl. Narrow
        // once at the boundary so the transition machinery below works against the full (bookkeeping-carrying) state.
        FlowStateImpl<E> current = impl(state);
        return switch (input) {
            case SagaInput.Event<E> ev -> evolveOnEvent(current, ev.event());
            case SagaInput.Timeout<E> to -> evolveOnTimeout(current, to.timeout().timerName());
        };
    }

    // A flow saga's state is only ever produced by this executor (initialState/evolve), so every state handed to evolve or
    // react is a FlowStateImpl. Narrow it here rather than casting inline so that a caller passing a hand-rolled FlowState
    // straight into the public evolve/react gets a clear message instead of a bare ClassCastException.
    @SuppressWarnings("unchecked")
    private FlowStateImpl<E> impl(FlowState<E> state) {
        if (state instanceof FlowStateImpl<?> flowState) {
            return (FlowStateImpl<E>) flowState;
        }
        throw new IllegalArgumentException("FlowState must be one produced by the flow saga executor (FlowStateImpl), got "
                + (state == null ? "null" : state.getClass().getName()));
    }

    private FlowStateImpl<E> evolveOnEvent(FlowStateImpl<E> state, E event) {
        if (!state.completed() && state.currentStep() == null) {
            // Instance creation: the start event enters the first step, its window opens after the start event itself. The
            // start event is received.get(0) and is always retained. The retained tail begins at absolute position 1. A
            // first-step window condition naming the start type therefore counts only post-start arrivals, never the start
            // delivery itself, exactly as a first-step join or classic on(...) already behaves.
            if (!startType.isInstance(event)) {
                return state;
            }
            String first = steps.get(0).name();
            // No react on the start event itself: onStart carries the instance-creation effects.
            return new FlowStateImpl<>(first, List.of(event), 1, 1, false, null, ActionKind.NONE, -1);
        }
        if (state.completed() || state.currentStep() == null) {
            return state;
        }
        List<E> received = append(state.received(), event);
        CompiledStep<E, C> step = stepsByName.get(state.currentStep());
        ReceivedEvents<E> receivedEvents = ReceivedEvents.of(received);
        List<E> window = received.subList(windowStart(state), received.size());
        List<Branch<E, C>> branches = step.branches();
        for (int i = 0; i < branches.size(); i++) {
            Branch<E, C> branch = branches.get(i);
            if (triggered(branch.trigger(), event, receivedEvents, window)) {
                return applyTransition(state, branch.then(), received, ActionKind.BRANCH, i);
            }
        }
        return withClearedBookkeeping(state, received);
    }

    // A classic on(Class, ...) branch fires only on a matching arriving event (a guard reads the event plus the received
    // log, but a guarded branch is deliberately NOT re-checked on later, unrelated events, see StepBuilder's javadoc). A
    // window-condition branch (on(StepCondition, ...) or the join sugar) fires whenever the accumulating window since step
    // entry satisfies its tree, so it is re-evaluated on every arriving event regardless of that event's own type, since a
    // tree can span several leaf types.
    private static <E> boolean triggered(Trigger<E> trigger, E event, ReceivedEvents<E> receivedEvents, List<E> window) {
        return switch (trigger) {
            case ArrivingEvent<E> arriving ->
                    arriving.eventType().isInstance(event) && (arriving.guard() == null || arriving.guard().test(event, receivedEvents));
            case WindowCondition<E> windowCondition -> conditionFulfilled(windowCondition.condition(), window);
        };
    }

    private FlowStateImpl<E> evolveOnTimeout(FlowStateImpl<E> state, String timerName) {
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
    // both when an event matches no branch / fulfils no window condition, and on an ignored timeout. windowStart and
    // stepEntryIndex are preserved: no transition happened, so the current step's window is unchanged and its accumulating
    // events must not be dropped (a window condition counts over them).
    private FlowStateImpl<E> withClearedBookkeeping(FlowStateImpl<E> state, List<E> received) {
        return new FlowStateImpl<>(state.currentStep(), received, state.windowStart(), state.stepEntryIndex(), state.completed(),
                state.currentStep(), ActionKind.NONE, -1);
    }

    // The relative index into the retained received list where the current step's window begins. received.get(0) is the
    // pinned initiating event, so absolute position p maps to relative index p - windowStart + 1.
    private static int windowStart(FlowStateImpl<?> state) {
        return state.stepEntryIndex() - state.windowStart() + 1;
    }

    // Every transition resets stepEntryIndex to the new step's entry, including a transitionTo back into the current step
    // (a self-loop), so re-entering a step, classic branch or window condition alike, restarts every window that step
    // carries. In a mixed step, a classic branch self-looping wipes a sibling window condition's partial count the same
    // way it already wipes a join's. This is today's join semantics generalized, kept deliberately, and becomes visible
    // once branches mix, so it is also stated in ADR 120, the on(StepCondition) javadoc and the docs, and asserted by a test.
    private FlowStateImpl<E> applyTransition(FlowStateImpl<E> from, Continuation continuation, List<E> received, ActionKind kind, int branchIndex) {
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
                    yield new FlowStateImpl<>(steps.get(next).name(), retained, newWindowStart, newStepEntry, false, fromStep, kind, branchIndex);
                }
                yield new FlowStateImpl<>(null, retained, newWindowStart, newStepEntry, true, fromStep, kind, branchIndex);
            }
            case Continuation.TransitionTo transitionTo -> new FlowStateImpl<>(transitionTo.stepName(), retained, newWindowStart, newStepEntry, false, fromStep, kind, branchIndex);
            case Continuation.End ignored -> new FlowStateImpl<>(null, retained, newWindowStart, newStepEntry, true, fromStep, kind, branchIndex);
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
        for (C command : onStartCommands.apply(metadata, startEvent)) {
            effects.add(SagaEffect.issue(command));
        }
        armTimeoutIfAny(effects, state.currentStep(), ReceivedEvents.of(state.received()));
        return effects;
    }

    @Override
    public List<SagaEffect<C>> react(FlowState<E> state, SagaInput<E> input) {
        // As in evolve, the concrete type is always FlowStateImpl; react routes on the bookkeeping it carries.
        FlowStateImpl<E> current = impl(state);
        return switch (current.lastAction()) {
            case NONE -> List.of();
            case BRANCH -> reactToBranch(current, input);
            case JOIN -> reactToJoin(current);
            case TIMEOUT -> reactToTimeout(current);
        };
    }

    // Every classic on(...) branch and every window-condition branch (on(StepCondition, ...), and the join sugar) writes
    // ActionKind.BRANCH with its real index, so this one method reacts to both: BranchReaction always receives the
    // triggering event's metadata and the event itself, a classic-on adapter uses them, a window-condition adapter ignores
    // them and reads only the received window. BRANCH is only ever set from evolveOnEvent, so the input here is always a
    // SagaInput.Event and the cast is safe.
    private List<SagaEffect<C>> reactToBranch(FlowStateImpl<E> state, SagaInput<E> input) {
        CompiledStep<E, C> from = stepsByName.get(state.previousStep());
        Branch<E, C> branch = from.branches().get(state.matchedBranchIndex());
        SagaInput.Event<E> triggering = (SagaInput.Event<E>) input;
        ReceivedEvents<E> receivedEvents = ReceivedEvents.of(state.received());
        List<SagaEffect<C>> effects = issueAll(branch.reaction().react(triggering.metadata(), triggering.event(), receivedEvents));
        retargetTimers(effects, state, false);
        return effects;
    }

    // Defensive only, since evolve never writes ActionKind.JOIN any more (a lowered join is a WindowCondition branch,
    // written as BRANCH at index 0), so this is unreachable through the live evolve-then-react pipeline in the same
    // delivery. It exists because ActionKind keeps the JOIN constant for wire compatibility, so an instance persisted
    // mid-join by a pre-upgrade process round-trips a document whose lastAction still reads "JOIN", and evolve overwrites
    // lastAction from the fresh input before react ever runs, so that stale value is read back but never acted on. Kept
    // in its own method, not folded into reactToBranch, so it can never take the (SagaInput.Event<E>) cast that method
    // relies on. A defensive path reached on a timeout input would otherwise throw ClassCastException instead of
    // degrading harmlessly.
    private List<SagaEffect<C>> reactToJoin(FlowStateImpl<E> state) {
        CompiledStep<E, C> from = stepsByName.get(state.previousStep());
        Branch<E, C> branch = from.branches().get(0);
        ReceivedEvents<E> receivedEvents = ReceivedEvents.of(state.received());
        // metadata and the triggering event are unreachable placeholders here. The only reaction ActionKind.JOIN can ever
        // have named is a window-condition one (a join lowers to nothing else), and that adapter reads only receivedEvents.
        List<SagaEffect<C>> effects = issueAll(branch.reaction().react(EventMetadata.empty(), null, receivedEvents));
        retargetTimers(effects, state, false);
        return effects;
    }

    private List<SagaEffect<C>> reactToTimeout(FlowStateImpl<E> state) {
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
    private void retargetTimers(List<SagaEffect<C>> effects, FlowStateImpl<E> state, boolean firedFromTimer) {
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

    // Generalizes the old per-Expectation join check. An AtLeast leaf counts its matcher's hits in the window, AllOf and
    // AnyOf recurse and combine with && / ||. window is the same step-entry-anchored slice evolveOnEvent already computed.
    private static <E> boolean conditionFulfilled(StepCondition<E> condition, List<E> window) {
        return switch (condition) {
            case StepCondition.AtLeast<E> atLeast -> countMatches(atLeast.matcher(), window) >= atLeast.count();
            case StepCondition.AllOf<E> allOf -> allOf.conditions().stream().allMatch(child -> conditionFulfilled(child, window));
            case StepCondition.AnyOf<E> anyOf -> anyOf.conditions().stream().anyMatch(child -> conditionFulfilled(child, window));
        };
    }

    private static <E> int countMatches(StepCondition.EventMatcher<E> matcher, List<E> window) {
        int count = 0;
        for (E event : window) {
            if (matcher.eventType().isInstance(event) && (matcher.predicate() == null || matcher.predicate().test(event))) {
                count++;
            }
        }
        return count;
    }

    private static <E> List<E> append(List<E> received, E event) {
        // FlowState's constructor makes the immutable copy, so build a single sized ArrayList here rather than copying twice.
        List<E> result = new ArrayList<>(received.size() + 1);
        result.addAll(received);
        result.add(event);
        return result;
    }

    // --- Compiled model (package-private) -----------------------------------------------------------------------------

    record CompiledStep<E, C>(String name, List<Branch<E, C>> branches, @Nullable TimeoutSpec<E, C> timeout) {
    }

    /** What makes a branch fire, a classic arriving-event match, or a window condition over the step's received events. */
    sealed interface Trigger<E> permits ArrivingEvent, WindowCondition {
    }

    record ArrivingEvent<E>(Class<? extends E> eventType, @Nullable BiPredicate<E, ReceivedEvents<E>> guard) implements Trigger<E> {
    }

    record WindowCondition<E>(StepCondition<E> condition) implements Trigger<E> {
    }

    record Branch<E, C>(Trigger<E> trigger, BranchReaction<E, C> reaction, Continuation then) {
    }

    /**
     * A branch's reaction, unified across both trigger kinds. A classic on(...) adapter uses {@code metadata} and
     * {@code triggering} and ignores {@code received}. A window-condition adapter (on(StepCondition, ...), and the join
     * sugar) reads only {@code received} and ignores the other two.
     */
    @FunctionalInterface
    interface BranchReaction<E, C> {
        List<C> react(EventMetadata metadata, E triggering, ReceivedEvents<E> received);
    }

    record TimeoutSpec<E, C>(@Nullable Duration after, @Nullable Function<ReceivedEvents<E>, Instant> at, Function<ReceivedEvents<E>, List<C>> onExpiry, Continuation then) {
    }
}
