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
import org.occurrent.dsl.saga.TimerName;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;
import org.occurrent.dsl.saga.internal.TypeDispatch;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Compiles a flow definition (steps, branches, window conditions, timeouts) down to the core {@link Saga} the executor
 * runs. The step name is the persisted position, a timer is named by {@link FlowSaga#stepTimer(String)}, and because a
 * timer lives only in the saga's own state envelope (there is exactly one per name), a re-armed timer replaces the
 * previous one and no separate fencing is needed here.
 */
final class FlowSagaImpl<E, C> implements Saga<E, FlowState<E>, C> {

    /** The namespace every step timer belongs to, so a step's timer is stored as {@code step:<stepName>}. */
    static final String TIMER_NAMESPACE = "step";

    /**
     * Default number of received events kept behind the current step's entry, on top of the initiating event and the
     * current step's own events. 100 comfortably covers a retry loop or a guard that looks a few steps back, while capping
     * what a long-running instance carries from step to step.
     */
    static final int DEFAULT_HISTORY_WINDOW = 100;

    /** What {@code stepWindow} is when it was never set, meaning every event the current step receives is kept. */
    static final int UNBOUNDED_STEP_WINDOW = Integer.MAX_VALUE;

    private final Class<? extends E> startType;
    private final BiFunction<EventMetadata, E, List<C>> onStartCommands;
    private final List<CompiledStep<E, C>> steps;
    private final Map<String, Integer> stepIndex;
    private final Map<String, CompiledStep<E, C>> stepsByName;
    private final TypeDispatch<Function<E, @Nullable String>> correlators;
    private final @Nullable Function<E, @Nullable String> correlateAll;
    private final Set<Class<? extends E>> startEventTypes;
    private final Set<Class<? extends E>> eventTypes;
    // How many received events before the current step's entry are kept, and so what a guard and a reaction can still read
    // of the earlier history. Applied when a step is left.
    private final int historyWindow;
    // How many of the current step's own events are kept. Applied on every delivery, since a step being parked on is what
    // it caps. UNBOUNDED_STEP_WINDOW keeps all of them.
    private final int stepWindow;

    FlowSagaImpl(Class<? extends E> startType,
                 BiFunction<EventMetadata, E, List<C>> onStartCommands,
                 List<CompiledStep<E, C>> steps,
                 Map<String, Integer> stepIndex,
                 Map<String, CompiledStep<E, C>> stepsByName,
                 Map<Class<?>, Function<E, @Nullable String>> correlators,
                 @Nullable Function<E, @Nullable String> correlateAll,
                 Set<Class<? extends E>> startEventTypes,
                 Set<Class<? extends E>> eventTypes,
                 int historyWindow,
                 int stepWindow) {
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
        this.stepWindow = stepWindow;
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
            // No react on the start event itself, onStart carries the instance-creation effects. The counts start out
            // unknown, so the next delivery derives them from a window that begins after this event.
            return new FlowStateImpl<>(first, List.of(event), 1, 1, false, null, -1, ActionKind.NONE, -1, null);
        }
        if (state.completed() || state.currentStep() == null) {
            return state;
        }
        CompiledStep<E, C> step = stepsByName.get(state.currentStep());
        List<E> appended = append(state.received(), event);
        // Drop the current step's oldest events past stepWindow before anything reads them, so a guard and a reaction in
        // this delivery see exactly what gets persisted.
        int windowStart = boundedWindowStart(state.stepEntryIndex(), state.windowStart(), appended.size());
        List<E> received = retain(appended, state.windowStart(), windowStart);
        List<E> window = received.subList(windowStartIndex(state.stepEntryIndex(), windowStart, received.size()), received.size());
        List<Integer> counts = stepConditionCounts(state, step, windowStart, window, event);
        int[] leafCursor = {0};
        List<Branch<E, C>> branches = step.branches();
        for (int i = 0; i < branches.size(); i++) {
            Branch<E, C> branch = branches.get(i);
            if (triggered(branch.trigger(), event, received, window, counts, leafCursor)) {
                return applyTransition(state, windowStart, branch.then(), received, ActionKind.BRANCH, i);
            }
        }
        return withClearedBookkeeping(state, windowStart, received, progressFor(step, counts));
    }

    private static <E, C> @Nullable StepConditionProgress progressFor(CompiledStep<E, C> step, @Nullable List<Integer> counts) {
        return counts == null ? null : new StepConditionProgress(step.leaves().fingerprint(), counts);
    }

    // A classic on(Class, ...) branch fires only on a matching arriving event (a guard reads the event plus the received
    // log, but a guarded branch is deliberately NOT re-checked on later, unrelated events, see StepBuilder's javadoc). A
    // window-condition branch (on(StepCondition, ...) or the join sugar) fires whenever the accumulating window since step
    // entry satisfies its tree, so it is re-evaluated on every arriving event regardless of that event's own type, since a
    // tree can span several leaf types. received is wrapped in ReceivedEvents only inside the guard branch, since an
    // unguarded classic branch (the common case) and a window condition never read it.
    private static <E> boolean triggered(Trigger<E> trigger, E event, List<E> received, List<E> window,
                                         @Nullable List<Integer> counts, int[] leafCursor) {
        return switch (trigger) {
            case ArrivingEvent<E> arriving ->
                    arriving.eventType().isInstance(event) && (arriving.guard() == null || arriving.guard().test(event, ReceivedEvents.of(received)));
            case WindowCondition<E> windowCondition -> counts == null
                    ? conditionFulfilled(windowCondition.condition(), window)
                    : conditionFulfilled(windowCondition.condition(), counts, leafCursor);
        };
    }

    private FlowStateImpl<E> evolveOnTimeout(FlowStateImpl<E> state, TimerName timerName) {
        if (state.completed() || state.currentStep() == null) {
            return unchangedExceptBookkeeping(state);
        }
        if (!timerName.equals(FlowSaga.stepTimer(state.currentStep()))) {
            return unchangedExceptBookkeeping(state);
        }
        CompiledStep<E, C> step = stepsByName.get(state.currentStep());
        if (step.timeout() == null) {
            return unchangedExceptBookkeeping(state);
        }
        return applyTransition(state, state.windowStart(), step.timeout().then(), state.received(), ActionKind.TIMEOUT, -1);
    }

    // No event arrived, so nothing is dropped and no count changes.
    private FlowStateImpl<E> unchangedExceptBookkeeping(FlowStateImpl<E> state) {
        return withClearedBookkeeping(state, state.windowStart(), state.received(), state.stepConditionProgress());
    }

    // Stay in the current step with no transition. Keep the given received log and counts but reset the evolve-to-react
    // bookkeeping, so react (which routes on lastAction) does nothing rather than re-running a previous transition's
    // reaction. Used both when an event matches no branch or fulfils no window condition, and on an ignored timeout.
    // stepEntryIndex is preserved because no transition happened, so the current step's window still starts where it did.
    private FlowStateImpl<E> withClearedBookkeeping(FlowStateImpl<E> state, int windowStart, List<E> received,
                                                    @Nullable StepConditionProgress progress) {
        return new FlowStateImpl<>(state.currentStep(), received, windowStart, state.stepEntryIndex(), state.completed(),
                state.currentStep(), -1, ActionKind.NONE, -1, progress);
    }

    // The relative index into a retained received list where the window of a step entered at entryPosition begins.
    // received.get(0) is the pinned initiating event, so absolute position p maps to relative index p - windowStart + 1.
    // Clamped into [1, size], because a store defaults each absent bookkeeping field on its own and can hand back a
    // combination no evolve ever wrote. Index 0 is the pinned initiating event and belongs to no step's window, and a
    // start past the end gives an empty window rather than an IndexOutOfBoundsException from inside command dispatch.
    // The lower clamp also carries stepWindow's own case, where windowStart has legitimately passed the step's entry, and
    // index 1 is then the oldest of the step's events that survived.
    private static int windowStartIndex(int entryPosition, int windowStart, int size) {
        return Math.min(Math.max(1, entryPosition - windowStart + 1), size);
    }

    // Where the retained tail has to start so that at most stepWindow of the current step's own events are kept.
    // The tail is one run of events, and the current step's events sit at the end of it behind whatever carry-over
    // historyWindow granted, so dropping the step's oldest events means dropping the whole carry-over ahead of them first.
    // Advancing the start by the excess alone would drop that many carry-over events and leave every one of the step's,
    // which caps nothing and takes the history a guard was promised.
    private int boundedWindowStart(int stepEntryIndex, int windowStart, int size) {
        if (stepWindow == UNBOUNDED_STEP_WINDOW) {
            return windowStart;
        }
        int firstStepEvent = windowStartIndex(stepEntryIndex, windowStart, size);
        int kept = size - firstStepEvent;
        if (kept <= stepWindow) {
            // The step is within its cap, so the carry-over is left exactly as historyWindow left it.
            return windowStart;
        }
        int carryOver = firstStepEvent - 1;
        return windowStart + carryOver + (kept - stepWindow);
    }

    // Every transition resets stepEntryIndex to the new step's entry, including a transitionTo back into the current step
    // (a self-loop), so re-entering a step, classic branch or window condition alike, restarts every window that step
    // carries. In a mixed step, a classic branch self-looping wipes a sibling window condition's partial count the same
    // way it already wipes a join's. This is today's join semantics generalized, kept deliberately, and becomes visible
    // once branches mix, so it is also stated in ADR 120, the on(StepCondition) javadoc and the docs, and asserted by a test.
    private FlowStateImpl<E> applyTransition(FlowStateImpl<E> from, int windowStart, Continuation continuation, List<E> received, ActionKind kind, int branchIndex) {
        // The new step is entered after every event received so far, so its entry is the absolute event count. received holds
        // the initiating event (position 0) plus the tail starting at windowStart, so that count is windowStart plus the tail
        // length, i.e. windowStart + (received.size() - 1). When nothing has been dropped (windowStart == 1) this is exactly
        // received.size(), matching the pre-windowing behaviour.
        int newStepEntry = windowStart + received.size() - 1;
        // Drop received events older than historyWindow behind the step we are leaving. Anchoring on the step we leave (not
        // the one we enter) keeps that step's own events for its reaction to read, and historyWindow adds earlier events on
        // top for guards that look further back. windowStart only ever advances, which is also what makes this leave a
        // stepWindow drop from this same delivery alone rather than trying to move back behind it.
        int newWindowStart = Math.max(windowStart, from.stepEntryIndex() - historyWindow);
        List<E> retained = retain(received, windowStart, newWindowStart);
        String fromStep = from.currentStep();
        // Where the fired window begins, carried over for react, since overwriting stepEntryIndex with the entered step's
        // entry destroys it and it cannot be recovered from the state afterwards. newWindowStart never exceeds it, so the
        // fired window is always fully inside retained.
        int previousStepEntry = from.stepEntryIndex();
        // The entered step's window is empty, so its counts start out unknown and the next delivery derives them. That is
        // also what resets a self-loop's partial counts, since a transitionTo naming the current step comes through here too.
        return switch (continuation) {
            case Continuation.Next ignored -> {
                int next = stepIndex.get(fromStep) + 1;
                if (next < steps.size()) {
                    yield new FlowStateImpl<>(steps.get(next).name(), retained, newWindowStart, newStepEntry, false, fromStep, previousStepEntry, kind, branchIndex, null);
                }
                yield new FlowStateImpl<>(null, retained, newWindowStart, newStepEntry, true, fromStep, previousStepEntry, kind, branchIndex, null);
            }
            case Continuation.TransitionTo transitionTo -> new FlowStateImpl<>(transitionTo.stepName(), retained, newWindowStart, newStepEntry, false, fromStep, previousStepEntry, kind, branchIndex, null);
            case Continuation.End ignored -> new FlowStateImpl<>(null, retained, newWindowStart, newStepEntry, true, fromStep, previousStepEntry, kind, branchIndex, null);
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
        ReceivedEvents<E> receivedEvents = reactionWindow(state, branch.trigger());
        List<SagaEffect<C>> effects = issueAll(branch.reaction().react(triggering.metadata(), triggering.event(), receivedEvents));
        retargetTimers(effects, state, false);
        return effects;
    }

    // What a firing branch's reaction reads. Every WindowCondition trigger, on(StepCondition, ...) and the lowered join
    // sugar alike, gets whatever of the events received since the step it fired from was entered is still retained, all
    // of them unless a stepWindow cap has already evicted the step's own oldest ones. The condition still fires on the
    // count it counted regardless, since that count is carried forward rather than re-derived from what remains. A
    // classic branch reads the whole retained history instead, and so does a WindowCondition whose state has
    // previousStepEntryIndex -1, which is what a store rebuilt without that field hands back.
    private ReceivedEvents<E> reactionWindow(FlowStateImpl<E> state, Trigger<E> trigger) {
        boolean windowed = trigger instanceof WindowCondition<E>
                && state.previousStepEntryIndex() >= 0;
        if (!windowed) {
            return ReceivedEvents.of(state.received());
        }
        int from = windowStartIndex(state.previousStepEntryIndex(), state.windowStart(), state.received().size());
        return new ReceivedEventsList<>(state.received(), from);
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
        ReceivedEvents<E> receivedEvents = reactionWindow(state, branch.trigger());
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
                effects.add(SagaEffect.cancelTimeout(FlowSaga.stepTimer(fromStep)));
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
        TimerName timerName = FlowSaga.stepTimer(stepName);
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
    // Used when this step's counts are not carried in the state, which is either the first delivery into the step, a
    // declaration the counts no longer describe, or a step whose leaves cannot be told apart (see StepLeaves).
    private static <E> boolean conditionFulfilled(StepCondition<E> condition, List<E> window) {
        return switch (condition) {
            case StepCondition.AtLeast<E> atLeast -> countMatches(atLeast.matcher(), window) >= atLeast.count();
            case StepCondition.AllOf<E> allOf -> {
                for (StepCondition<E> child : allOf.conditions()) {
                    if (!conditionFulfilled(child, window)) {
                        yield false;
                    }
                }
                yield true;
            }
            case StepCondition.AnyOf<E> anyOf -> {
                for (StepCondition<E> child : anyOf.conditions()) {
                    if (conditionFulfilled(child, window)) {
                        yield true;
                    }
                }
                yield false;
            }
        };
    }

    // The same check read off the carried counts instead of the window. leafCursor walks the tree in declaration order, the
    // order StepLeaves collected the matchers in, and it is advanced over every leaf even once the answer is settled,
    // because a later child's position in the count list depends on how many leaves the earlier children hold. That is why
    // neither composite stops early here, unlike the window version above.
    private static <E> boolean conditionFulfilled(StepCondition<E> condition, List<Integer> counts, int[] leafCursor) {
        return switch (condition) {
            case StepCondition.AtLeast<E> atLeast -> counts.get(leafCursor[0]++) >= atLeast.count();
            case StepCondition.AllOf<E> allOf -> {
                boolean fulfilled = true;
                for (StepCondition<E> child : allOf.conditions()) {
                    fulfilled = conditionFulfilled(child, counts, leafCursor) && fulfilled;
                }
                yield fulfilled;
            }
            case StepCondition.AnyOf<E> anyOf -> {
                boolean fulfilled = false;
                for (StepCondition<E> child : anyOf.conditions()) {
                    fulfilled = conditionFulfilled(child, counts, leafCursor) || fulfilled;
                }
                yield fulfilled;
            }
        };
    }

    /**
     * The counts to evaluate this step's conditions with, or {@code null} to count the window instead. The carried counts
     * are used once they were counted for the declaration this step has now, and are re-derived from the window when they
     * were not, which covers a document written before the field existed and a redeploy that changed a leaf. Re-deriving
     * needs the events, so a step whose older events {@code stepWindow} already dropped has nothing left to fall back on
     * and refuses the delivery instead of counting short.
     */
    private @Nullable List<Integer> stepConditionCounts(FlowStateImpl<E> state, CompiledStep<E, C> step, int windowStart,
                                                        List<E> window, E event) {
        StepLeaves<E> leaves = step.leaves();
        if (leaves.matchers().isEmpty() || !leaves.countable()) {
            return null;
        }
        StepConditionProgress progress = state.stepConditionProgress();
        if (progress != null && describesTheSameLeaves(progress, leaves)) {
            return incremented(leaves.matchers(), progress.matchCounts(), event);
        }
        if (droppedFromTheCurrentStep(state, windowStart)) {
            throw new IllegalStateException("step '" + step.name() + "' cannot be evaluated for this instance, because the"
                    + " step's condition declaration changed while the instance was parked in it and stepWindow had already"
                    + " dropped the events its counts would be rebuilt from. Retrying the delivery cannot help. Put the"
                    + " previous condition declaration for this step back until the parked instances have moved on, or"
                    + " delete the instance");
        }
        return counted(leaves.matchers(), window);
    }

    // Whether any of the current step's own events are already gone, which is what leaves nothing to rebuild its counts
    // from. Reading the tail as starting past the step's entry is the signal, and the entry check is what keeps a store's
    // defaulting from looking like one, since an instance that has entered a step was entered at position 1 or later, while
    // a defaulted entry reads as 0 and a defaulted tail start reads as 1 and so can never pass a real entry position.
    private static boolean droppedFromTheCurrentStep(FlowStateImpl<?> state, int windowStart) {
        return state.stepEntryIndex() >= 1 && windowStart > state.stepEntryIndex();
    }

    // Whether the carried counts were counted for the leaves this step declares now. The length check and the negative
    // check are here because a store defaults each absent field on its own and can hand back a list no evolve ever wrote,
    // and a count is only ever written as zero or more.
    private static <E> boolean describesTheSameLeaves(StepConditionProgress progress, StepLeaves<E> leaves) {
        if (!progress.leafFingerprint().equals(leaves.fingerprint()) || progress.matchCounts().size() != leaves.matchers().size()) {
            return false;
        }
        for (int count : progress.matchCounts()) {
            if (count < 0) {
                return false;
            }
        }
        return true;
    }

    // Saturating at Integer.MAX_VALUE, because a leaf's truth can never be undone by a later event, so a count that high
    // already exceeds every threshold a leaf can ask for and losing the exact total past it changes no answer.
    private static <E> List<Integer> incremented(List<StepCondition.EventMatcher<E>> matchers, List<Integer> counts, E event) {
        List<Integer> next = new ArrayList<>(counts.size());
        for (int i = 0; i < matchers.size(); i++) {
            int count = counts.get(i);
            next.add(matches(matchers.get(i), event) && count < Integer.MAX_VALUE ? count + 1 : count);
        }
        return next;
    }

    private static <E> List<Integer> counted(List<StepCondition.EventMatcher<E>> matchers, List<E> window) {
        List<Integer> counts = new ArrayList<>(matchers.size());
        for (StepCondition.EventMatcher<E> matcher : matchers) {
            counts.add(countMatches(matcher, window));
        }
        return counts;
    }

    private static <E> int countMatches(StepCondition.EventMatcher<E> matcher, List<E> window) {
        int count = 0;
        for (E event : window) {
            if (matches(matcher, event)) {
                count++;
            }
        }
        return count;
    }

    private static <E> boolean matches(StepCondition.EventMatcher<E> matcher, E event) {
        return matcher.eventType().isInstance(event) && (matcher.predicate() == null || matcher.predicate().test(event));
    }

    private static <E> List<E> append(List<E> received, E event) {
        // FlowState's constructor makes the immutable copy, so build a single sized ArrayList here rather than copying twice.
        List<E> result = new ArrayList<>(received.size() + 1);
        result.addAll(received);
        result.add(event);
        return result;
    }

    // --- Compiled model (package-private) -----------------------------------------------------------------------------

    record CompiledStep<E, C>(String name, List<Branch<E, C>> branches, @Nullable TimeoutSpec<E, C> timeout, StepLeaves<E> leaves) {
    }

    /**
     * Every leaf the window conditions of one step declare, flattened across its branches in declaration order, which is
     * the order a kept count list is read in.
     * <p>
     * {@code fingerprint} holds the step's name and, per leaf, its event type and the name of its predicate if it has one.
     * What it deliberately leaves out is the count a leaf asks for, because a kept count is the raw number of events that
     * matched the leaf's matcher rather than something capped at a threshold, so raising or lowering a count on a redeploy
     * still leaves the kept numbers meaning what they meant. Each part is written with its length in front, so no step name
     * and no predicate name can forge a boundary between parts.
     * <p>
     * {@code uncountableWhy} says why this step's counts cannot be kept at all, and is null when they can. Two things make
     * a step uncountable, and both come down to a predicate having no identity of its own. A leaf whose predicate has no
     * name cannot be told from the same leaf carrying a changed predicate after a redeploy, and two leaves that share a
     * name while holding different predicates cannot be told from each other. Such a step counts its window on every
     * delivery instead, which is exactly what every step did before counts were kept, and {@code stepWindow} refuses to
     * build a saga containing one because dropping that step's events would leave nothing to count.
     */
    record StepLeaves<E>(List<StepCondition.EventMatcher<E>> matchers, String fingerprint, @Nullable String uncountableWhy) {

        boolean countable() {
            return uncountableWhy == null;
        }

        static <E, C> StepLeaves<E> of(String stepName, List<Branch<E, C>> branches) {
            List<StepCondition.EventMatcher<E>> matchers = new ArrayList<>();
            for (Branch<E, C> branch : branches) {
                if (branch.trigger() instanceof WindowCondition<E> windowCondition) {
                    StepConditionWalk.forEachLeafMatcher(windowCondition.condition(), matchers::add);
                }
            }
            StringBuilder fingerprint = new StringBuilder();
            appendPart(fingerprint, stepName);
            Map<String, StepCondition.EventMatcher<E>> firstPerEntry = new LinkedHashMap<>();
            String why = null;
            for (StepCondition.EventMatcher<E> matcher : matchers) {
                String type = matcher.eventType().getName();
                String predicatePart = matcher.predicate() == null ? ""
                        : "?" + (matcher.predicateId() == null ? "" : matcher.predicateId());
                appendPart(fingerprint, type);
                appendPart(fingerprint, predicatePart);
                String simple = matcher.eventType().getSimpleName();
                if (why == null && matcher.predicate() != null && matcher.predicateId() == null) {
                    why = "its leaf over " + simple + " carries a predicate with no name, and a lambda is a different object"
                            + " every time the class loads, so a redeploy that changed that predicate cannot be told from one"
                            + " that did not. Name it with event(" + simple + ".class, count, \"<name>\", predicate)";
                }
                StepCondition.EventMatcher<E> earlier = firstPerEntry.putIfAbsent(type + predicatePart, matcher);
                // Comparing against the first leaf of the entry is enough, since any leaf accepting different events from it
                // means they do not all count the same events, which is the only thing that makes crossing two counts harmless.
                if (why == null && earlier != null && !earlier.matchesTheSameEvents(matcher)) {
                    why = "two of its leaves over " + simple + " share the predicate name '" + matcher.predicateId()
                            + "' while holding different predicates, so nothing tells their counts apart. Give them different"
                            + " names, or pass the same predicate to both if they are the same test";
                }
            }
            return new StepLeaves<>(List.copyOf(matchers), fingerprint.toString(), why);
        }

        // Length in front of every part, so a step name or a predicate name holding the separator cannot pass itself off as
        // two parts and make one declaration's fingerprint match another's.
        private static void appendPart(StringBuilder fingerprint, String part) {
            fingerprint.append(part.length()).append(':').append(part).append('|');
        }
    }

    /** What makes a branch fire, a classic arriving-event match, or a window condition over the step's received events. */
    sealed interface Trigger<E> permits ArrivingEvent, WindowCondition {
    }

    record ArrivingEvent<E>(Class<? extends E> eventType, @Nullable BiPredicate<E, ReceivedEvents<E>> guard) implements Trigger<E> {
    }

    /**
     * A window condition trigger, built by {@code on(StepCondition, ...)} directly and by the deprecated {@code
     * join(...)}'s lowering alike. Both read the window {@code condition} was evaluated over, the events received since the
     * step this branch fired from was entered.
     */
    record WindowCondition<E>(StepCondition<E> condition) implements Trigger<E> {
    }

    record Branch<E, C>(Trigger<E> trigger, BranchReaction<E, C> reaction, Continuation then) {
    }

    /**
     * A branch's reaction, unified across both trigger kinds. A classic on(...) adapter uses {@code metadata} and
     * {@code triggering} and ignores {@code received}. A window-condition adapter (on(StepCondition, ...), and the join
     * sugar) reads only {@code received} and ignores the other two, so it tolerates the null {@code triggering} that
     * {@code reactToJoin}'s defensive path passes.
     */
    @FunctionalInterface
    interface BranchReaction<E, C> {
        List<C> react(EventMetadata metadata, @Nullable E triggering, ReceivedEvents<E> received);
    }

    record TimeoutSpec<E, C>(@Nullable Duration after, @Nullable Function<ReceivedEvents<E>, Instant> at, Function<ReceivedEvents<E>, List<C>> onExpiry, Continuation then) {
    }
}
