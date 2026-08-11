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

package org.occurrent.dsl.saga.flow.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.ReceivedEvents;

import java.util.List;
import java.util.Objects;

/**
 * The concrete state of a flow saga instance: the {@link FlowState} the executor evolves and the store persists. On top
 * of the user-meaningful {@link #currentStep()}, {@link #received()} and {@link #completed()} it carries the transition
 * bookkeeping the flow lowering writes in {@code evolve} and reads back in {@code react} ({@link #windowStart()},
 * {@link #stepEntryIndex()}, {@link #previousStep()}, {@link #previousStepEntryIndex()}, {@link #lastAction()} and
 * {@link #matchedBranchIndex()}), which is part of the record only because {@code evolve} can communicate with
 * {@code react} solely through the returned state.
 * <p>
 * This type is {@code public} only so that a {@code SagaStateStore} in another module can construct and read it. It is
 * not a user-facing API. Author flow sagas through {@link FlowSaga} (or the Kotlin {@code saga} block) and observe their
 * state through {@link FlowState}.
 *
 * <h2>Bounded retention</h2>
 * {@link #received()} is <em>not</em> the full history. To cap what a long-running instance keeps across steps (and with
 * it the per-save serialization cost), the flow lowering retains only a window of received events, the initiating event,
 * always kept as {@code received.get(0)}, followed by the events from the current step's entry back through a
 * configurable carry-over of earlier events (see the flow builder's {@code historyWindow}). Events older than the window
 * are dropped, so a guard, a window-condition reaction, or a timeout reaction that reads {@link ReceivedEvents} sees only
 * the retained window (the initiating event is the one exception, it is always available). {@link #windowStart()} is the
 * absolute position the retained tail begins at, which lets a window condition's matching window be reconstructed even
 * after the prefix is dropped. {@link #stepEntryIndex()} is an absolute position too, not an index into the (bounded)
 * {@link #received()} list.
 * <p>
 * What {@code historyWindow} limits is the carry-over <em>behind</em> the current step's entry, and only that. The current
 * step's own events are all retained however many arrive, because a window condition counts over them, and the trim only
 * ever runs on a transition. {@code historyWindow(0)} is no exception, dropping every earlier event and still keeping the
 * active step's. So an instance parked in one step while a large number of correlated events arrive does grow, and nothing
 * currently caps that. A flow whose steps turn over stays within the window.
 *
 * <h2>Compatibility note</h2>
 * The bookkeeping fields ({@link #stepEntryIndex()}, {@link #windowStart()}, {@link #previousStep()},
 * {@link #previousStepEntryIndex()}, {@link #lastAction()}, {@link #matchedBranchIndex()}) are an implementation detail of
 * the flow lowering. They are not a
 * stable wire format: their meaning can change between versions, and a store that persists a flow saga's state must round-
 * trip whatever it wrote without interpreting them. Only {@link #currentStep()}, {@link #received()} and
 * {@link #completed()} carry user-meaningful semantics. {@link ActionKind#JOIN} is one such implementation detail moving
 * between versions. Since ADR 120, every branch firing writes {@link ActionKind#BRANCH} instead, a lowered {@code join}
 * step included, so {@code JOIN} is never written by current code. The constant stays declared so a document a
 * pre-ADR-120 process wrote, and never re-evolved since, still round-trips instead of failing {@code ActionKind.valueOf}.
 * {@link #previousStepEntryIndex()} is the newest of them, added in 0.33.0, and {@code -1} means "not known", which is what
 * a document written before it existed reads back as.
 *
 * @param <E>                     the domain event type
 * @param previousStepEntryIndex  the absolute position {@link #previousStep()} was entered at, so {@code react} can hand a
 *                                window-condition reaction the window that fulfilled it rather than the whole retained
 *                                history. {@code -1} when no transition just happened, or when a store dropped the field,
 *                                in which case the reaction falls back to the whole retained history
 */
public record FlowStateImpl<E>(@Nullable String currentStep,
                               List<E> received,
                               int windowStart,
                               int stepEntryIndex,
                               boolean completed,
                               @Nullable String previousStep,
                               int previousStepEntryIndex,
                               ActionKind lastAction,
                               int matchedBranchIndex) implements FlowState<E> {

    /**
     * What the last {@code evolve} did, so {@code react} knows which reaction to run. Internal bookkeeping.
     * {@link #JOIN} is retained for wire compatibility only. Current code always writes {@link #BRANCH}, see this
     * record's compatibility note.
     */
    public enum ActionKind {NONE, BRANCH, JOIN, TIMEOUT}

    public FlowStateImpl {
        Objects.requireNonNull(received, "received cannot be null");
        Objects.requireNonNull(lastAction, "lastAction cannot be null");
        received = List.copyOf(received);
    }

    /**
     * The pre-0.33.0 component list, without {@link #previousStepEntryIndex()}. Kept so a {@code SagaStateStore} written
     * against the earlier record still compiles and still behaves. The absent value becomes {@code -1}, and a
     * window-condition reaction then reads the whole retained history, which is what it read before the field existed.
     */
    public FlowStateImpl(@Nullable String currentStep, List<E> received, int windowStart, int stepEntryIndex,
                         boolean completed, @Nullable String previousStep, ActionKind lastAction, int matchedBranchIndex) {
        this(currentStep, received, windowStart, stepEntryIndex, completed, previousStep, -1, lastAction, matchedBranchIndex);
    }

    /** The initial state of a flow saga instance, before its start event has been applied. */
    public static <E> FlowStateImpl<E> initial() {
        return new FlowStateImpl<>(null, List.of(), 0, 0, false, null, -1, ActionKind.NONE, -1);
    }
}
