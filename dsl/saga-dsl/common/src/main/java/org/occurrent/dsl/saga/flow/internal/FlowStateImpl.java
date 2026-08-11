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
 * <h2>Retained events</h2>
 * {@link #received()} is <em>not</em> the full history. To cap what a long-running instance keeps (and with it the
 * per-save serialization cost), the flow lowering keeps only some of the received events, the initiating event, always
 * kept as {@code received.get(0)}, followed by the events from the current step's entry back through a configurable
 * carry-over of earlier events (see the flow builder's {@code historyWindow}). Older events are dropped, so a guard, a
 * window-condition reaction, or a timeout reaction that reads {@link ReceivedEvents} sees only what is still kept (the
 * initiating event is the one exception, it is always available). {@link #windowStart()} is the absolute position the
 * retained tail begins at, which lets a window condition's matching window be reconstructed even after the prefix is
 * dropped. {@link #stepEntryIndex()} is an absolute position too, not an index into the {@link #received()} list.
 * <p>
 * {@code historyWindow} limits the carry-over <em>behind</em> the current step's entry and is applied on a transition,
 * while {@code stepWindow} limits how many of the current step's own events are kept and is applied on every delivery. {@code stepWindow} is unbounded by default, so unless it is set, an instance parked in
 * one step while a large number of correlated events arrive keeps every one of them, whatever {@code historyWindow} is.
 * Set both and the total kept is at most {@code historyWindow + 2 * stepWindow + 1}, since a transition keeps the
 * step being left at its own cap while the step being entered fills its own cap before anything is evicted.
 * <p>
 * Dropping the current step's events would short-count a window condition if its counts had to be re-derived from them,
 * so {@link #stepConditionProgress()} carries the counts forward instead and a condition completes on the same event it
 * always would.
 *
 * <h2>Compatibility note</h2>
 * The bookkeeping fields ({@link #stepEntryIndex()}, {@link #windowStart()}, {@link #previousStep()},
 * {@link #previousStepEntryIndex()}, {@link #lastAction()}, {@link #matchedBranchIndex()},
 * {@link #stepConditionProgress()}) are an implementation detail of
 * the flow lowering. They are not a
 * stable wire format, their meaning can change between versions, and a store that persists a flow saga's state must round-
 * trip whatever it wrote without interpreting them. Only {@link #currentStep()}, {@link #received()} and
 * {@link #completed()} carry user-meaningful semantics. {@link ActionKind#JOIN} is one such implementation detail moving
 * between versions. Since ADR 120, every branch firing writes {@link ActionKind#BRANCH} instead, a lowered {@code join}
 * step included, so {@code JOIN} is never written by current code. The constant stays declared so a document a
 * pre-ADR-120 process wrote, and never re-evolved since, still round-trips instead of failing {@code ActionKind.valueOf}.
 * {@link #previousStepEntryIndex()} was added in 0.33.0, and {@code -1} means "not known", which is what a document
 * written before it existed reads back as. {@link #stepConditionProgress()} was added in 0.33.0 too, and {@code null}
 * means "not known" for it, since {@code 0} is a real count and {@code -1} keeps meaning only what it means on
 * {@link #previousStepEntryIndex()}.
 *
 * @param <E>                     the domain event type
 * @param previousStepEntryIndex  the absolute position {@link #previousStep()} was entered at, so {@code react} can hand a
 *                                window-condition reaction the window that fulfilled it rather than the whole retained
 *                                history. {@code -1} when no transition just happened, or when a store dropped the field,
 *                                in which case the reaction falls back to the whole retained history
 * @param stepConditionProgress   how many events in the current step's window have matched each of its window-condition
 *                                leaves so far, or {@code null} when that is not known, which is what a document written
 *                                before the field existed reads back as. The counts are re-derived from the window when
 *                                they are absent or no longer describe the step's declaration
 */
public record FlowStateImpl<E>(@Nullable String currentStep,
                               List<E> received,
                               int windowStart,
                               int stepEntryIndex,
                               boolean completed,
                               @Nullable String previousStep,
                               int previousStepEntryIndex,
                               ActionKind lastAction,
                               int matchedBranchIndex,
                               @Nullable StepConditionProgress stepConditionProgress) implements FlowState<E> {

    /**
     * What the last {@code evolve} did, so {@code react} knows which reaction to run. Internal bookkeeping.
     * {@link #JOIN} is retained for wire compatibility only. Current code always writes {@link #BRANCH}, see this
     * record's compatibility note.
     */
    public enum ActionKind {NONE, BRANCH, JOIN, TIMEOUT}

    /**
     * How many events have matched each window-condition leaf of the current step, in the order the step declares them
     * across all of its branches, together with the fingerprint of the declaration they were counted for. Internal
     * bookkeeping. A store round-trips both without interpreting either.
     *
     * @param leafFingerprint what the counts were counted for, so a declaration that changed under a parked instance is
     *                        noticed rather than silently read as the new one
     * @param matchCounts     one count per leaf, uncapped, so a count that a later declaration raises or lowers its
     *                        threshold on still means the same thing
     */
    public record StepConditionProgress(String leafFingerprint, List<Integer> matchCounts) {
        public StepConditionProgress {
            Objects.requireNonNull(leafFingerprint, "leafFingerprint cannot be null");
            Objects.requireNonNull(matchCounts, "matchCounts cannot be null");
            matchCounts = List.copyOf(matchCounts);
        }
    }

    public FlowStateImpl {
        Objects.requireNonNull(received, "received cannot be null");
        Objects.requireNonNull(lastAction, "lastAction cannot be null");
        received = List.copyOf(received);
    }

    /**
     * The pre-0.33.0 component list, without {@link #previousStepEntryIndex()} or {@link #stepConditionProgress()}. Kept
     * so a {@code SagaStateStore} written against the earlier record still compiles and still behaves. The absent entry
     * position becomes {@code -1}, so a window-condition reaction reads the whole retained history as it did before that
     * field existed, and the absent counts become {@code null}, so they are re-derived from the window.
     */
    public FlowStateImpl(@Nullable String currentStep, List<E> received, int windowStart, int stepEntryIndex,
                         boolean completed, @Nullable String previousStep, ActionKind lastAction, int matchedBranchIndex) {
        this(currentStep, received, windowStart, stepEntryIndex, completed, previousStep, -1, lastAction, matchedBranchIndex, null);
    }

    /** The initial state of a flow saga instance, before its start event has been applied. */
    public static <E> FlowStateImpl<E> initial() {
        return new FlowStateImpl<>(null, List.of(), 0, 0, false, null, -1, ActionKind.NONE, -1, null);
    }
}
