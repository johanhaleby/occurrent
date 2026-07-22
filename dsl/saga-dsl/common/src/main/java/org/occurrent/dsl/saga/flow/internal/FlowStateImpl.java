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
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.ReceivedEvents;

import java.util.List;
import java.util.Objects;

/**
 * The concrete state of a flow saga instance: the {@link FlowState} the executor evolves and the store persists. On top
 * of the user-meaningful {@link #currentStep()}, {@link #received()} and {@link #completed()} it carries the transition
 * bookkeeping the flow lowering writes in {@code evolve} and reads back in {@code react} ({@link #windowStart()},
 * {@link #stepEntryIndex()}, {@link #previousStep()}, {@link #lastAction()} and {@link #matchedBranchIndex()}), which is
 * part of the record only because {@code evolve} can communicate with {@code react} solely through the returned state.
 * <p>
 * This type is {@code public} only so that a {@code SagaStateStore} in another module can construct and read it; it is
 * not a user-facing API. Author flow sagas through {@link FlowState} and its builder instead.
 *
 * <h2>Bounded retention</h2>
 * {@link #received()} is <em>not</em> the full history. To keep a long-running instance from growing without bound (and to
 * cap per-save serialization cost), the flow lowering retains only a bounded window of received events: the initiating
 * event, always kept as {@code received.get(0)}, followed by the events from the current step's entry back through a
 * configurable carry-over of earlier events (see the flow builder's {@code historyWindow}). Events older than the window
 * are dropped, so a guard, a join reaction, or a timeout reaction that reads {@link ReceivedEvents} sees only the retained
 * window (the initiating event is the one exception, it is always available). {@link #windowStart()} is the absolute
 * position the retained tail begins at, which lets the join-matching window be reconstructed even after the prefix is
 * dropped. {@link #stepEntryIndex()} is an absolute position too, not an index into the (bounded) {@link #received()} list.
 *
 * <h2>Compatibility note</h2>
 * The bookkeeping fields ({@link #stepEntryIndex()}, {@link #windowStart()}, {@link #previousStep()},
 * {@link #lastAction()}, {@link #matchedBranchIndex()}) are an implementation detail of the flow lowering. They are not a
 * stable wire format: their meaning can change between versions, and a store that persists a flow saga's state must round-
 * trip whatever it wrote without interpreting them. Only {@link #currentStep()}, {@link #received()} and
 * {@link #completed()} carry user-meaningful semantics.
 *
 * @param <E> the domain event type
 */
public record FlowStateImpl<E>(@Nullable String currentStep,
                               List<E> received,
                               int windowStart,
                               int stepEntryIndex,
                               boolean completed,
                               @Nullable String previousStep,
                               ActionKind lastAction,
                               int matchedBranchIndex) implements FlowState<E> {

    /** What the last {@code evolve} did, so {@code react} knows which reaction to run. Internal bookkeeping. */
    public enum ActionKind {NONE, BRANCH, JOIN, TIMEOUT}

    public FlowStateImpl {
        Objects.requireNonNull(received, "received cannot be null");
        Objects.requireNonNull(lastAction, "lastAction cannot be null");
        received = List.copyOf(received);
    }

    /** The initial state of a flow saga instance, before its start event has been applied. */
    public static <E> FlowStateImpl<E> initial() {
        return new FlowStateImpl<>(null, List.of(), 0, 0, false, null, ActionKind.NONE, -1);
    }
}
