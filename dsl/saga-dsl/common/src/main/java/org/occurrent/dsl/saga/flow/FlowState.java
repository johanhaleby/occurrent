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

import java.util.List;

/**
 * The state of a flow saga instance: which step it is in, the events it has received, and whether it has completed. This
 * is the {@code S} of the {@code Saga<E, FlowState<E>, C>} a flow saga compiles to, so it is what the executor persists.
 * <p>
 * The genuinely user-meaningful parts are {@link #currentStep()}, {@link #received()} and {@link #completed()}. The
 * remaining components ({@link #stepEntryIndex()}, {@link #previousStep()}, {@link #lastAction()} and
 * {@link #matchedBranchIndex()}) are transition bookkeeping the flow lowering writes in {@code evolve} and reads back in
 * {@code react}, and they are part of the record only because {@code evolve} can communicate with {@code react} solely
 * through the returned state.
 *
 * @param <E> the domain event type
 */
public record FlowState<E>(@Nullable String currentStep,
                           List<E> received,
                           int stepEntryIndex,
                           boolean completed,
                           @Nullable String previousStep,
                           ActionKind lastAction,
                           int matchedBranchIndex) {

    /** What the last {@code evolve} did, so {@code react} knows which reaction to run. Internal bookkeeping. */
    public enum ActionKind {NONE, BRANCH, JOIN, TIMEOUT}

    public FlowState {
        received = List.copyOf(received);
    }

    /** The initial state of a flow saga instance, before its start event has been applied. */
    public static <E> FlowState<E> initial() {
        return new FlowState<>(null, List.of(), 0, false, null, ActionKind.NONE, -1);
    }

    /** A view over the received events, or {@code null} before the start event has been applied. */
    public @Nullable ReceivedEvents<E> receivedEvents() {
        return received.isEmpty() ? null : ReceivedEvents.of(received);
    }
}
