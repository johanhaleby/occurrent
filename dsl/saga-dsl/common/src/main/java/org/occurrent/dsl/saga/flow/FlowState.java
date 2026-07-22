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
 * A flow saga only ever produces this state through its executor, so you observe it (for example after reading an instance
 * back from a {@code SagaStateStore}) rather than construct it. The observable surface is deliberately small:
 * {@link #currentStep()}, {@link #received()} (or the richer {@link #receivedEvents()} view) and {@link #completed()}. The
 * flow lowering keeps additional transition bookkeeping to drive {@code evolve} and {@code react}, but that is an internal
 * implementation detail (see {@code org.occurrent.dsl.saga.flow.internal.FlowStateImpl}) and is not exposed here.
 * <p>
 * A store persists whatever bookkeeping the concrete state carries and rounds-trips it without interpreting it; only the
 * three components above carry user-meaningful semantics.
 *
 * @param <E> the domain event type
 * @see ReceivedEvents
 */
public interface FlowState<E> {

    /** The step the instance is currently in, or {@code null} before the start event has been applied or once completed. */
    @Nullable String currentStep();

    /**
     * The retained received events: the initiating event (always {@code received.get(0)}) plus the bounded window kept
     * behind the current step (see the flow builder's {@code historyWindow}). Not necessarily the full history.
     */
    List<E> received();

    /** Whether the instance has reached a terminal step. */
    boolean completed();

    /**
     * A view over the retained received events (the initiating event plus the bounded window), or {@code null} before the
     * start event has been applied.
     */
    default @Nullable ReceivedEvents<E> receivedEvents() {
        List<E> received = received();
        return received.isEmpty() ? null : ReceivedEvents.of(received);
    }
}
