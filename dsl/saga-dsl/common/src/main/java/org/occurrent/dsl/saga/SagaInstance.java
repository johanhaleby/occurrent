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

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;

import java.time.Instant;

/**
 * The lifecycle of one saga instance, as something to observe rather than to act on. This is what an operational
 * question needs: is this instance still running, when did it last move, which step is it on, is its next timeout
 * overdue. Reach it through {@link SagaInstances}.
 * <p>
 * The surface is deliberately narrow. A saga's own state {@code S} is process-internal, so it is not here: a caller
 * that folds over it couples itself to how the process is implemented, and a read model shaped for querying belongs
 * in the projection DSL instead. Nor is the executor's delivery bookkeeping (the optimistic-lock version, the
 * redelivery watermarks, the pending timer names), which exists only so the executor is safe under at-least-once
 * delivery and means nothing outside it.
 * <p>
 * Note that {@link SagaEnvelope} implements this interface but stays the {@link SagaStateStore} type, so it still
 * exposes everything the executor needs. This interface narrows what an observing caller is handed. It does not put
 * the envelope's own components out of reach.
 */
public interface SagaInstance {

    /** The instance id, which is the saga's correlation id. */
    String sagaId();

    /** Whether the instance is still running or has completed. */
    SagaStatus status();

    /** Whether the instance has completed. A completed instance holds no timers and ignores further events. */
    boolean isCompleted();

    /**
     * The input this instance is failing on, or {@code null} when it is failing on nothing.
     * <p>
     * A non-null answer with a {@link SagaStatus#ACTIVE} status means an input has failed at least once and the
     * quarantine budget is still running, so the instance is expected to recover on its own. A non-null answer with a
     * {@link SagaStatus#QUARANTINED} status means the budget elapsed and the instance stopped, and the record says
     * where and why. Those two read the same way here on purpose, because the operational question is the same one:
     * what is this instance stuck on.
     */
    @Nullable SagaFailure failure();

    /** When the instance was created, or {@code null} if the store did not record it. */
    @Nullable Instant createdAt();

    /** When the instance was last saved, which is when it last folded an event or fired a timer. */
    @Nullable Instant updatedAt();

    /** When the instance completed, or {@code null} while it is still active. */
    @Nullable Instant completedAt();

    /**
     * When the earliest pending timer is due, or {@code null} when the instance has none. A timer whose due time is
     * well in the past is the signal that an instance is stuck: the poller should have fired it already.
     * <p>
     * The timer's <em>name</em> is not exposed, because which timers a saga arms is part of how the process is
     * written, not of its observable lifecycle.
     */
    @Nullable Instant nextTimerAt();

    /**
     * The step a flow saga is currently waiting in, or {@code null} for a saga written against the core builder, which
     * has named states only in its own state type rather than in a step the executor knows about.
     * <p>
     * A store is expected to answer this without loading the saga's state, so it is populated even on an instance
     * returned by a query that projected the state away.
     */
    @Nullable String currentStep();
}
