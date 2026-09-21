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

/**
 * Where a saga instance is in its lifecycle. An instance is {@link #ACTIVE} until its saga reaches a terminal state,
 * at which point it becomes {@link #COMPLETED} and absorbing: it cancels its timers and skips any further event. An
 * instance that keeps failing can become {@link #QUARANTINED} instead, which is neither of those, because it has
 * stopped moving but it has not finished. That constant lists when it happens.
 * <p>
 * This is a top-level type rather than a member of {@link SagaEnvelope} because it is part of the user-facing
 * {@link SagaInstance} view as well as the {@link SagaStateStore} SPI, and a narrow observation interface should not
 * have to name the store's envelope type in its own signature.
 */
public enum SagaStatus {
    /** The instance is running: it still folds events and can still fire timers. */
    ACTIVE,

    /** The instance reached a terminal state. It holds no timers and ignores further events. */
    COMPLETED,

    /**
     * The instance is suspended on an input it could not handle. It skips every event addressed to it and fires no
     * timers, so the subscription it shares with every other instance of the same saga is free to move on.
     * <p>
     * This is not terminal, but nothing in 0.34.0 brings an instance out of it. {@link SagaInstance#failure()} says
     * which input the instance stopped on, when it started failing, and what the saga threw.
     * {@code SagaStateStore.delete(sagaId)} abandons the instance once you have decided not to recover it.
     * <p>
     * Note that {@code findByStatus(ACTIVE, ...)} does not return a quarantined instance. Enumerate this status too when
     * you are looking for instances that have stopped moving.
     * <p>
     * The blocking {@code SagaRunner} writes this status, and only for an event that failed. A failing timeout never
     * quarantines an instance. The runner decides in two steps, and the second can refuse after the first has said yes.
     * <p>
     * A failing event is considered for quarantine only when all of these hold.
     * <ul>
     *   <li>The saga worked out which instance the event belongs to. A failure in the converter or the id extractor is
     *       never charged to an instance.</li>
     *   <li>{@code SagaRunnerConfig.quarantineAfter} is set, and the runner did not switch it off at startup. It does
     *       that on a subscription model that cannot guarantee it holds every event it delivers.</li>
     *   <li>The event carries a redelivery key, which is a stream id with its stream version or a global position.</li>
     *   <li>What was thrown is not an {@link OutOfMemoryError}.</li>
     *   <li>The instance is {@link #ACTIVE} and does not already count the event as handled.</li>
     *   <li>The instance has been failing for at least {@code quarantineAfter}, measured from
     *       {@link SagaFailure#firstFailedAt()}. The failure that first writes that record is never considered.</li>
     * </ul>
     * An event that meets all of them is quarantined only when all of these hold as well, and any one of them can refuse
     * on its own.
     * <ul>
     *   <li>The subscription model confirms, for that event, that acknowledging it is not what would destroy the last
     *       copy of it. A check that throws counts as a refusal. The runner logs a WARN for a refusal, though
     *       not for every repeat of it.</li>
     *   <li>The store read the decision is made on and the write that records it both complete. When either throws, the
     *       runner logs nothing of its own about the quarantine.</li>
     *   <li>The compare-and-set write wins. When another writer changed the instance first, the write is discarded
     *       without a log line, and the runner cannot tell what that writer did.</li>
     * </ul>
     * When every condition holds, the instance is written with this status and the runner returns normally instead of
     * rethrowing. When any one does not, a failure propagates to the subscription model, and whether the event is
     * offered again is that model's to decide.
     * <p>
     * So an instance that has been failing for longer than {@code quarantineAfter} can still be {@link #ACTIVE}, and the
     * logs alone do not tell you which status it has. Read its status with {@code SagaInstances.find} or
     * {@code findByStatus} instead.
     */
    QUARANTINED
}
