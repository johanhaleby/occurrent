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
 * instance whose input kept failing for longer than the runner's quarantine budget becomes {@link #QUARANTINED}
 * instead, which is neither of those: it has stopped moving but it has not finished.
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
     * The instance is suspended at the position of an input it could not handle. It skips every event addressed to it
     * and fires no timers, so the subscription it shares with every other instance of the same saga is free to move on.
     * <p>
     * This is not terminal. {@link SagaInstance#failure()} says where the instance stopped and why, and releasing it
     * through {@code SagaSubscription.release(String)} replays the subscription from that position and makes it
     * {@link #ACTIVE} again.
     * <p>
     * Note that {@code findByStatus(ACTIVE, ...)} does not return a quarantined instance. Enumerate this status too when
     * you are looking for instances that have stopped moving.
     */
    QUARANTINED
}
