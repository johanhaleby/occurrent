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
import org.occurrent.dsl.saga.flow.FlowState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * The persisted record of one saga instance: its user state {@code S}, its lifecycle status, its pending timers, and the
 * bookkeeping the executor needs to be safe under at-least-once delivery. The envelope is the single source of truth for
 * the instance's timers (there is no separate scheduler), so a timer is exactly a {@link TimerEntry} here, saved together
 * with the state in one compare-and-set write.
 *
 * @param sagaId            the instance id (the saga's correlation id)
 * @param state             the user state {@code S}
 * @param status            whether the instance is active or completed
 * @param version           the optimistic-lock version, incremented on every save. A fresh instance is saved at version 1.
 * @param timers            the pending timers, by name
 * @param streamWatermarks  the highest stream version folded per stream id, for deduplicating redelivered stream events
 * @param positionWatermark the highest global position folded, for deduplicating redelivered position-carrying events
 * @param createdAt         when the instance was created
 * @param updatedAt         when the instance was last saved
 * @param completedAt       when the instance completed, or {@code null} while active
 * @param started           whether {@code onStart} has ever run for this instance. Almost always {@code true}: it is
 *                          {@code false} only for an instance whose very first event failed, which is recorded against
 *                          an envelope that exists purely to hold that record. Start detection reads this rather than
 *                          whether a document exists, so such an instance still starts properly once it is released.
 * @param failure           the input this instance is failing on, or {@code null} when it is not failing on anything.
 *                          Present from the first failure onwards, so it outlives a single attempt; {@code status}
 *                          says whether the failing has lasted past the runner's quarantine budget
 * @param currentStep       the step a flow saga is waiting in, derived from {@code state} whenever it is present. A
 *                          store may pass this directly, but only when it passes a {@code null} state: that is how a
 *                          store answers {@link SagaInstance#currentStep()} from a projected read without loading the
 *                          state at all. Whenever {@code state} is present the constructor re-derives this and ignores
 *                          what was passed, so the two can never disagree.
 * @param <S>               the user state type
 */
public record SagaEnvelope<S extends @Nullable Object>(String sagaId,
                                                       S state,
                                                       SagaStatus status,
                                                       long version,
                                                       List<TimerEntry> timers,
                                                       Map<String, Long> streamWatermarks,
                                                       @Nullable Long positionWatermark,
                                                       @Nullable Instant createdAt,
                                                       @Nullable Instant updatedAt,
                                                       @Nullable Instant completedAt,
                                                       @Nullable String currentStep,
                                                       boolean started,
                                                       @Nullable SagaFailure failure) implements SagaInstance {

    public SagaEnvelope {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(status, "status cannot be null");
        timers = List.copyOf(timers);
        streamWatermarks = Map.copyOf(streamWatermarks);
        if (state != null) {
            // currentStep duplicates something state already knows, which is the kind of field that drifts. Re-derive it
            // here rather than trusting callers to keep the two aligned: a passed value is honoured only when there is no
            // state to derive from, which is exactly the projected-read case a store needs it for.
            currentStep = state instanceof FlowState<?> flowState ? flowState.currentStep() : null;
        }
    }

    /**
     * An envelope for an instance that has started and is failing on nothing, which is every instance built before
     * quarantine existed.
     *
     * @deprecated since 0.34.0. Use the canonical constructor and pass {@code started} and {@code failure} explicitly.
     * This overload keeps a call site written against 0.33.0 compiling, and it can only ever answer {@code true} and
     * {@code null} for the two it does not take, so an envelope that really is quarantined cannot be built through it.
     */
    @Deprecated(since = "0.34.0")
    public SagaEnvelope(String sagaId, S state, SagaStatus status, long version, List<TimerEntry> timers,
                        Map<String, Long> streamWatermarks, @Nullable Long positionWatermark, @Nullable Instant createdAt,
                        @Nullable Instant updatedAt, @Nullable Instant completedAt, @Nullable String currentStep) {
        this(sagaId, state, status, version, timers, streamWatermarks, positionWatermark, createdAt, updatedAt,
                completedAt, currentStep, true, null);
    }

    /** A pending timer: its name and when it should fire (epoch millis). */
    public record TimerEntry(String name, long firesAtEpochMilli) {
        public TimerEntry {
            requireNonNull(name, "name cannot be null");
        }
    }

    /** Whether the instance has completed. */
    @Override
    public boolean isCompleted() {
        return status == SagaStatus.COMPLETED;
    }

    /** Whether the instance is quarantined at the position of an input it could not handle. */
    public boolean isQuarantined() {
        return status == SagaStatus.QUARANTINED;
    }

    /** The earliest firing time across all pending timers, or empty when there are none. Drives the due-timer query. */
    public OptionalLong earliestTimerFiresAtEpochMilli() {
        return timers.stream().mapToLong(TimerEntry::firesAtEpochMilli).min();
    }

    /**
     * {@inheritDoc}
     * <p>
     * The same instant as {@link #earliestTimerFiresAtEpochMilli()}, which stays in epoch millis because it is what
     * the poller's indexed query compares against.
     */
    @Override
    public @Nullable Instant nextTimerAt() {
        OptionalLong earliest = earliestTimerFiresAtEpochMilli();
        return earliest.isPresent() ? Instant.ofEpochMilli(earliest.getAsLong()) : null;
    }

}
