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
                                                       @Nullable String currentStep) implements SagaInstance {

    public SagaEnvelope {
        timers = List.copyOf(timers);
        streamWatermarks = Map.copyOf(streamWatermarks);
        if (state != null) {
            // currentStep duplicates something state already knows, which is the kind of field that drifts. Re-derive it
            // here rather than trusting callers to keep the two aligned: a passed value is honoured only when there is no
            // state to derive from, which is exactly the projected-read case a store needs it for.
            currentStep = state instanceof FlowState<?> flowState ? flowState.currentStep() : null;
        }
    }

    /** A pending timer: its name and when it should fire (epoch millis). */
    public record TimerEntry(String name, long firesAtEpochMilli) {
    }

    /** Whether the instance has completed. */
    @Override
    public boolean isCompleted() {
        return status == SagaStatus.COMPLETED;
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
