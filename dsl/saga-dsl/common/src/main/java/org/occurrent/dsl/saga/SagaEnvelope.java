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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

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
 * @param positionWatermark ignored. 0.31.0 to 0.34.0 stored the highest global position folded here, and the saga no
 *                          longer reads it, because a position says nothing about whether an event was already folded.
 *                          A stored value still loads, and the executor writes {@code null}. Deprecated since 0.35.0
 *                          and removed in a later release
 * @param createdAt         when the instance was created
 * @param updatedAt         when the instance was last saved
 * @param completedAt       when the instance completed, or {@code null} while active
 * @param started           whether a start transition has been saved for this instance, which is not the same as
 *                          whether {@code onStart} was ever called. An instance whose very first event failed after
 *                          {@code onStart} had run still saves {@code false}, because the transition it belonged to
 *                          was never saved, so the whole start is retried on a later redelivery. Start detection reads
 *                          this rather than whether a document exists
 * @param failure           the input this instance is failing on, or {@code null} when no failure has been recorded
 *                          for it, which does not mean it is not failing (see {@link SagaInstance#failure()}). Once
 *                          written it outlives a single attempt, and {@code status} says whether the instance was
 *                          quarantined for it
 * @param currentStep       the step a flow saga is waiting in, derived from {@code state} whenever it is present. A
 *                          store may pass this directly, but only when it passes a {@code null} state: that is how a
 *                          store answers {@link SagaInstance#currentStep()} from a projected read without loading the
 *                          state at all. Whenever {@code state} is present the constructor re-derives this and ignores
 *                          what was passed, so the two can never disagree.
 * @param streamMarks       what the executor knows about the versions it has applied per stream and event type, beyond the
 *                          highest one in {@code streamWatermarks}. It lets an event that arrives after a higher version
 *                          of the same type, which a feed that reorders can deliver, be told from a redelivery. Empty
 *                          for an instance stored before 0.35.0, which reads as every type of a stream being applied up to
 *                          that stream's watermark
 * @param heldEventsChecked whether the instance has drained every event held for it. Only the save that ends a drain
 *                          writes {@code true}, and holding an event unsets it, so an input finding it {@code false} on a
 *                          started instance drains first. Absent reads as {@code false}, so an instance stored before
 *                          0.35.0 drains once
 * @param drainedHeldKeys   the keys of the held events this instance applied since its last completed drain. They prove
 *                          an apply whose release did not happen, for example because the process stopped in between
 * @param selfHeldKeys      the keys of versions that failed on this instance and are held because they could not be
 *                          listed as pending. A version whose key is here is applied when it arrives, however far
 *                          behind it is, and applying it removes the key
 * @param startMark         the event that started the instance, or {@code null} for one stored before 0.35.0. An event
 *                          of the start's stream with a lower version committed before the start. On a saga whose one
 *                          start event type is a concrete class, such an event of another type is skipped when it is
 *                          delivered after the start, and the drain releases its held copy without applying it
 * @param <S>               the user state type
 */
public record SagaEnvelope<S extends @Nullable Object>(String sagaId,
                                                       S state,
                                                       SagaStatus status,
                                                       long version,
                                                       List<TimerEntry> timers,
                                                       Map<String, Long> streamWatermarks,
                                                       @Deprecated(since = "0.35.0") @Nullable Long positionWatermark,
                                                       @Nullable Instant createdAt,
                                                       @Nullable Instant updatedAt,
                                                       @Nullable Instant completedAt,
                                                       @Nullable String currentStep,
                                                       boolean started,
                                                       @Nullable SagaFailure failure,
                                                       Map<String, StreamMarks> streamMarks,
                                                       boolean heldEventsChecked,
                                                       Set<String> drainedHeldKeys,
                                                       Set<String> selfHeldKeys,
                                                       @Nullable StartMark startMark) implements SagaInstance {

    public SagaEnvelope {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(status, "status cannot be null");
        timers = List.copyOf(timers);
        streamWatermarks = Map.copyOf(streamWatermarks);
        streamMarks = Map.copyOf(streamMarks);
        drainedHeldKeys = Set.copyOf(drainedHeldKeys);
        selfHeldKeys = Set.copyOf(selfHeldKeys);
        if (state != null) {
            // currentStep duplicates something state already knows, which is the kind of field that drifts. Re-derive it
            // here rather than trusting callers to keep the two aligned: a passed value is honoured only when there is no
            // state to derive from, which is exactly the projected-read case a store needs it for.
            currentStep = state instanceof FlowState<?> flowState ? flowState.currentStep() : null;
        }
    }

    /**
     * An envelope with nothing held for it and no start mark, which reads as an instance with a drain due.
     */
    public SagaEnvelope(String sagaId, S state, SagaStatus status, long version, List<TimerEntry> timers,
                        Map<String, Long> streamWatermarks, @Nullable Long positionWatermark, @Nullable Instant createdAt,
                        @Nullable Instant updatedAt, @Nullable Instant completedAt, @Nullable String currentStep,
                        boolean started, @Nullable SagaFailure failure, Map<String, StreamMarks> streamMarks) {
        this(sagaId, state, status, version, timers, streamWatermarks, positionWatermark, createdAt, updatedAt,
                completedAt, currentStep, started, failure, streamMarks, false, Set.of(), Set.of(), null);
    }

    /**
     * An envelope with no stream marks, which is every instance stored before 0.35.0. It reads as every event type of a
     * stream being applied up to that stream's watermark.
     */
    public SagaEnvelope(String sagaId, S state, SagaStatus status, long version, List<TimerEntry> timers,
                        Map<String, Long> streamWatermarks, @Nullable Long positionWatermark, @Nullable Instant createdAt,
                        @Nullable Instant updatedAt, @Nullable Instant completedAt, @Nullable String currentStep,
                        boolean started, @Nullable SagaFailure failure) {
        this(sagaId, state, status, version, timers, streamWatermarks, positionWatermark, createdAt, updatedAt,
                completedAt, currentStep, started, failure, Map.of());
    }

    /**
     * An envelope for an instance that has started and has no failure recorded, which is every instance built before
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

    /**
     * What is known of one stream's versions beyond its watermark.
     *
     * @param floor  every version at or below this was applied before the executor started recording versions per
     *               event type, so it is skipped whatever its type
     * @param byType the versions applied per cloud event type
     * @param pending the versions per cloud event type that were handed to the executor and failed, ascending, so still
     *                outstanding. A version in here is applied when it arrives again however far behind the highest
     *                one applied it is, because it is known not to have been applied. Applying it removes it, and
     *                nothing else does
     * @param legacyFailed the versions of the stream whose failure was recorded before the executor listed failed versions
     *                as pending, ascending, so the event type is not known. A stream version names exactly one event, so
     *                the version is enough to recognise it. It is treated like a pending version and removed when the
     *                version is applied. It is filled only when such a failure record is replaced by another input's, so
     *                it holds at most the one record an instance carried over from before
     * @param pendingSince when each version in {@code pending} first failed, by version. The quarantine budget of an input
     *                runs from here, so a failure of one input never moves the clock of another. A pending version
     *                missing from it falls back to the failure record's {@code firstFailedAt} when the record names it,
     *                and otherwise to the moment it fails again
     */
    public record StreamMarks(long floor, Map<String, TypeMark> byType, Map<String, List<Long>> pending, List<Long> legacyFailed,
                              Map<Long, Instant> pendingSince) {
        public StreamMarks {
            legacyFailed = List.copyOf(legacyFailed);
            byType = Map.copyOf(byType);
            Map<String, List<Long>> copied = new HashMap<>();
            pending.forEach((type, versions) -> copied.put(type, List.copyOf(versions)));
            pending = Map.copyOf(copied);
            pendingSince = Map.copyOf(pendingSince);
        }

        /** Marks that know no instant for their pending versions. */
        public StreamMarks(long floor, Map<String, TypeMark> byType, Map<String, List<Long>> pending, List<Long> legacyFailed) {
            this(floor, byType, pending, legacyFailed, Map.of());
        }

        /** Marks with no failure recorded before pending existed. */
        public StreamMarks(long floor, Map<String, TypeMark> byType, Map<String, List<Long>> pending) {
            this(floor, byType, pending, List.of());
        }

        /** Marks with nothing outstanding. */
        public StreamMarks(long floor, Map<String, TypeMark> byType) {
            this(floor, byType, Map.of());
        }
    }

    /**
     * The versions of one event type applied from one stream.
     *
     * @param mark    the highest version applied
     * @param applied the versions below {@code mark} that were applied, ascending. A version below {@code mark} that is
     *                not in here and not too far behind it was never applied, so it is applied when it arrives
     * @param horizon the highest version this mark has stopped remembering, under the reorder window in force when it was
     *                recorded. A version at or below it is skipped even if a larger window is configured later, since
     *                the versions between the two windows are no longer in {@code applied}
     */
    public record TypeMark(long mark, List<Long> applied, long horizon) {
        public TypeMark {
            applied = List.copyOf(applied);
        }
    }

    /**
     * The event that started an instance.
     *
     * @param streamId the stream the start was written to
     * @param version  the start's stream version
     * @param position the start's global position, or {@code null} when it carried none
     */
    public record StartMark(String streamId, long version, @Nullable Long position) {
        public StartMark {
            requireNonNull(streamId, "streamId cannot be null");
        }
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

    /** Whether the instance is quarantined on an input it could not handle. */
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
