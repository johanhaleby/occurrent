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

package org.occurrent.dsl.saga.codec;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Turns a {@link SagaEnvelope} into the {@link StoredSaga} a store keeps, and back. It is the only place that knows the
 * field names, the stored statuses, how a flow saga's state is written and what a field missing from stored data means.
 * A store never builds or reads an envelope.
 * <p>
 * The field names, their order and the value types are those of the MongoDB document 0.34.0 and 0.35.0 write, so an
 * instance stored by either reads back and a store that keeps the layout needs no data migration. A field missing from
 * data written by 0.34.0 reads as the value that was true for that data. The map keys of {@code streamWatermarks} and
 * {@code streamMarks} are written in sorted order.
 * <p>
 * A flow saga's state is written field by field, with its received events as {@code CloudEvent} values made by the
 * {@link CloudEventConverter} given to {@link #of}. A store that writes them as CloudEvent JSON keeps the events by
 * their CloudEvent type, so a domain event can move to another package. The codec warns once an instance retains 1000
 * events, and again after it has dropped below that and crossed it anew.
 *
 * @param <S> the user state type
 */
public final class SagaCodec<S extends @Nullable Object> {

    private static final Logger log = LoggerFactory.getLogger(SagaCodec.class);

    public static final String STATE = "state";
    public static final String STATUS = "status";
    public static final String VERSION = "version";
    public static final String TIMERS = "timers";
    public static final String NEXT_TIMER_FIRES_AT = "nextTimerFiresAt";
    public static final String CURRENT_STEP = "currentStep";
    public static final String STREAM_WATERMARKS = "streamWatermarks";
    public static final String POSITION_WATERMARK = "positionWatermark";
    public static final String STREAM_MARKS = "streamMarks";
    public static final String CREATED_AT = "createdAt";
    public static final String UPDATED_AT = "updatedAt";
    public static final String COMPLETED_AT = "completedAt";
    public static final String STARTED = "started";
    public static final String FAILURE_INPUT = "failureInput";
    public static final String FAILURE_POSITION = "failurePosition";
    public static final String FAILURE_FIRST_FAILED_AT = "failureFirstFailedAt";
    public static final String FAILURE_TYPE = "failureType";
    public static final String FAILURE_MESSAGE = "failureMessage";
    public static final String FAILURE_RELEASED_AT = "failureReleasedAt";
    public static final String FAILURE_QUARANTINE_ID = "failureQuarantineId";
    public static final String QUARANTINE_SKIP_COUNT = "quarantineSkipCount";

    private static final String TIMER_NAME = "name";
    private static final String TIMER_FIRES_AT = "firesAtEpochMilli";
    private static final String TIMER_NEXT_ATTEMPT_AT = "nextAttemptAtEpochMilli";
    private static final String MARKS_STREAM_ID = "streamId";
    private static final String MARKS_FLOOR = "floor";
    private static final String MARKS_TYPES = "types";
    private static final String MARKS_TYPE = "type";
    private static final String MARKS_MARK = "mark";
    private static final String MARKS_APPLIED = "applied";
    private static final String MARKS_HORIZON = "horizon";
    private static final String MARKS_PENDING = "pending";
    private static final String MARKS_VERSIONS = "versions";
    private static final String MARKS_LEGACY_FAILED = "legacyFailed";
    private static final String MARKS_PENDING_SINCE = "pendingSince";
    private static final String MARKS_VERSION = "version";
    private static final String MARKS_SINCE = "since";

    private static final String FLOW_CURRENT_STEP = "currentStep";
    private static final String FLOW_WINDOW_START = "windowStart";
    private static final String FLOW_STEP_ENTRY_INDEX = "stepEntryIndex";
    private static final String FLOW_COMPLETED = "completed";
    private static final String FLOW_PREVIOUS_STEP = "previousStep";
    private static final String FLOW_PREVIOUS_STEP_ENTRY_INDEX = "previousStepEntryIndex";
    private static final String FLOW_LAST_ACTION = "lastAction";
    private static final String FLOW_MATCHED_BRANCH_INDEX = "matchedBranchIndex";
    /** The field of a flow saga's encoded state that lists its received events, each a {@code CloudEvent}. */
    public static final String FLOW_RECEIVED = "received";
    private static final String FLOW_STEP_CONDITION_PROGRESS = "stepConditionProgress";
    private static final String FLOW_LEAF_FINGERPRINT = "leafFingerprint";
    private static final String FLOW_MATCH_COUNTS = "matchCounts";

    /**
     * The status an instance is stored with when it is quarantined with a quarantine id, which is every instance 0.35.0
     * or later quarantines. It reads back as {@link SagaStatus#QUARANTINED}. 0.34.0 decodes the status with
     * {@code SagaStatus.valueOf} and throws on a string it does not know, so a 0.34.0 node that meets such an instance
     * during a rolling upgrade fails on it and retries it, rather than skipping its inputs without recording them, which
     * would leave a release with inputs it never replays.
     */
    public static final String QUARANTINED_RECORDING = "QUARANTINED_RECORDING";

    /**
     * The status an {@link SagaStatus#ACTIVE} instance is stored with while it holds stream marks, which every input
     * with a stream version adds. It reads back as {@link SagaStatus#ACTIVE}. 0.34.0 saves the whole document and
     * knows nothing of the marks, so a save from a 0.34.0 node drops them, and 0.35.0 then skips a pending or late input
     * as applied already. 0.34.0 throws on this string the same way it does on {@link #QUARANTINED_RECORDING}, so a
     * 0.34.0 node that meets such an instance during a rolling upgrade fails on it and retries it instead of saving it.
     * Its timer poller matches only {@code ACTIVE}, so the timers of such an instance wait for a 0.35.0 node.
     */
    public static final String ACTIVE_MARKED = "ACTIVE_MARKED";

    private static final Set<String> ACTIVE_STATUSES = Set.of(SagaStatus.ACTIVE.name(), ACTIVE_MARKED);
    private static final Set<String> QUARANTINED_STATUSES = Set.of(SagaStatus.QUARANTINED.name(), QUARANTINED_RECORDING);

    // Not currentStep, which is derived from the state, so writing it from an envelope whose state is ignored would move
    // the step away from the state it describes.
    private static final List<String> FIELDS_EXCEPT_STATE = List.of(STATUS, VERSION, TIMERS, NEXT_TIMER_FIRES_AT,
            STREAM_WATERMARKS, POSITION_WATERMARK, STREAM_MARKS, CREATED_AT, UPDATED_AT, COMPLETED_AT, STARTED,
            FAILURE_INPUT, FAILURE_POSITION, FAILURE_FIRST_FAILED_AT, FAILURE_TYPE, FAILURE_MESSAGE,
            FAILURE_RELEASED_AT, FAILURE_QUARANTINE_ID, QUARANTINE_SKIP_COUNT);

    private static final List<String> INSTANCE_FIELDS = List.of(STATUS, VERSION, TIMERS, NEXT_TIMER_FIRES_AT,
            CURRENT_STEP, CREATED_AT, UPDATED_AT, COMPLETED_AT, STARTED, FAILURE_INPUT, FAILURE_POSITION,
            FAILURE_FIRST_FAILED_AT, FAILURE_TYPE, FAILURE_MESSAGE, FAILURE_RELEASED_AT, FAILURE_QUARANTINE_ID,
            QUARANTINE_SKIP_COUNT);

    private static final List<String> FIELDS_WITHOUT_STATE = List.of(STATUS, VERSION, TIMERS, NEXT_TIMER_FIRES_AT,
            CURRENT_STEP, STREAM_WATERMARKS, POSITION_WATERMARK, STREAM_MARKS, CREATED_AT, UPDATED_AT, COMPLETED_AT,
            STARTED, FAILURE_INPUT, FAILURE_POSITION, FAILURE_FIRST_FAILED_AT, FAILURE_TYPE, FAILURE_MESSAGE,
            FAILURE_RELEASED_AT, FAILURE_QUARANTINE_ID, QUARANTINE_SKIP_COUNT);

    // A round number one to two orders of magnitude below MongoDB's 16 MB document limit for typical CloudEvent sizes. It
    // is a warning and not a cap, and the remedy, a stepWindow on the flow, is configurable per saga.
    static final int RETAINED_EVENT_WARNING_THRESHOLD = 1_000;

    // Bounds the size of the latch below. An instance that is retired by a TTL index never reaches delete, so its entry
    // stays until the instance that was checked least recently is evicted. That instance warns again on its next save.
    static final int RETAINED_EVENT_WARNING_LATCH_CAPACITY = 10_000;

    // Holds a saga id for as long as it has warned and not dropped below the threshold. Access-ordered so the instance
    // checked least recently is the one evicted. Every access goes through a synchronized block.
    private final Map<String, Boolean> retainedEventWarningLatch = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > RETAINED_EVENT_WARNING_LATCH_CAPACITY;
        }
    };

    private final SagaStateCodec<S> stateCodec;
    private final @Nullable CloudEventConverter<Object> flowEvents;

    private SagaCodec(SagaStateCodec<S> stateCodec, @Nullable CloudEventConverter<Object> flowEvents) {
        this.stateCodec = stateCodec;
        this.flowEvents = flowEvents;
    }

    /**
     * @param stateCodec the codec of a state that is not a flow saga's
     * @param flowEvents the converter for the received events of a flow saga, or {@code null} for a saga that is not a
     *                   flow saga. With a converter, a stored state that is a map is read as a flow saga's state, and a
     *                   {@link FlowState} other than the flow executor's is refused
     */
    @SuppressWarnings("unchecked")
    public static <S extends @Nullable Object> SagaCodec<S> of(SagaStateCodec<S> stateCodec, @Nullable CloudEventConverter<?> flowEvents) {
        return new SagaCodec<>(requireNonNull(stateCodec, "stateCodec cannot be null"), (CloudEventConverter<Object>) flowEvents);
    }

    /** The whole instance, state included. */
    public StoredSaga encode(SagaEnvelope<S> envelope) {
        return encode(envelope, true);
    }

    /** The instance without its state and without {@code currentStep}, which is derived from the state. */
    public StoredSaga encodeWithoutState(SagaEnvelope<S> envelope) {
        return encode(envelope, false);
    }

    /**
     * The instance. A state that is missing from {@code stored}, as it is from a read that projects it away, gives an
     * envelope with a {@code null} state and the stored {@code currentStep}.
     */
    public SagaEnvelope<S> decode(StoredSaga stored) {
        return decode(stored, true);
    }

    /** The instance without reading its state, so it decodes even when the state no longer does. */
    public SagaEnvelope<S> decodeWithoutState(StoredSaga stored) {
        return decode(stored, false);
    }

    /** Forgets that {@code sagaId} has warned about its retained events, for an instance that is deleted. */
    public void forget(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        synchronized (retainedEventWarningLatch) {
            retainedEventWarningLatch.remove(sagaId);
        }
    }

    /** The stored statuses that stand for {@code status}, which is two for {@code ACTIVE} and for {@code QUARANTINED}. */
    public static Set<String> storedStatuses(SagaStatus status) {
        requireNonNull(status, "status cannot be null");
        return switch (status) {
            case ACTIVE -> ACTIVE_STATUSES;
            case QUARANTINED -> QUARANTINED_STATUSES;
            default -> Set.of(status.name());
        };
    }

    /** The fields {@code compareAndSaveWithoutState} sets, or unsets when the saga has no value for one. */
    public static List<String> fieldsExceptState() {
        return FIELDS_EXCEPT_STATE;
    }

    /** The fields to read for an enumeration, which is every field of an instance except the state and what a store keeps only for the executor. */
    public static List<String> instanceFields() {
        return INSTANCE_FIELDS;
    }

    /** The fields to read for {@code findWithoutState}, which is {@link #instanceFields()} and the watermarks and marks. */
    public static List<String> fieldsWithoutState() {
        return FIELDS_WITHOUT_STATE;
    }

    /** The index of a saga read back as {@code encoded}, which a store builds when it reads only the fields it queries on. */
    public static SagaIndex indexOf(EncodedSaga encoded) {
        Map<String, Object> fields = requireNonNull(encoded, "encoded cannot be null").fields();
        String storedStatus = (String) fields.get(STATUS);
        Object nextTimer = fields.get(NEXT_TIMER_FIRES_AT);
        Object updatedAt = fields.get(UPDATED_AT);
        boolean releasing = fields.get(FAILURE_RELEASED_AT) != null && QUARANTINED_STATUSES.contains(storedStatus);
        return new SagaIndex(storedStatus, nextTimer == null ? null : ((Number) nextTimer).longValue(),
                updatedAt == null ? null : ((Number) updatedAt).longValue(), releasing, (String) fields.get(FAILURE_QUARANTINE_ID));
    }

    private StoredSaga encode(SagaEnvelope<S> envelope, boolean withState) {
        requireNonNull(envelope, "envelope cannot be null");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put(STATUS, storedStatus(envelope));
        fields.put(VERSION, envelope.version());
        List<Object> timers = new ArrayList<>();
        for (TimerEntry timer : envelope.timers()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put(TIMER_NAME, timer.name());
            entry.put(TIMER_FIRES_AT, timer.firesAtEpochMilli());
            if (timer.nextAttemptAtEpochMilli() != null) {
                entry.put(TIMER_NEXT_ATTEMPT_AT, timer.nextAttemptAtEpochMilli());
            }
            timers.add(entry);
        }
        fields.put(TIMERS, timers);
        envelope.earliestDueAtEpochMilli().ifPresent(next -> fields.put(NEXT_TIMER_FIRES_AT, next));
        if (withState && envelope.currentStep() != null) {
            fields.put(CURRENT_STEP, envelope.currentStep());
        }
        fields.put(STREAM_WATERMARKS, new LinkedHashMap<>(new TreeMap<>(envelope.streamWatermarks())));
        if (envelope.positionWatermark() != null) {
            fields.put(POSITION_WATERMARK, envelope.positionWatermark());
        }
        if (!envelope.streamMarks().isEmpty()) {
            fields.put(STREAM_MARKS, encodeMarks(envelope.streamMarks()));
        }
        putInstant(fields, CREATED_AT, envelope.createdAt());
        putInstant(fields, UPDATED_AT, envelope.updatedAt());
        putInstant(fields, COMPLETED_AT, envelope.completedAt());
        // Only when false, so data written before 0.34.0 and an instance that has started are the same.
        if (!envelope.started()) {
            fields.put(STARTED, false);
        }
        SagaFailure failure = envelope.failure();
        if (failure != null) {
            fields.put(FAILURE_INPUT, failure.input());
            fields.put(FAILURE_FIRST_FAILED_AT, failure.firstFailedAt().toEpochMilli());
            fields.put(FAILURE_TYPE, failure.failureType());
            if (failure.position() != null) {
                fields.put(FAILURE_POSITION, failure.position());
            }
            if (failure.failureMessage() != null) {
                fields.put(FAILURE_MESSAGE, failure.failureMessage());
            }
            putInstant(fields, FAILURE_RELEASED_AT, failure.releasedAt());
            if (failure.quarantineId() != null) {
                fields.put(FAILURE_QUARANTINE_ID, failure.quarantineId());
            }
        }
        if (envelope.quarantineSkipCount() != null) {
            fields.put(QUARANTINE_SKIP_COUNT, envelope.quarantineSkipCount());
        }
        S state = envelope.state();
        if (withState && state != null) {
            fields.put(STATE, encodeState(envelope.sagaId(), state));
        }
        EncodedSaga encoded = new EncodedSaga(fields);
        return new StoredSaga(envelope.sagaId(), envelope.version(), encoded, indexOf(encoded));
    }

    private static String storedStatus(SagaEnvelope<?> envelope) {
        if (envelope.quarantineId() != null) {
            return QUARANTINED_RECORDING;
        }
        if (envelope.status() == SagaStatus.ACTIVE && !envelope.streamMarks().isEmpty()) {
            return ACTIVE_MARKED;
        }
        return envelope.status().name();
    }

    private static SagaStatus readStatus(String stored) {
        return switch (stored) {
            case QUARANTINED_RECORDING -> SagaStatus.QUARANTINED;
            case ACTIVE_MARKED -> SagaStatus.ACTIVE;
            default -> SagaStatus.valueOf(stored);
        };
    }

    private SagaEnvelope<S> decode(StoredSaga stored, boolean withState) {
        requireNonNull(stored, "stored cannot be null");
        Map<String, Object> fields = stored.encoded().fields();
        Object encodedState = withState ? fields.get(STATE) : null;
        S state = encodedState == null ? null : decodeState(encodedState);

        List<TimerEntry> timers = new ArrayList<>();
        for (Object timer : list(fields.get(TIMERS))) {
            Map<String, Object> entry = map(timer);
            timers.add(new TimerEntry((String) entry.get(TIMER_NAME), longOf(entry.get(TIMER_FIRES_AT)), nullableLong(entry.get(TIMER_NEXT_ATTEMPT_AT))));
        }

        Map<String, Long> streamWatermarks = new LinkedHashMap<>();
        if (fields.get(STREAM_WATERMARKS) != null) {
            map(fields.get(STREAM_WATERMARKS)).forEach((streamId, version) -> streamWatermarks.put(streamId, longOf(version)));
        }

        // Data written before 0.34.0 has no started flag, and every instance in it had started.
        boolean started = !fields.containsKey(STARTED) || (Boolean) fields.get(STARTED);

        // The envelope honours the stored currentStep only when there is no state to derive it from.
        return new SagaEnvelope<>(stored.sagaId(), state, readStatus((String) fields.get(STATUS)), stored.version(), timers,
                streamWatermarks, nullableLong(fields.get(POSITION_WATERMARK)), instant(fields.get(CREATED_AT)),
                instant(fields.get(UPDATED_AT)), instant(fields.get(COMPLETED_AT)), (String) fields.get(CURRENT_STEP), started,
                decodeFailure(fields), decodeMarks(fields.get(STREAM_MARKS)), nullableLong(fields.get(QUARANTINE_SKIP_COUNT)));
    }

    private static @Nullable SagaFailure decodeFailure(Map<String, Object> fields) {
        String input = (String) fields.get(FAILURE_INPUT);
        if (input == null) {
            return null;
        }
        return new SagaFailure(input, nullableLong(fields.get(FAILURE_POSITION)),
                requireNonNull(instant(fields.get(FAILURE_FIRST_FAILED_AT)), FAILURE_FIRST_FAILED_AT),
                requireNonNull((String) fields.get(FAILURE_TYPE), FAILURE_TYPE),
                (String) fields.get(FAILURE_MESSAGE),
                instant(fields.get(FAILURE_RELEASED_AT)),
                (String) fields.get(FAILURE_QUARANTINE_ID));
    }

    private static List<Object> encodeMarks(Map<String, StreamMarks> streamMarks) {
        List<Object> streams = new ArrayList<>();
        new TreeMap<>(streamMarks).forEach((streamId, marks) -> {
            List<Object> types = new ArrayList<>();
            new TreeMap<>(marks.byType()).forEach((type, typeMark) -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put(MARKS_TYPE, type);
                entry.put(MARKS_MARK, typeMark.mark());
                entry.put(MARKS_APPLIED, new ArrayList<>(typeMark.applied()));
                entry.put(MARKS_HORIZON, typeMark.horizon());
                types.add(entry);
            });
            List<Object> pending = new ArrayList<>();
            new TreeMap<>(marks.pending()).forEach((type, versions) -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put(MARKS_TYPE, type);
                entry.put(MARKS_VERSIONS, new ArrayList<>(versions));
                pending.add(entry);
            });
            List<Object> pendingSince = new ArrayList<>();
            new TreeMap<>(marks.pendingSince()).forEach((version, since) -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put(MARKS_VERSION, version);
                // Truncated to millis, the precision every other instant of the instance is kept at
                entry.put(MARKS_SINCE, since.truncatedTo(ChronoUnit.MILLIS));
                pendingSince.add(entry);
            });
            Map<String, Object> stream = new LinkedHashMap<>();
            stream.put(MARKS_STREAM_ID, streamId);
            stream.put(MARKS_FLOOR, marks.floor());
            stream.put(MARKS_TYPES, types);
            stream.put(MARKS_PENDING, pending);
            stream.put(MARKS_LEGACY_FAILED, new ArrayList<>(marks.legacyFailed()));
            stream.put(MARKS_PENDING_SINCE, pendingSince);
            streams.add(stream);
        });
        return streams;
    }

    // Data written before 0.35.0 has no marks, which reads as every event type of a stream being applied up to the
    // stream's watermark.
    private static Map<String, StreamMarks> decodeMarks(@Nullable Object stored) {
        Map<String, StreamMarks> streamMarks = new LinkedHashMap<>();
        for (Object streamValue : list(stored)) {
            Map<String, Object> stream = map(streamValue);
            Map<String, TypeMark> byType = new LinkedHashMap<>();
            for (Object typeValue : list(stream.get(MARKS_TYPES))) {
                Map<String, Object> type = map(typeValue);
                Object horizon = type.get(MARKS_HORIZON);
                byType.put((String) type.get(MARKS_TYPE), new TypeMark(longOf(type.get(MARKS_MARK)), longs(type.get(MARKS_APPLIED)),
                        horizon == null ? Long.MIN_VALUE : longOf(horizon)));
            }
            // A stream stored before pending existed has no field, which reads as nothing outstanding.
            Map<String, List<Long>> pending = new LinkedHashMap<>();
            for (Object entryValue : list(stream.get(MARKS_PENDING))) {
                Map<String, Object> entry = map(entryValue);
                pending.put((String) entry.get(MARKS_TYPE), longs(entry.get(MARKS_VERSIONS)));
            }
            // The same for a stream stored before failures of unknown type were kept.
            List<Long> legacyFailed = longs(stream.get(MARKS_LEGACY_FAILED));
            // A stream stored before the instant was kept reads as no instant, and the budget then falls back to the
            // failure record.
            Map<Long, Instant> pendingSince = new LinkedHashMap<>();
            for (Object entryValue : list(stream.get(MARKS_PENDING_SINCE))) {
                Map<String, Object> entry = map(entryValue);
                pendingSince.put(longOf(entry.get(MARKS_VERSION)), (Instant) entry.get(MARKS_SINCE));
            }
            streamMarks.put((String) stream.get(MARKS_STREAM_ID), new StreamMarks(longOf(stream.get(MARKS_FLOOR)), byType, pending, legacyFailed, pendingSince));
        }
        return streamMarks;
    }

    private @Nullable Object encodeState(String sagaId, S state) {
        if (flowEvents != null && state instanceof FlowStateImpl<?> flowState) {
            return encodeFlowState(sagaId, flowState, flowEvents);
        }
        if (flowEvents != null && state instanceof FlowState<?>) {
            // The flow executor's state is read back field by field, so another FlowState implementation would be
            // written one way and read the other.
            throw new IllegalArgumentException("a flow saga store can only persist the flow executor's FlowState (FlowStateImpl), got "
                                               + state.getClass().getName());
        }
        return stateCodec.encode(state);
    }

    @SuppressWarnings("unchecked")
    private S decodeState(Object encoded) {
        if (flowEvents != null && encoded instanceof Map<?, ?> flowState) {
            return (S) decodeFlowState((Map<String, Object>) flowState, flowEvents);
        }
        return stateCodec.decode(encoded);
    }

    private Map<String, Object> encodeFlowState(String sagaId, FlowStateImpl<?> flowState, CloudEventConverter<Object> converter) {
        warnIfRetainedSizeCrossesThreshold(sagaId, flowState.received().size());
        Map<String, Object> state = new LinkedHashMap<>();
        if (flowState.currentStep() != null) {
            state.put(FLOW_CURRENT_STEP, flowState.currentStep());
        }
        state.put(FLOW_WINDOW_START, flowState.windowStart());
        state.put(FLOW_STEP_ENTRY_INDEX, flowState.stepEntryIndex());
        state.put(FLOW_COMPLETED, flowState.completed());
        if (flowState.previousStep() != null) {
            state.put(FLOW_PREVIOUS_STEP, flowState.previousStep());
        }
        state.put(FLOW_PREVIOUS_STEP_ENTRY_INDEX, flowState.previousStepEntryIndex());
        state.put(FLOW_LAST_ACTION, flowState.lastAction().name());
        state.put(FLOW_MATCHED_BRANCH_INDEX, flowState.matchedBranchIndex());
        StepConditionProgress progress = flowState.stepConditionProgress();
        if (progress != null) {
            Map<String, Object> progressFields = new LinkedHashMap<>();
            progressFields.put(FLOW_LEAF_FINGERPRINT, progress.leafFingerprint());
            progressFields.put(FLOW_MATCH_COUNTS, new ArrayList<>(progress.matchCounts()));
            state.put(FLOW_STEP_CONDITION_PROGRESS, progressFields);
        }
        List<Object> received = new ArrayList<>();
        for (Object event : flowState.received()) {
            received.add(converter.toCloudEvent(event));
        }
        state.put(FLOW_RECEIVED, received);
        return state;
    }

    private static FlowStateImpl<Object> decodeFlowState(Map<String, Object> state, CloudEventConverter<Object> converter) {
        List<Object> received = new ArrayList<>();
        for (Object cloudEvent : list(state.get(FLOW_RECEIVED))) {
            received.add(converter.toDomainEvent((CloudEvent) cloudEvent));
        }
        return new FlowStateImpl<>(
                (String) state.get(FLOW_CURRENT_STEP),
                received,
                // Data written before this field existed never dropped anything, so its tail started right after the
                // pinned initiating event, position 1. Only a FlowStateImpl.initial() that never started has 0.
                intOf(state.get(FLOW_WINDOW_START), 1),
                intOf(state.get(FLOW_STEP_ENTRY_INDEX), 0),
                state.get(FLOW_COMPLETED) != null && (Boolean) state.get(FLOW_COMPLETED),
                (String) state.get(FLOW_PREVIOUS_STEP),
                // -1 is the record's own "not known", so data written before this field existed makes a window-condition
                // reaction fall back to the whole retained history, which is what it saw then.
                intOf(state.get(FLOW_PREVIOUS_STEP_ENTRY_INDEX), -1),
                ActionKind.valueOf((String) state.get(FLOW_LAST_ACTION)),
                intOf(state.get(FLOW_MATCHED_BRANCH_INDEX), -1),
                decodeStepConditionProgress(state));
    }

    // Absent counts read back as null, so the flow lowering counts the step's window instead. A progress without its
    // fingerprint reads as absent, since the counts cannot be matched to a declaration without it.
    private static @Nullable StepConditionProgress decodeStepConditionProgress(Map<String, Object> state) {
        Object progressValue = state.get(FLOW_STEP_CONDITION_PROGRESS);
        if (progressValue == null) {
            return null;
        }
        Map<String, Object> progress = map(progressValue);
        String fingerprint = (String) progress.get(FLOW_LEAF_FINGERPRINT);
        if (fingerprint == null) {
            return null;
        }
        List<Integer> counts = new ArrayList<>();
        for (Object count : list(progress.get(FLOW_MATCH_COUNTS))) {
            counts.add(((Number) count).intValue());
        }
        return new StepConditionProgress(fingerprint, counts);
    }

    // Warns once when an instance's retained event count crosses the threshold from below, and again only after a save
    // has taken it below the threshold and it crosses anew. Package private so a test can drive it with plain counts.
    void warnIfRetainedSizeCrossesThreshold(String sagaId, int retainedEventCount) {
        boolean shouldWarn;
        synchronized (retainedEventWarningLatch) {
            if (retainedEventCount < RETAINED_EVENT_WARNING_THRESHOLD) {
                retainedEventWarningLatch.remove(sagaId);
                return;
            }
            // get() and not containsKey(), because get() is what marks the entry as recently used.
            if (retainedEventWarningLatch.get(sagaId) != null) {
                shouldWarn = false;
            } else {
                retainedEventWarningLatch.put(sagaId, Boolean.TRUE);
                shouldWarn = true;
            }
        }
        if (shouldWarn) {
            log.warn("Flow saga instance '{}' has retained {} received events, at or above the warning threshold of {}. " +
                     "Consider capping the flow's step with stepWindow(...) to trim what it retains, or the document " +
                     "risks growing toward MongoDB's 16 MB document limit.",
                    sagaId, retainedEventCount, RETAINED_EVENT_WARNING_THRESHOLD);
        }
    }

    int retainedEventWarningLatchSize() {
        synchronized (retainedEventWarningLatch) {
            return retainedEventWarningLatch.size();
        }
    }

    private static void putInstant(Map<String, Object> fields, String field, @Nullable Instant instant) {
        if (instant != null) {
            fields.put(field, instant.toEpochMilli());
        }
    }

    private static @Nullable Instant instant(@Nullable Object epochMilli) {
        return epochMilli == null ? null : Instant.ofEpochMilli(((Number) epochMilli).longValue());
    }

    private static long longOf(@Nullable Object number) {
        return ((Number) requireNonNull(number, "a stored number is missing")).longValue();
    }

    private static @Nullable Long nullableLong(@Nullable Object number) {
        return number instanceof Number n ? n.longValue() : null;
    }

    private static int intOf(@Nullable Object number, int defaultValue) {
        return number instanceof Number n ? n.intValue() : defaultValue;
    }

    private static List<Long> longs(@Nullable Object values) {
        List<Long> longs = new ArrayList<>();
        for (Object value : list(values)) {
            longs.add(longOf(value));
        }
        return longs;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(@Nullable Object value) {
        return value == null ? List.of() : (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(@Nullable Object value) {
        return value == null ? Collections.emptyMap() : (Map<String, Object>) value;
    }
}
