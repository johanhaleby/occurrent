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

package org.occurrent.dsl.saga.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The pure, stack-agnostic decision step of the saga executor: given the current envelope (or {@code null} for a new
 * instance), one input, and its delivery metadata, it decides whether to skip (redelivery, terminal, or an event that
 * cannot start an instance) and otherwise computes the next envelope and the commands to dispatch. It performs no I/O, so
 * a blocking or reactor runner can wrap it identically. The runner owns loading, the compare-and-set save, dispatch, and
 * timer polling.
 * <p>
 * Redelivery is deduplicated from the event's stream version, per stream id. An event without a stream id and a stream
 * version, which Occurrent's own stored events always carry, cannot be deduplicated and is re-folded on redelivery, so a
 * custom event source feeding a saga must carry both extensions for the fold to be redelivery-safe. A global position is
 * never used for this, because events can reach a saga in a different order than their positions.
 */
public final class SagaExecutionSupport {

    private static final Logger log = LoggerFactory.getLogger(SagaExecutionSupport.class);

    private SagaExecutionSupport() {
    }

    /**
     * Whether a failure is this instance's own, rather than a condition of the JVM that this instance happened to be
     * running in when it struck. Both input paths ask this before charging a failure to an instance, so that the answer
     * is one rule rather than two implementations of one rule.
     * <p>
     * Nearly everything is the instance's own, whatever its type. A {@code StackOverflowError} out of a recursive
     * {@code evolve} is the instance's code failing, and so is the {@code NoClassDefFoundError} every later attempt
     * raises once an {@code ExceptionInInitializerError} has left a class the reaction needs unusable. Neither is any
     * less
     * the instance's fault for being an {@code Error}, and neither stops being it on the next delivery.
     * <p>
     * {@link OutOfMemoryError} is the exclusion, and it is the only one. It says the JVM ran out of heap while this
     * instance held the thread, which is a statement about the process rather than about the instance's work, and any
     * other instance running at that moment would have met the same thing. Charging it to whichever instance was
     * unlucky would stop that instance until somebody releases it, over a heap exhaustion it had nothing to do with.
     * <p>
     * The test is on the failure itself and not on its causes. A reaction that catches an {@code OutOfMemoryError} and
     * wraps it has decided the failure is its own, and this takes it at its word.
     * <p>
     * {@code VirtualMachineError} is deliberately not the line. {@code StackOverflowError} is one by hierarchy, and it
     * is both the instance's own fault and among the likelier ways a saga fails, so excluding the supertype would
     * exclude the case this most needs to catch.
     */
    public static boolean isAttributableToTheInstance(Throwable failure) {
        return !(failure instanceof OutOfMemoryError);
    }

    /**
     * How many versions behind the highest one applied of its type a stream event may arrive and still be applied. A feed
     * that reorders a stream's events of one type, such as a broker consumer with more than one message in flight, can
     * deliver version 7 before version 6. 256 covers a prefetch far beyond what such a feed is configured with.
     */
    public static final int DEFAULT_REORDER_WINDOW = 256;

    /** What the executor makes of a stream event, given the versions the instance has already applied. */
    public enum Delivery {
        /** The instance has not applied it, so it is applied. */
        NEW,
        /** The instance has applied it, so it is skipped. */
        REDELIVERY,
        /** Further behind the highest applied version of its type than the window covers, so it is skipped. */
        BEHIND_WINDOW
    }

    /**
     * Delivery metadata used to deduplicate a redelivered event. All fields are {@code null} for a timer input.
     * {@code eventType} is the cloud event type, and it only has to be there when the feed can reorder a stream's events
     * across types. A stream event without one is treated as being of a single type named by the empty string.
     * {@code eventId} and {@code eventSource} are the CloudEvent id and source, which key an input a quarantined instance
     * skips when it has no stream id and stream version, see {@link #skipKey()}.
     */
    public record EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position, @Nullable String eventType,
                            @Nullable String eventId, @Nullable String eventSource) {
        public static final EventMeta NONE = new EventMeta(null, null, null, null);

        public EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position, @Nullable String eventType) {
            this(streamId, streamVersion, position, eventType, null, null);
        }

        public EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position) {
            this(streamId, streamVersion, position, null);
        }

        /**
         * The key a quarantined instance records this input under when it skips it, see
         * {@link SkippedInput#key()}. That is {@link #redeliveryKey()} when there is one, and otherwise a
         * key built from the CloudEvent id and source. {@code null} for a timer input and for an event that has neither.
         */
        public @Nullable String skipKey() {
            String redeliveryKey = redeliveryKey();
            if (redeliveryKey != null) {
                return redeliveryKey;
            }
            return eventId == null || eventSource == null ? null : SkippedInputKey.cloudEventKey(eventId, eventSource);
        }

        /**
         * The string that identifies this input across redeliveries, which is the stream id with its stream version,
         * or {@code null} when the event lacks either. {@code isRedelivery} looks at the same two things, so the rule
         * lives here instead of being written twice.
         * <p>
         * The global position is not part of the key. A subscription model assigns it, and an event can commit at a
         * lower position than one the saga has already folded. An event store that assigns no position at all, one
         * built with {@code withoutStreamPosition()} for instance, still gives every event a stream id and version.
         */
        public @Nullable String redeliveryKey() {
            if (streamId != null && streamVersion != null) {
                return streamId + "@" + streamVersion;
            }
            return null;
        }

        /** Whether {@link #redeliveryKey()} exists, meaning a redelivery of this input can be told from a new one. */
        public boolean carriesRedeliveryKey() {
            return redeliveryKey() != null;
        }
    }

    /**
     * The result of processing one input. When {@link #processed()} is false the input was not applied (it was a
     * redelivery, the instance is terminal or quarantined, or the event cannot start an instance). The executor then
     * dispatches nothing. When there is a {@link #skipToRecord()}, which is a quarantined instance recording the input
     * it skips, it records that as {@link SagaExecutionSupport#skipToRecord} says. Otherwise it dispatches {@link #commands()} in order and then saves {@link #envelope()} with
     * {@code compareAndSave(..., expectedVersion())}.
     */
    public record Outcome<S extends @Nullable Object, C>(boolean processed,
                                                         @Nullable SagaEnvelope<S> envelope,
                                                         List<C> commands,
                                                         long expectedVersion,
                                                         @Nullable SkippedInput skipToRecord) {
        static <S extends @Nullable Object, C> Outcome<S, C> skip() {
            return new Outcome<>(false, null, List.of(), 0, null);
        }

        static <S extends @Nullable Object, C> Outcome<S, C> skipToRecord(SkippedInput toRecord) {
            return new Outcome<>(false, null, List.of(), 0, toRecord);
        }

        static <S extends @Nullable Object, C> Outcome<S, C> processed(SagaEnvelope<S> envelope, List<C> commands, long expectedVersion) {
            return new Outcome<>(true, envelope, commands, expectedVersion, null);
        }
    }

    /**
     * Process one input against {@code current} (or {@code null} for a not-yet-existing instance).
     *
     * @param saga    the saga descriptor
     * @param sagaId  the correlation id of the instance
     * @param current the stored envelope, or {@code null} if none exists yet
     * @param input   the event or timeout to apply
     * @param meta    the event's delivery metadata (use {@link EventMeta#NONE} for a timeout)
     * @param now     the current instant, used to resolve relative timers and stamp timestamps
     */
    public static <E, S extends @Nullable Object, C> Outcome<S, C> process(Saga<E, S, C> saga,
                                                                           String sagaId,
                                                                           @Nullable SagaEnvelope<S> current,
                                                                           SagaInput<E> input,
                                                                           EventMeta meta,
                                                                           Instant now) {
        return process(saga, sagaId, current, input, meta, now, DEFAULT_REORDER_WINDOW);
    }

    /**
     * As {@link #process(Saga, String, SagaEnvelope, SagaInput, EventMeta, Instant)}, with the reorder window the runner
     * is configured with.
     *
     * @param reorderWindow how many versions behind the highest one applied of its type a stream event may arrive and
     *                      still be applied, at least 1
     */
    public static <E, S extends @Nullable Object, C> Outcome<S, C> process(Saga<E, S, C> saga,
                                                                           String sagaId,
                                                                           @Nullable SagaEnvelope<S> current,
                                                                           SagaInput<E> input,
                                                                           EventMeta meta,
                                                                           Instant now,
                                                                           int reorderWindow) {
        // Whether a start transition was saved, which is neither "onStart was called" nor "a document exists". An
        // instance whose very first event failed has an envelope holding nothing but that failure. Keying on the
        // document would leave such an instance permanently "already started", so a later redelivery of its start event
        // would skip onStart with nothing anywhere saying so.
        boolean hasStarted = current != null && current.started();
        @Nullable E startEvent = startEventOrNull(saga, hasStarted, input);
        if (!hasStarted && startEvent == null) {
            // A timeout never starts an instance, and a non-start event for an instance that has not started is skipped.
            // An instance quarantined on its start event lands here first, and records the input like the skip below.
            return current != null && current.isQuarantined() ? recordingSkip(current, meta, reorderWindow) : Outcome.skip();
        }
        if (current != null && current.isCompleted()) {
            return Outcome.skip();
        }
        if (current != null && current.isQuarantined()) {
            // A quarantined instance is inert. It skips every input addressed to it and its watermarks stay where they are.
            // Advancing one would record an input as handled that the instance never applied, and nothing afterwards can
            // tell that the state has a gap in it. It records the input's key instead, which a release replays.
            return recordingSkip(current, meta, reorderWindow);
        }
        if (current != null && classify(current, meta, reorderWindow) != Delivery.NEW) {
            return Outcome.skip();
        }
        return apply(saga, sagaId, current, input, meta, now, OptionalInt.of(reorderWindow), hasStarted, startEvent);
    }

    // The input applied to the instance, past every reason to skip it.
    private static <E, S extends @Nullable Object, C> Outcome<S, C> apply(Saga<E, S, C> saga, String sagaId, @Nullable SagaEnvelope<S> current,
                                                                          SagaInput<E> input, EventMeta meta, Instant now, OptionalInt reorderWindow,
                                                                          boolean hasStarted, @Nullable E startEvent) {

        boolean starting = !hasStarted;
        S previousState = hasStarted ? current.state() : saga.initialState();
        S nextState = saga.evolve(previousState, input);

        List<SagaEffect<C>> effects = new ArrayList<>();
        if (starting) {
            // A start always arrives as an Event, so its delivery metadata rides on the input. The fall back to
            // metadata-less is only defensive.
            EventMetadata startMetadata = input instanceof SagaInput.Event<E> ev ? ev.metadata() : SagaInput.NO_METADATA;
            effects.addAll(saga.onStart(nextState, startMetadata, startEvent));
        }
        effects.addAll(saga.react(nextState, input));

        // A timer that fires into nothing, neither folding the state nor producing an effect, is almost always a name
        // typo: a StartTimeout armed under one name and its evolveOnTimeout/reactOnTimeout registered under another. The
        // timer is still consumed (below), so the saga stalls silently. Warn rather than change behaviour, so the mistake
        // surfaces in the log instead of as a process that mysteriously never advances.
        if (input instanceof SagaInput.Timeout<E> firedTimer && effects.isEmpty() && Objects.equals(previousState, nextState)) {
            log.warn("Saga '{}' fired timer '{}' but no handler folded its state or produced an effect; the timer is consumed and the instance does not advance. Check that the timer name matches its evolveOnTimeout/reactOnTimeout registration.",
                    sagaId, firedTimer.timeout().timerName());
        }

        List<C> commands = new ArrayList<>();
        Map<String, TimerEntry> timers = new LinkedHashMap<>();
        if (current != null) {
            for (TimerEntry timer : current.timers()) {
                timers.put(timer.name(), timer);
            }
        }
        // A timer is one-shot: firing it consumes it, so a timeout that neither cancels its timer nor completes the
        // instance does not re-fire every poll. Recurrence is explicit, via a StartTimeout effect below (which re-adds it).
        if (input instanceof SagaInput.Timeout<E> firedTimer) {
            timers.remove(firedTimer.timeout().timerName().encode());
        }
        applyEffects(effects, commands, timers, now);

        boolean terminal = saga.isTerminal(nextState);
        if (terminal) {
            timers.clear();
        }

        boolean isNew = current == null;
        long expectedVersion = isNew ? 0 : current.version();
        Instant createdAt = isNew ? now : current.createdAt();
        Map<String, Long> streamWatermarks = isNew ? new LinkedHashMap<>() : new LinkedHashMap<>(current.streamWatermarks());
        Map<String, StreamMarks> streamMarks = isNew ? new LinkedHashMap<>() : new LinkedHashMap<>(current.streamMarks());
        if (meta.streamId() != null && meta.streamVersion() != null) {
            streamMarks.put(meta.streamId(), recorded(streamMarks.get(meta.streamId()), streamWatermarks.get(meta.streamId()), meta, reorderWindow));
            streamWatermarks.merge(meta.streamId(), meta.streamVersion(), Math::max);
        }

        SagaEnvelope<S> next = new SagaEnvelope<>(
                sagaId,
                nextState,
                terminal ? SagaStatus.COMPLETED : SagaStatus.ACTIVE,
                expectedVersion + 1,
                List.copyOf(timers.values()),
                streamWatermarks,
                null,
                createdAt,
                now,
                terminal ? now : null,
                // Derived from nextState by the envelope's constructor; nothing sensible to pass here.
                null,
                true,
                failureAfter(current, meta, terminal),
                streamMarks,
                null);
        return Outcome.processed(next, commands, expectedVersion);
    }

    private static <S extends @Nullable Object, C> Outcome<S, C> recordingSkip(SagaEnvelope<S> current, EventMeta meta, int reorderWindow) {
        SkippedInput toRecord = skipToRecord(current, meta, reorderWindow);
        return toRecord == null ? Outcome.skip() : Outcome.skipToRecord(toRecord);
    }

    /**
     * The record the quarantined instance {@code current} writes for the input {@code meta} describes, which it skips,
     * or {@code null} when it records nothing. Its {@code seq} is {@link SagaEnvelope#quarantineSkipCount()}.
     * <p>
     * The executor saves {@link #withSkipCounted(SagaEnvelope)} and this record under
     * {@link SagaEnvelope#quarantineId()} in one write, with
     * {@code SagaStateStoreSkippedInputs.compareAndSaveSkipped(..., current.version(), ...)}. Raising the count takes the
     * {@code seq} for this record and no other. A record of the key that is there already stays, and the new {@code seq}
     * goes unused. A lost write reads the instance again and decides again, so an instance that turned active in between
     * applies the input instead.
     * <p>
     * Nothing is recorded when the instance is not quarantined, was quarantined by 0.34.0 and has no quarantine id or
     * no count, when the input is a timer or an event with no key, when the instance counts the input as applied
     * already, or when the key is the failure record's input, which a release replays first anyway.
     */
    public static <S extends @Nullable Object> @Nullable SkippedInput skipToRecord(SagaEnvelope<S> current, EventMeta meta, int reorderWindow) {
        String key = meta.skipKey();
        Long count = current.quarantineSkipCount();
        SagaFailure failure = current.failure();
        if (current.quarantineId() == null || count == null || key == null || failure == null || failure.input().equals(key)) {
            return null;
        }
        // An input the instance applied before it was quarantined is skipped as a redelivery anyway, so there is nothing
        // to replay
        if (classify(current, meta, reorderWindow) != Delivery.NEW) {
            return null;
        }
        return new SkippedInput(count, key);
    }

    /**
     * The quarantined instance {@code current} one version up with its {@link SagaEnvelope#quarantineSkipCount()}
     * raised by one, and nothing else changed, so the watermarks and the stream marks stay where they are.
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> withSkipCounted(SagaEnvelope<S> current) {
        long count = Objects.requireNonNull(current.quarantineSkipCount(), "quarantineSkipCount");
        return withBookkeeping(current, current.status(), current.updatedAt(), current.failure(), current.streamMarks(), count + 1);
    }

    /**
     * The releasing instance {@code current} one version up and nothing else changed, which a release saves when it drops
     * a record that {@link #replay} would not apply, so the record goes in a write that expects the version the release
     * read.
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> oneVersionUp(SagaEnvelope<S> current) {
        return withBookkeeping(current, current.status(), current.updatedAt(), current.failure(), current.streamMarks(), current.quarantineSkipCount());
    }

    /** The {@link SkippedInput#seq()} a release records the input the instance was quarantined on under, before every skip. */
    public static final long RELEASE_HEAD_SEQ = -1;

    /**
     * The record a release of {@code current} writes for the input the instance was quarantined on, at
     * {@link #RELEASE_HEAD_SEQ}, so it is replayed first. The executor records it at the head in the write of
     * {@link #startRelease}, with {@code SagaStateStoreSkippedInputs.compareAndSaveReleaseStarted}. The record an earlier
     * release kept when it was quarantined again on this input would otherwise keep its place.
     */
    public static <S extends @Nullable Object> SkippedInput releaseHead(SagaEnvelope<S> current) {
        return new SkippedInput(RELEASE_HEAD_SEQ, Objects.requireNonNull(current.failure(), "failure").input());
    }

    /**
     * The quarantined instance {@code current} with a release started, or {@code null} when a release of it is running
     * already, which the caller then goes on with. Save it together with the record of {@link #releaseHead}, with
     * {@code SagaStateStoreSkippedInputs.compareAndSaveReleaseStarted(..., current.version(), ...)}.
     * <p>
     * The write sets {@link SagaFailure#releasedAt()} to {@code now}. It restarts the
     * quarantine budget of the input the instance was quarantined on at {@code now}, on the failure record and on that
     * input's pending entry, so a release that fails on it again gets a full budget.
     *
     * @throws IllegalStateException when the instance is not quarantined, or was quarantined by 0.34.0 and has no
     *                               quarantine id
     */
    public static <S extends @Nullable Object> @Nullable SagaEnvelope<S> startRelease(SagaEnvelope<S> current, Instant now) {
        if (!current.isQuarantined()) {
            throw new IllegalStateException("Saga instance '" + current.sagaId() + "' is " + current.status() + ", not QUARANTINED, so there is nothing to release.");
        }
        SagaFailure failure = current.failure();
        String quarantineId = current.quarantineId();
        if (failure == null || quarantineId == null) {
            throw cannotRelease(current, "it was quarantined by 0.34.0, which recorded none of the inputs it skipped");
        }
        if (failure.isReleasing()) {
            return null;
        }
        SagaFailure releasing = new SagaFailure(failure.input(), failure.position(), now, failure.failureType(), failure.failureMessage(), now, quarantineId);
        Map<String, StreamMarks> streamMarks = new LinkedHashMap<>(current.streamMarks());
        if (SkippedInputKey.parse(failure.input()) instanceof SkippedInputKey.StreamKey(String streamId, long streamVersion)) {
            StreamMarks marks = streamMarks.get(streamId);
            if (marks != null && marks.pendingSince().containsKey(streamVersion)) {
                Map<Long, Instant> pendingSince = new LinkedHashMap<>(marks.pendingSince());
                pendingSince.put(streamVersion, now);
                streamMarks.put(streamId, new StreamMarks(marks.floor(), marks.byType(), marks.pending(), marks.legacyFailed(), pendingSince));
            }
        }
        return withBookkeeping(current, SagaStatus.QUARANTINED, now, releasing, streamMarks, current.quarantineSkipCount());
    }

    /**
     * Why an instance cannot be released, with the way out that is left.
     */
    public static <S extends @Nullable Object> IllegalStateException cannotRelease(SagaEnvelope<S> current, String because) {
        return new IllegalStateException("Saga instance '" + current.sagaId() + "' cannot be released, because " + because + ". "
                                         + "Delete the instance with SagaStateStore.delete(\"" + current.sagaId() + "\"), which throws its state away.");
    }

    /**
     * Apply {@code record}, the first record of a releasing instance, with the quarantined skip bypassed for that one
     * input. {@code input} and {@code meta} are the event read back from the event store by the record's key, and
     * {@code meta} carries that event's own stream id and stream version, whatever kind of key the record has. So the
     * input is deduplicated by its stream marks like any other, and a record of an input the instance applied already
     * is recognised.
     * <p>
     * It applies the input through the same steps as {@link #process}. The instance stays {@link SagaStatus#QUARANTINED}
     * and keeps its failure record, with {@link SagaFailure#releasedAt()}. The executor deletes the record in the write
     * that applies it. An input that completes the instance ends the release with it.
     * <p>
     * The release reads and writes the marks with no reorder window, so every version it applies stays in them until the
     * instance is released, see {@link #withReleased}. An input that completes the instance trims them to
     * {@code reorderWindow} in the same write, since a completed instance replays nothing more. An instance quarantined
     * again during the release keeps them all, because the records after the failing one stay, and the next release
     * reads those records against the same marks. With the configured reorder window, an earlier record that raised the mark of its type
     * would put a later record of that type behind the window, and the release would delete it without applying it. A
     * record is written only while its input is new, so without a window a record that is not new names an input the
     * instance applied, through another record of the same input or through a replay whose delete a crash prevented.
     * <p>
     * Such a record, and a record that is not a start event of an instance that never started, which the instance would
     * skip outside a release too, comes back as an outcome that is not {@link Outcome#processed()} and has no envelope,
     * and the executor deletes the record in a write of {@link #oneVersionUp}.
     *
     * @throws IllegalStateException when the instance is not releasing, or {@code meta} has no stream id and stream
     *                               version
     */
    public static <E, S extends @Nullable Object, C> Outcome<S, C> replay(Saga<E, S, C> saga, String sagaId, SagaEnvelope<S> current,
                                                                          SkippedInput record, SagaInput<E> input, EventMeta meta, Instant now,
                                                                          int reorderWindow) {
        if (!current.isReleasing()) {
            throw new IllegalStateException("Saga instance '" + sagaId + "' is not releasing, so it cannot replay '" + record.key() + "'.");
        }
        if (meta.redeliveryKey() == null) {
            throw new IllegalStateException("Saga instance '" + sagaId + "' cannot replay '" + record.key() + "', because the event read back has no stream id and stream version.");
        }
        boolean hasStarted = current.started();
        @Nullable E startEvent = startEventOrNull(saga, hasStarted, input);
        if (!hasStarted && startEvent == null || classify(current, meta, NO_REORDER_WINDOW) != Delivery.NEW) {
            return Outcome.skip();
        }
        Outcome<S, C> applied = apply(saga, sagaId, current, input, meta, now, NO_REORDER_WINDOW, hasStarted, startEvent);
        SagaEnvelope<S> next = Objects.requireNonNull(applied.envelope());
        if (next.isCompleted()) {
            SagaEnvelope<S> completed = new SagaEnvelope<>(next.sagaId(), next.state(), next.status(), next.version(), next.timers(),
                    next.streamWatermarks(), next.positionWatermark(), next.createdAt(), next.updatedAt(), next.completedAt(), next.currentStep(),
                    next.started(), next.failure(), withinWindow(next.streamMarks(), reorderWindow), next.quarantineSkipCount());
            return Outcome.processed(completed, applied.commands(), applied.expectedVersion());
        }
        SagaEnvelope<S> releasing = new SagaEnvelope<>(next.sagaId(), next.state(), SagaStatus.QUARANTINED, next.version(), next.timers(),
                next.streamWatermarks(), null, next.createdAt(), next.updatedAt(), next.completedAt(), next.currentStep(), next.started(),
                current.failure(), next.streamMarks(), current.quarantineSkipCount());
        return Outcome.processed(releasing, applied.commands(), applied.expectedVersion());
    }

    /**
     * The releasing instance {@code current} released, {@link SagaStatus#ACTIVE} with no failure record, no quarantine
     * id and no skip count. Save it with
     * {@code SagaStateStoreSkippedInputs.compareAndSaveQuarantineEnded(..., current.version(), ...)}, and only when
     * {@code current} was read before a read of its records that found none. A skip records its input in a write that
     * raises the version, so a skip recorded after {@code current} was read makes this write lose, and the release reads
     * the records again.
     * <p>
     * The released instance keeps no more applied versions per stream and event type than {@code reorderWindow}, as
     * after a live write, since the release kept every version it applied.
     *
     * @throws IllegalStateException when the instance is not releasing
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> withReleased(SagaEnvelope<S> current, Instant now, int reorderWindow) {
        if (!current.isReleasing()) {
            throw new IllegalStateException("Saga instance '" + current.sagaId() + "' is not releasing.");
        }
        return withBookkeeping(current, SagaStatus.ACTIVE, now, null, withinWindow(current.streamMarks(), reorderWindow), null);
    }

    // The marks with no more applied versions than reorderWindow keeps, the way a live write trims them. A release
    // keeps every version it applies, see replay(..)
    private static Map<String, StreamMarks> withinWindow(Map<String, StreamMarks> streamMarks, int reorderWindow) {
        Map<String, StreamMarks> trimmed = new LinkedHashMap<>();
        streamMarks.forEach((streamId, marks) -> {
            Map<String, TypeMark> byType = new LinkedHashMap<>();
            marks.byType().forEach((type, typeMark) -> {
                long horizon = horizon(typeMark.horizon(), typeMark.mark(), OptionalInt.of(reorderWindow));
                byType.put(type, new TypeMark(typeMark.mark(), typeMark.applied().stream().filter(version -> version > horizon).toList(), horizon));
            });
            trimmed.put(streamId, new StreamMarks(marks.floor(), byType, marks.pending(), marks.legacyFailed(), marks.pendingSince()));
        });
        return trimmed;
    }

    /**
     * Whether {@code after}, read when a write computed from {@code before} lost its compare-and-set, differs from
     * {@code before} only by inputs the instance skipped and recorded in between. That is a higher version with every
     * other compared field equal. {@link SagaEnvelope#quarantineSkipCount()} isn't compared, since a skip raises it,
     * and neither are the saga id, the current step and the deprecated position watermark. A release then writes
     * what it computed again on top of {@code after}, see {@link #rebased}, rather than applying the input a second time.
     * The state is compared with {@code equals}, so a state type without one never matches, and the input is applied
     * again and its reaction dispatched again, as any lost write is.
     */
    public static <S extends @Nullable Object> boolean differsOnlyByRecordedSkips(SagaEnvelope<S> before, SagaEnvelope<S> after) {
        return after.version() > before.version()
               && Objects.equals(before.state(), after.state())
               && before.status() == after.status()
               && before.timers().equals(after.timers())
               && before.streamWatermarks().equals(after.streamWatermarks())
               && before.streamMarks().equals(after.streamMarks())
               && Objects.equals(before.createdAt(), after.createdAt())
               && Objects.equals(before.updatedAt(), after.updatedAt())
               && Objects.equals(before.completedAt(), after.completedAt())
               && before.started() == after.started()
               && Objects.equals(before.failure(), after.failure());
    }

    /**
     * {@code computed}, which a release computed from an instance that has since only recorded skipped inputs, moved on
     * top of {@code after}. One version above it, and with its skip count while the instance stays quarantined. Save it
     * the way {@code computed} was saved, expecting {@code after.version()}.
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> rebased(SagaEnvelope<S> computed, SagaEnvelope<S> after) {
        return new SagaEnvelope<>(computed.sagaId(), computed.state(), computed.status(), after.version() + 1, computed.timers(),
                computed.streamWatermarks(), null, computed.createdAt(), computed.updatedAt(), computed.completedAt(), computed.currentStep(),
                computed.started(), computed.failure(), computed.streamMarks(),
                computed.isQuarantined() ? after.quarantineSkipCount() : computed.quarantineSkipCount());
    }

    /**
     * What to write when the record {@code key} a release was applying failed, or {@code null} when nothing needs writing
     * and the release simply stops for now. Save it with {@code compareAndSaveWithoutState(..., expectedVersion())}. The
     * caller checks that {@code key} is still the first record of the instance.
     * <p>
     * The failure is recorded the way {@link #onFailure} records one, measured from when the input first failed, except
     * that the instance stays {@link SagaStatus#QUARANTINED} and releasing. When the input's budget has run out, the
     * instance is quarantined on it again, with the same quarantine id. The input becomes the failure record's input,
     * {@link SagaFailure#releasedAt()} is cleared, and the record stays, so a later release replays the input first and
     * then the records after it. When the record's key is a CloudEvent key, the failure record names the input by its
     * stream key instead, and the later release replays the record after that input and only deletes it, since it is
     * applied by then.
     *
     * @throws IllegalStateException when {@code meta} has no stream id and stream version, which a release never
     *                               replays
     */
    public static <S extends @Nullable Object> @Nullable FailureRecord<S> onReleaseFailure(SagaEnvelope<S> current, String key, EventMeta meta,
                                                                                           Throwable failure, Instant now,
                                                                                           Duration quarantineAfter, int reorderWindow) {
        String input = meta.redeliveryKey();
        if (input == null) {
            throw new IllegalStateException("Saga instance '" + current.sagaId() + "' failed on '" + key + "' during a release, but the event has no stream id and stream version, which a release never replays.");
        }
        SagaFailure existing = current.failure();
        if (!current.isReleasing() || existing == null) {
            return null;
        }
        // With no window, as replay(..) reads the marks, since with one an input an earlier record put behind it would
        // read as applied and its failure would never be recorded
        if (classify(current, meta, NO_REORDER_WINDOW) != Delivery.NEW) {
            // Another writer applied it in between, so there is no failure left to record
            return null;
        }
        Instant failingSince = failingSince(current, meta, now);
        boolean budgetUsedUp = Duration.between(failingSince, now).compareTo(quarantineAfter) >= 0;
        if (!budgetUsedUp && existing.input().equals(input) && isPending(current.streamMarks().get(meta.streamId()), meta)) {
            return null;
        }
        // The record named an input the release has applied by now, since a release applies its records in order. So
        // unlike onFailure nothing of the replaced record is kept on the stream marks, which would make an applied input
        // outstanding again
        Map<String, StreamMarks> streamMarks = withPending(current.streamMarks(), current.streamWatermarks(), meta, reorderWindow, failingSince);
        if (budgetUsedUp) {
            SagaFailure record = new SagaFailure(input, meta.position(), failingSince, failure.getClass().getName(), shortened(failure.getMessage()),
                    null, existing.quarantineId());
            return new FailureRecord<>(requarantined(current, now, record, streamMarks), current.version(), true);
        }
        SagaFailure record = new SagaFailure(input, meta.position(), failingSince, failure.getClass().getName(), shortened(failure.getMessage()),
                existing.releasedAt(), existing.quarantineId());
        return new FailureRecord<>(withBookkeeping(current, SagaStatus.QUARANTINED, now, record, streamMarks, current.quarantineSkipCount()),
                current.version(), false);
    }

    /**
     * The releasing instance {@code current} quarantined again on the record {@code key}, because the event store no
     * longer has that event. Save it with {@code compareAndSaveWithoutState(..., current.version())}. The record
     * stays, and a later release moves it to the head.
     * <p>
     * No budget applies. The missing input becomes the failure record's input, by the key it was recorded under, which
     * is a CloudEvent key for an input that arrived without a stream id and version. The quarantine id stays,
     * {@link SagaFailure#releasedAt()} is cleared, and the records after it stay. Nothing is dropped, so a later release,
     * once the event is back, replays it first. The record's {@code failureType} is {@link #MISSING_EVENT_FAILURE_TYPE}
     * and its message names the key. When the missing input is the one the instance was quarantined on, the record keeps
     * that input's position. A recorded input's record has no position, since the position is read from the event.
     *
     * @throws IllegalStateException when the instance is not releasing
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> withMissingInputQuarantined(SagaEnvelope<S> current, String key, Instant now) {
        return withUnreplayableInputQuarantined(current, key, now, MISSING_EVENT_FAILURE_TYPE, "The event '" + key + MISSING_INPUT_MESSAGE_END);
    }

    /**
     * The releasing instance {@code current} quarantined again on the record {@code key}, because the event the store
     * gave back for it has no stream id and stream version, which only an {@code EventStoreQueries} other than
     * Occurrent's own event stores can give. A release replays an input by its stream id and stream version only, since
     * that is what tells an input it applied already from one it has not. Saved as {@link #withMissingInputQuarantined} is,
     * with the record kept and no budget, and the failure record names the key with
     * {@link #EVENT_WITHOUT_STREAM_KEY_FAILURE_TYPE} as its {@code failureType}.
     *
     * @throws IllegalStateException when the instance is not releasing
     */
    public static <S extends @Nullable Object> SagaEnvelope<S> withInputWithoutStreamKeyQuarantined(SagaEnvelope<S> current, String key, Instant now) {
        return withUnreplayableInputQuarantined(current, key, now, EVENT_WITHOUT_STREAM_KEY_FAILURE_TYPE,
                "The event '" + key + "' has no stream id and stream version, so the release could not replay it");
    }

    private static <S extends @Nullable Object> SagaEnvelope<S> withUnreplayableInputQuarantined(SagaEnvelope<S> current, String key, Instant now,
                                                                                                String failureType, String failureMessage) {
        SagaFailure releasing = current.failure();
        if (!current.isReleasing() || releasing == null) {
            throw new IllegalStateException("Saga instance '" + current.sagaId() + "' is not releasing, so it cannot be quarantined again on '" + key + "'.");
        }
        Long position = key.equals(releasing.input()) ? releasing.position() : null;
        SagaFailure record = new SagaFailure(key, position, now, failureType, failureMessage, null, releasing.quarantineId());
        return requarantined(current, now, record, current.streamMarks());
    }

    // Quarantined again during a release, under the same quarantine id. The skip count stays, since the records after
    // the failing one stay. SagaCodec refuses to read a releasing instance without one, so 0 is only ever taken for
    // an envelope built in code. The marks keep every version the release applied, since trimming them to the window
    // would put a record that stays behind it, and the next release would delete that record without applying it
    private static <S extends @Nullable Object> SagaEnvelope<S> requarantined(SagaEnvelope<S> current, Instant now, SagaFailure record,
                                                                             Map<String, StreamMarks> streamMarks) {
        Long count = current.quarantineSkipCount();
        return withBookkeeping(current, SagaStatus.QUARANTINED, now, record, streamMarks, count == null ? 0L : count);
    }

    /** The {@link SagaFailure#failureType()} of an instance quarantined again because an event it replays is missing from the store. */
    public static final String MISSING_EVENT_FAILURE_TYPE = "EventMissingFromStore";

    /**
     * The {@link SagaFailure#failureType()} of an instance quarantined again because an event it replays has no stream id
     * and stream version.
     */
    public static final String EVENT_WITHOUT_STREAM_KEY_FAILURE_TYPE = "EventWithoutStreamKey";

    // No reorder window, so the marks forget no version however far apart two versions are, see replay(..)
    private static final OptionalInt NO_REORDER_WINDOW = OptionalInt.empty();

    private static final String MISSING_INPUT_MESSAGE_END = "' is missing from the event store, so the release could not replay it";

    /** Whether {@code failure} is the record {@link #withMissingInputQuarantined} writes. */
    public static boolean namesAMissingInput(@Nullable SagaFailure failure) {
        return failure != null && MISSING_EVENT_FAILURE_TYPE.equals(failure.failureType())
               && ("The event '" + failure.input() + MISSING_INPUT_MESSAGE_END).equals(failure.failureMessage());
    }

    // The instance one version up with its status, failure record and quarantine bookkeeping replaced. The state,
    // timers and watermarks stay as they are, and the position watermark is dropped, as everywhere else.
    private static <S extends @Nullable Object> SagaEnvelope<S> withBookkeeping(SagaEnvelope<S> current, SagaStatus status, @Nullable Instant updatedAt,
                                                                               @Nullable SagaFailure failure, Map<String, StreamMarks> streamMarks,
                                                                               @Nullable Long quarantineSkipCount) {
        return new SagaEnvelope<>(current.sagaId(), current.state(), status, current.version() + 1, current.timers(),
                current.streamWatermarks(), null, current.createdAt(), updatedAt, current.completedAt(), current.currentStep(),
                current.started(), failure, streamMarks, quarantineSkipCount);
    }

    /**
     * What to write when an input has failed. {@link #envelope()} holds the failure record to save with
     * {@code compareAndSaveWithoutState(..., expectedVersion())}, and {@link #quarantined()} says whether the budget has
     * now elapsed, meaning the record quarantines the instance if the executor goes on to write it. What the executor
     * checks before writing it is listed on {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED}.
     * <p>
     * Without the state, and not {@code compareAndSave}, because the envelope this carries came from a read that did not
     * decode the state, so saving it whole would erase the state of the instance this record is about.
     */
    public record FailureRecord<S extends @Nullable Object>(SagaEnvelope<S> envelope, long expectedVersion, boolean quarantined) {
    }

    /**
     * Decide what a failed input costs the instance, or {@code null} when it costs it nothing and the exception should
     * simply propagate the way it always has.
     * <p>
     * A failure on an instance with no record produces one, naming the input and when it first failed. A later failure
     * of the input the record already names compares the elapsed time against {@code quarantineAfter} and writes nothing
     * while it is under it and listed pending, so a retry loop hammering one input costs one store write rather than one
     * per attempt. A failure of a different input
     * does write, because the record has to name the input the instance is failing on now, so an instance where two
     * inputs fail in turn writes once per delivery. That is what it cost before this rule too. Past the budget the
     * record quarantines the instance on the input whose failure found the budget used up, if the executor writes it.
     * <p>
     * The elapsed time runs from when the input now failing first failed. That is the instant its pending entry holds,
     * else the record's {@code firstFailedAt} when the record names it, else now. A different input failing rewrites the
     * record to name that input with that input's own instant, so an input parked long ago and replayed today never
     * spends a budget another input started. An instance where two inputs fail in turn still reaches its budget,
     * because each one keeps its own instant on its pending entry, and the clock of neither goes back to zero.
     * <p>
     * "The same input" is {@link EventMeta#redeliveryKey()}, not the global position, so an event from a store that
     * assigns no position is recorded and quarantined like any other. An input carrying no key, which includes one that
     * carries only a position, is refused here, because a record naming it could never be matched against the input
     * that wrote it.
     * <p>
     * A stored record whose input is a position key, written by 0.31.0 to 0.34.0, is treated as no record at all. No
     * input has that key any more, so keeping it would only let an unrelated failure find a budget that ran out long ago.
     * <p>
     * A successful input clears the record only when it is the input the record names. Any other one leaves it alone,
     * including {@code firstFailedAt}, because a saga re-arms its timers explicitly and a timer firing more often than
     * the budget would otherwise put the clock back to zero forever.
     * <p>
     * The budget is wall-clock rather than an attempt count because the retry loop is not always Occurrent's. On the
     * MongoDB subscription models it is a {@code RetryStrategy} the user can replace, and behind a broker source it is
     * the broker's own redelivery. Those run at unrelated rates, so a count means a different amount of time on each
     * while five minutes means five minutes on both. It also follows that a transport which never re-offers a failing
     * input can never reach the budget, and such a saga keeps the behaviour it has always had.
     *
     * @param saga            the saga descriptor, needed for its initial state when the failing input is the one that
     *                        would have created the instance
     * @param sagaId          the correlation id of the instance
     * @param current         the stored envelope, or {@code null} when the failing input would have created it
     * @param meta            the failing event's delivery metadata, which has to carry a redelivery key
     * @param failure         what the saga, or its dispatcher, threw
     * @param now             the current instant
     * @param quarantineAfter how long an input may keep failing before its instance is quarantined
     */
    public static <E, S extends @Nullable Object, C> @Nullable FailureRecord<S> onFailure(Saga<E, S, C> saga,
                                                                                          String sagaId,
                                                                                          @Nullable SagaEnvelope<S> current,
                                                                                          EventMeta meta,
                                                                                          Throwable failure,
                                                                                          Instant now,
                                                                                          Duration quarantineAfter) {
        return onFailure(saga, sagaId, current, meta, failure, now, quarantineAfter, DEFAULT_REORDER_WINDOW);
    }

    /**
     * As {@link #onFailure(Saga, String, SagaEnvelope, EventMeta, Throwable, Instant, Duration)}, with the reorder window
     * the runner is configured with.
     */
    public static <E, S extends @Nullable Object, C> @Nullable FailureRecord<S> onFailure(Saga<E, S, C> saga,
                                                                                          String sagaId,
                                                                                          @Nullable SagaEnvelope<S> current,
                                                                                          EventMeta meta,
                                                                                          Throwable failure,
                                                                                          Instant now,
                                                                                          Duration quarantineAfter,
                                                                                          int reorderWindow) {
        String input = meta.redeliveryKey();
        if (input == null) {
            // Nothing here tells one delivery of this input from the next, so a record written now could never be
            // matched against the input that wrote it, and the budget could never elapse. That is the same event
            // RedeliveryDetection already refuses under REQUIRED, its default, before it ever reaches the saga.
            return null;
        }
        if (current != null && (current.isCompleted() || current.isQuarantined())) {
            // Neither reaches the saga, so neither is what threw on the first read. A retry of a lost failure write reads
            // the instance again, and another writer may have completed or quarantined it in between
            return null;
        }
        if (current != null && classify(current, meta, reorderWindow) != Delivery.NEW) {
            // The save committed and its response was lost on the way back, so the reloaded instance already counts
            // this input as handled. A record written here is never cleared, because the redelivery that would clear it
            // is skipped as one, so keep the committed outcome instead.
            return null;
        }
        SagaFailure existing = currentFailure(current);
        Instant failingSince = failingSince(current, meta, now);
        if (existing == null) {
            SagaFailure record = new SagaFailure(input, meta.position(), failingSince, failure.getClass().getName(), shortened(failure.getMessage()), null, null);
            boolean budgetUsedUp = Duration.between(failingSince, now).compareTo(quarantineAfter) >= 0;
            return failureRecord(saga, sagaId, current, record, budgetUsedUp ? SagaStatus.QUARANTINED : SagaStatus.ACTIVE, now, budgetUsedUp, meta, null, reorderWindow);
        }
        // Measured from when this input first failed, not from when the instance did. Events 7 and 8 failing in turn as
        // 7, 8, 7, 8 each keep their own instant on their pending entry, so neither clock goes back to now and the
        // instance still reaches its budget. An input parked long ago charges only its own time, never a time another
        // input started.
        boolean budgetUsedUp = Duration.between(failingSince, now).compareTo(quarantineAfter) >= 0;
        if (!budgetUsedUp && existing.input().equals(input)) {
            // The same input failing again inside the budget, so the failure record stays as it is. A different input
            // failing does get written, because the record has to name the input the instance is failing on now. A
            // record written before pending existed names the input without listing it as pending, so the retry lists it,
            // and an input that is pending already writes nothing.
            return pendingRecord(saga, sagaId, current, meta, now, reorderWindow);
        }
        // Name the input failing now, with the instant it first failed and what it is failing with. An input that fails
        // one way and then another is still the same input failing, and the later exception is the more useful one to
        // read.
        SagaFailure record = new SagaFailure(input, meta.position(), failingSince, failure.getClass().getName(), shortened(failure.getMessage()), null, null);
        return failureRecord(saga, sagaId, current, record, budgetUsedUp ? SagaStatus.QUARANTINED : SagaStatus.ACTIVE, now, budgetUsedUp, meta, existing, reorderWindow);
    }

    // When the input meta names first failed: its pending entry's instant, else the record's when the record names it,
    // else now. One rule for the budget check and for every instant written, so a write never moves an input's clock.
    private static <S extends @Nullable Object> Instant failingSince(@Nullable SagaEnvelope<S> current, EventMeta meta, Instant now) {
        StreamMarks marks = current == null || meta.streamId() == null ? null : current.streamMarks().get(meta.streamId());
        Instant pendingSince = marks == null || meta.streamVersion() == null ? null : marks.pendingSince().get(meta.streamVersion());
        if (pendingSince != null) {
            return pendingSince;
        }
        SagaFailure existing = currentFailure(current);
        return existing != null && existing.input().equals(meta.redeliveryKey()) ? existing.firstFailedAt() : now;
    }

    // Only the input the record names clears it. Letting any successful input clear it lets a saga that re-arms a timer
    // more often than the budget reset the clock forever, so an event that never succeeds never reaches the budget, and
    // nothing bounds how long it keeps failing. A timer has no redelivery key, so it never matches.
    // An exception message is application data and has no length the saga controls. A message big enough to push the
    // instance over MongoDB's document limit would fail the failure write on every retry, so the instance would never
    // quarantine and nothing would bound how long its event keeps failing, which is the outcome this whole design
    // removes. The log still gets the whole throwable.
    static final int MAX_FAILURE_MESSAGE_LENGTH = 1000;

    private static @Nullable String shortened(@Nullable String failureMessage) {
        if (failureMessage == null || failureMessage.length() <= MAX_FAILURE_MESSAGE_LENGTH) {
            return failureMessage;
        }
        return failureMessage.substring(0, MAX_FAILURE_MESSAGE_LENGTH) + "... (truncated)";
    }

    private static <S extends @Nullable Object> @Nullable SagaFailure failureAfter(@Nullable SagaEnvelope<S> current, EventMeta meta, boolean terminal) {
        SagaFailure existing = currentFailure(current);
        if (existing == null || terminal) {
            // A completed instance can never quarantine, so a record left on it would describe a budget that can no
            // longer run, and it would sit outside the two states SagaInstance.failure() describes.
            return null;
        }
        return existing.input().equals(meta.redeliveryKey()) ? null : existing;
    }

    // 0.31.0 to 0.34.0 keyed an event that carried only a position as "position:N". Nothing produces that key now, so a
    // record holding one can never be matched or cleared by an input. A stream key always contains an "@", so a stream id
    // that starts with "position:" does not match.
    private static final Pattern LEGACY_POSITION_KEY = Pattern.compile("position:\\d+");

    private static <S extends @Nullable Object> @Nullable SagaFailure currentFailure(@Nullable SagaEnvelope<S> current) {
        SagaFailure stored = current == null ? null : current.failure();
        return stored != null && LEGACY_POSITION_KEY.matcher(stored.input()).matches() ? null : stored;
    }

    private static <E, S extends @Nullable Object, C> FailureRecord<S> failureRecord(Saga<E, S, C> saga, String sagaId,
                                                                                     @Nullable SagaEnvelope<S> current,
                                                                                     @Nullable SagaFailure record, SagaStatus status,
                                                                                     Instant now, boolean quarantined,
                                                                                     EventMeta meta, @Nullable SagaFailure replaced,
                                                                                     int reorderWindow) {
        if (current == null) {
            // An instance whose very first event failed has nothing to attach the record to, so the record inserts one.
            // It holds the initial state and started = false, which is honestly what it is, an instance that failed
            // before it began. Start detection reads that flag rather than the document, so a later redelivery of the
            // start event still runs onStart.
            boolean quarantining = status == SagaStatus.QUARANTINED;
            SagaEnvelope<S> inserted = new SagaEnvelope<>(sagaId, saga.initialState(), status, 1, List.of(), Map.of(),
                    null, now, now, null, null, false, quarantining ? withNewQuarantineId(record) : record,
                    withPending(Map.of(), Map.of(), meta, reorderWindow, now), quarantining ? 0L : null);
            return new FailureRecord<>(inserted, 0, quarantined);
        }
        return new FailureRecord<>(withFailure(current, record, status, now, meta, replaced, reorderWindow), current.version(), quarantined);
    }

    // Deliberately copies the stream watermarks and timers over untouched. A failure advances neither, because the input
    // was not handled, and a quarantined instance's timers stop because the store's due-timer query asks for ACTIVE ones.
    // The stream marks gain the failed version as pending and nothing else. The position watermark is dropped, as in
    // process, since nothing reads it. The write that quarantines the instance gives it a new quarantine id and a skip
    // count of 0. The id is what tells it apart from an instance quarantined by 0.34.0
    private static <S extends @Nullable Object> SagaEnvelope<S> withFailure(SagaEnvelope<S> current, @Nullable SagaFailure record,
                                                                           SagaStatus status, Instant now,
                                                                           EventMeta meta, @Nullable SagaFailure replaced, int reorderWindow) {
        boolean quarantining = status == SagaStatus.QUARANTINED;
        return new SagaEnvelope<>(current.sagaId(), current.state(), status, current.version() + 1, current.timers(),
                current.streamWatermarks(), null, current.createdAt(), now, current.completedAt(),
                current.currentStep(), current.started(), quarantining && record != null ? withNewQuarantineId(record) : record,
                withPending(withReplacedFailure(current.streamMarks(), current.streamWatermarks(), replaced, meta),
                        current.streamWatermarks(), meta, reorderWindow, failingSince(current, meta, now)),
                quarantining ? Long.valueOf(0L) : current.quarantineSkipCount());
    }

    // An instance that goes from active to quarantined starts a quarantine of its own, which the inputs it skips are
    // recorded under. A release that quarantines it again keeps the id instead, see requarantined
    private static SagaFailure withNewQuarantineId(SagaFailure record) {
        return new SagaFailure(record.input(), record.position(), record.firstFailedAt(), record.failureType(), record.failureMessage(),
                record.releasedAt(), UUID.randomUUID().toString());
    }

    // The stream marks after the version meta describes has failed. It is listed as pending among the versions of its
    // type, ascending, with the instant it first failed. Nothing but the save that applies a version removes it, because
    // dropping one acknowledged an event whose reaction never ran once the instance moved the window past it.
    private static Map<String, StreamMarks> withPending(Map<String, StreamMarks> streamMarks, Map<String, Long> streamWatermarks,
                                                       EventMeta meta, int reorderWindow, Instant failingSince) {
        String streamId = Objects.requireNonNull(meta.streamId());
        StreamMarks marks = streamMarks.get(streamId);
        String type = typeKey(meta);
        TreeSet<Long> versions = new TreeSet<>(marks == null ? List.of() : marks.pending().getOrDefault(type, List.of()));
        if (versions.add(Objects.requireNonNull(meta.streamVersion())) && versions.size() == reorderWindow + 1) {
            log.warn("Stream {} has more than {} failed versions of type '{}' outstanding, the reorder window. Each one is kept until it applies, "
                     + "so the instance grows with every further failure until the versions are replayed.",
                    streamId, reorderWindow, type);
        }
        Map<Long, Instant> pendingSince = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pendingSince());
        pendingSince.putIfAbsent(meta.streamVersion(), failingSince);
        Map<String, List<Long>> pending = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pending());
        pending.put(type, List.copyOf(versions));
        Map<String, StreamMarks> updated = new LinkedHashMap<>(streamMarks);
        // A version that failed again under its own type is pending now, so it stops being a failure of unknown type.
        long failedVersion = meta.streamVersion();
        List<Long> legacy = marks == null ? List.of() : marks.legacyFailed().stream().filter(v -> v != failedVersion).toList();
        updated.put(streamId, new StreamMarks(floorOf(marks, streamWatermarks.get(streamId)), marks == null ? Map.of() : marks.byType(), pending, legacy, pendingSince));
        return updated;
    }

    // A failure record names one input, so a different input failing replaces it. A record written before pending
    // existed is then the only memory that its input was never applied, and its event type is not known. It is kept as a
    // version of the stream, which a stream version identifies exactly. Only such records can get here unlisted, because a
    // failure recorded now is pending in the same write, so this holds at most the record an instance carried over from
    // before.
    private static Map<String, StreamMarks> withReplacedFailure(Map<String, StreamMarks> streamMarks, Map<String, Long> streamWatermarks,
                                                               @Nullable SagaFailure replaced, EventMeta meta) {
        if (replaced == null || replaced.input().equals(meta.redeliveryKey())) {
            return streamMarks;
        }
        int at = replaced.input().lastIndexOf('@');
        if (at < 0) {
            return streamMarks;
        }
        String streamId = replaced.input().substring(0, at);
        long version;
        try {
            version = Long.parseLong(replaced.input().substring(at + 1));
        } catch (NumberFormatException notAStreamKey) {
            return streamMarks;
        }
        StreamMarks marks = streamMarks.get(streamId);
        // The record no longer names its input, so the input's clock moves to the marks with it
        Map<Long, Instant> pendingSince = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pendingSince());
        pendingSince.putIfAbsent(version, replaced.firstFailedAt());
        Map<String, StreamMarks> updated = new LinkedHashMap<>(streamMarks);
        if (marks != null && (marks.legacyFailed().contains(version)
                              || marks.pending().values().stream().anyMatch(versions -> versions.contains(version)))) {
            // Listed already. A store that does not keep the instants can leave it without one, and the record's is the
            // only one left.
            if (marks.pendingSince().containsKey(version)) {
                return streamMarks;
            }
            updated.put(streamId, new StreamMarks(marks.floor(), marks.byType(), marks.pending(), marks.legacyFailed(), pendingSince));
            return updated;
        }
        TreeSet<Long> legacy = new TreeSet<>(marks == null ? List.of() : marks.legacyFailed());
        legacy.add(version);
        updated.put(streamId, new StreamMarks(floorOf(marks, streamWatermarks.get(streamId)), marks == null ? Map.of() : marks.byType(),
                marks == null ? Map.of() : marks.pending(), List.copyOf(legacy), pendingSince));
        return updated;
    }


    private static <E, S extends @Nullable Object, C> @Nullable E startEventOrNull(Saga<E, S, C> saga, boolean hasStarted, SagaInput<E> input) {
        if (hasStarted || !(input instanceof SagaInput.Event<E> ev)) {
            return null;
        }
        E event = ev.event();
        for (Class<? extends E> startType : saga.startEventTypes()) {
            if (startType.isInstance(event)) {
                return event;
            }
        }
        return null;
    }

    /**
     * Whether {@code current} has already handled the input {@code meta} describes, or is too far past it to say. An
     * input without a stream id and a stream version is never a redelivery, whatever its position.
     * Public because the executor needs the same answer on a path where it could not load the instance, and two copies
     * of this rule would be free to disagree about which instance is past which input.
     */
    public static <S extends @Nullable Object> boolean isRedelivery(SagaEnvelope<S> current, EventMeta meta) {
        return isRedelivery(current, meta, DEFAULT_REORDER_WINDOW);
    }

    /** As {@link #isRedelivery(SagaEnvelope, EventMeta)}, with the reorder window the runner is configured with. */
    public static <S extends @Nullable Object> boolean isRedelivery(SagaEnvelope<S> current, EventMeta meta, int reorderWindow) {
        return classify(current, meta, reorderWindow) != Delivery.NEW;
    }

    /**
     * What {@code current} makes of the input {@code meta} describes. A stream event is told apart by its stream id,
     * its cloud event type and its stream version, because that is the order a feed can keep. Kafka orders one stream's
     * events of one type against each other and nothing across types, so the highest version applied is kept per type
     * and a lower version of another type is not a redelivery of it.
     * <p>
     * Within one type, a version above the highest applied is new. A version at it, or among the {@code applied} ones
     * below it, was applied. Any other version less than {@code reorderWindow} below it is a late arrival and is new.
     * One further behind than that is skipped as {@link Delivery#BEHIND_WINDOW}, since remembering every version would
     * make the state grow with the stream, and a restart that replays old events must not run their reactions again.
     * Skipping is wrong only for a version that was never applied, and a feed that delivers a version more than
     * {@code reorderWindow} behind the highest one, when that version did not fail, cannot be told from a replay, so the
     * version is lost. The exceptions are a pending version, which failed in the saga and so was not applied, and the
     * input the instance's failure record names. Both stay new however far behind they are. A releasing instance keeps
     * its failure record until the release ends, so there only the pending versions count.
     * <p>
     * An instance stored before 0.35.0 has a watermark per stream and no marks. Everything at or below that watermark
     * was applied, so it stays skipped whatever its type, and the type marks start from the next event.
     */
    public static <S extends @Nullable Object> Delivery classify(SagaEnvelope<S> current, EventMeta meta, int reorderWindow) {
        return classify(current, meta, OptionalInt.of(reorderWindow));
    }

    private static <S extends @Nullable Object> Delivery classify(SagaEnvelope<S> current, EventMeta meta, OptionalInt reorderWindow) {
        if (meta.streamId() == null || meta.streamVersion() == null) {
            return Delivery.NEW;
        }
        long version = meta.streamVersion();
        StreamMarks marks = current.streamMarks().get(meta.streamId());
        Long watermark = current.streamWatermarks().get(meta.streamId());
        long floor = floorOf(marks, watermark);
        if (version <= floor) {
            // A pending version or a failure record naming this input says it was never applied, whatever the floor says.
            // Later deliveries keep an unrelated failure record and only the save that applies the input clears it.
            // A release keeps the failure record while it replays, so a releasing instance goes by its pending versions
            // alone, which the quarantine listed this input among and the replay that applies it removes it from.
            return isOutstanding(marks, meta) || (!current.isReleasing() && namesFailureOf(current, meta))
                    ? Delivery.NEW : Delivery.REDELIVERY;
        }
        TypeMark typeMark = marks == null ? null : marks.byType().get(typeKey(meta));
        if (typeMark == null || version > typeMark.mark()) {
            return Delivery.NEW;
        }
        if (version == typeMark.mark() || typeMark.applied().contains(version)) {
            return Delivery.REDELIVERY;
        }
        // A pending version failed in the saga and was not applied, however far behind the mark it is by now.
        if (isOutstanding(marks, meta)) {
            return Delivery.NEW;
        }
        // The horizon outlasts a window that is raised later, because the versions it forgot are not in applied any more.
        if (version > horizon(typeMark.horizon(), typeMark.mark(), reorderWindow)) {
            return Delivery.NEW;
        }
        // A failure record naming this input says it was never applied, since the save that applies it clears the record.
        // It covers an instance written before pending existed, and skipping it as behind the window would acknowledge
        // an event whose reaction never ran. A releasing instance keeps that record after its replay applied the input,
        // so there the record says nothing and the pending versions checked above decide alone.
        return !current.isReleasing() && namesFailureOf(current, meta) ? Delivery.NEW : Delivery.BEHIND_WINDOW;
    }

    /**
     * What to write so that the stream event {@code meta} describes, which has just failed, is applied when it arrives
     * again however far behind the window it is by then. Pending only, this changes no failure record and no status.
     * <p>
     * {@code null} when there is nothing to write, which is when the event carries no stream id and version, the
     * instance is completed or quarantined, the instance already counts the input as handled, or the input is pending
     * already.
     * Written without the state, like {@link #onFailure}.
     */
    public static <E, S extends @Nullable Object, C> @Nullable FailureRecord<S> pendingRecord(Saga<E, S, C> saga, String sagaId,
                                                                                              @Nullable SagaEnvelope<S> current,
                                                                                              EventMeta meta, Instant now,
                                                                                              int reorderWindow) {
        if (meta.redeliveryKey() == null) {
            return null;
        }
        if (current != null && (current.isCompleted() || current.isQuarantined())) {
            return null;
        }
        // Already pending classifies as NEW too, so it is told apart here and there is nothing to write for it.
        if (current != null && (classify(current, meta, reorderWindow) != Delivery.NEW || isPending(current.streamMarks().get(meta.streamId()), meta))) {
            return null;
        }
        return failureRecord(saga, sagaId, current, current == null ? null : current.failure(), currentStatus(current), now, false, meta, null, reorderWindow);
    }

    /**
     * The instance with the timer {@code attempted} due again from {@code nextAttemptAt}, so the due-timer query orders
     * it behind the instances due before that. Save it with
     * {@code compareAndSaveWithoutState(..., current.version())}. It changes the timer and nothing else the executor
     * reads. The version stays the same, so an event that read the instance before the mark still wins its
     * compare-and-set instead of losing it to every poll. That save writes the timers it read, so it drops the mark, and the
     * timer is due at its {@code firesAt} again until it fails once more. {@code updatedAt} stays where it was, because
     * a failed fire is no progress.
     * <p>
     * {@code null} when there is nothing to write. That is when the instance is gone, completed or quarantined, when it
     * no longer has a timer of that name set to fire at that instant, or when the timer is already marked at or after
     * {@code nextAttemptAt}.
     */
    public static <S extends @Nullable Object> @Nullable SagaEnvelope<S> withFailedTimerAttempt(@Nullable SagaEnvelope<S> current,
                                                                                               TimerEntry attempted,
                                                                                               Instant nextAttemptAt) {
        if (current == null || current.status() != SagaStatus.ACTIVE) {
            return null;
        }
        long nextAttemptAtMillis = nextAttemptAt.toEpochMilli();
        List<TimerEntry> timers = new ArrayList<>(current.timers().size());
        boolean marked = false;
        for (TimerEntry timer : current.timers()) {
            Long markedAt = timer.nextAttemptAtEpochMilli();
            if (timer.name().equals(attempted.name()) && timer.firesAtEpochMilli() == attempted.firesAtEpochMilli()
                && (markedAt == null || markedAt < nextAttemptAtMillis)) {
                timers.add(new TimerEntry(timer.name(), timer.firesAtEpochMilli(), nextAttemptAtMillis));
                marked = true;
            } else {
                timers.add(timer);
            }
        }
        if (!marked) {
            return null;
        }
        return new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), timers,
                current.streamWatermarks(), null, current.createdAt(), current.updatedAt(),
                current.completedAt(), current.currentStep(), current.started(), current.failure(), current.streamMarks(),
                current.quarantineSkipCount());
    }

    // Pending, or a failure recorded before pending existed. Either says the version was handed to the saga and failed.
    private static boolean isOutstanding(@Nullable StreamMarks marks, EventMeta meta) {
        return isPending(marks, meta) || marks != null && meta.streamVersion() != null && marks.legacyFailed().contains(meta.streamVersion());
    }

    private static boolean isPending(@Nullable StreamMarks marks, EventMeta meta) {
        return marks != null && meta.streamVersion() != null && marks.pending().getOrDefault(typeKey(meta), List.of()).contains(meta.streamVersion());
    }

    private static <S extends @Nullable Object> SagaStatus currentStatus(@Nullable SagaEnvelope<S> current) {
        return current == null ? SagaStatus.ACTIVE : current.status();
    }

    private static <S extends @Nullable Object> boolean namesFailureOf(SagaEnvelope<S> current, EventMeta meta) {
        SagaFailure failure = currentFailure(current);
        return failure != null && failure.input().equals(meta.redeliveryKey());
    }

    // Everything at or below the floor was applied before versions were recorded per type. A stream the instance has
    // never seen has no floor, which Long.MIN_VALUE says without assuming where a stream's versions start.
    private static long floorOf(@Nullable StreamMarks marks, @Nullable Long watermark) {
        if (marks != null) {
            return marks.floor();
        }
        return watermark != null ? watermark : Long.MIN_VALUE;
    }

    private static String typeKey(EventMeta meta) {
        return meta.eventType() == null ? "" : meta.eventType();
    }

    // The highest version a type mark forgets, which with no reorder window is only the horizon it has already
    private static long horizon(long horizon, long mark, OptionalInt reorderWindow) {
        return reorderWindow.isPresent() ? Math.max(horizon, mark - reorderWindow.getAsInt()) : horizon;
    }

    // The marks of one stream after recording the version meta describes. The floor is fixed when the stream first gets
    // marks, at the watermark the instance had then, which is what the pre-0.35.0 rule would have skipped up to.
    private static StreamMarks recorded(@Nullable StreamMarks marks, @Nullable Long watermark, EventMeta meta, OptionalInt reorderWindow) {
        long floor = floorOf(marks, watermark);
        long version = Objects.requireNonNull(meta.streamVersion());
        Map<String, TypeMark> byType = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.byType());
        TypeMark previous = byType.get(typeKey(meta));
        TreeSet<Long> applied = new TreeSet<>(previous == null ? List.of() : previous.applied());
        long mark;
        if (previous == null) {
            mark = version;
        } else if (version > previous.mark()) {
            applied.add(previous.mark());
            mark = version;
        } else {
            applied.add(version);
            mark = previous.mark();
        }
        // Past the window a version is skipped, so remembering it would only grow the state.
        long horizon = horizon(previous == null ? Long.MIN_VALUE : previous.horizon(), mark, reorderWindow);
        applied.removeIf(earlier -> earlier <= horizon);
        byType.put(typeKey(meta), new TypeMark(mark, List.copyOf(applied), horizon));
        // Applying a version settles it, so it stops being outstanding.
        Map<String, List<Long>> pending = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pending());
        List<Long> outstanding = pending.getOrDefault(typeKey(meta), List.of());
        if (outstanding.contains(version)) {
            List<Long> remaining = outstanding.stream().filter(v -> v != version).toList();
            if (remaining.isEmpty()) {
                pending.remove(typeKey(meta));
            } else {
                pending.put(typeKey(meta), remaining);
            }
        }
        List<Long> legacy = marks == null ? List.of() : marks.legacyFailed().stream().filter(v -> v != version).toList();
        Map<Long, Instant> pendingSince = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pendingSince());
        pendingSince.remove(version);
        return new StreamMarks(floor, byType, pending, legacy, pendingSince);
    }

    private static <C> void applyEffects(List<SagaEffect<C>> effects, List<C> commands, Map<String, TimerEntry> timers, Instant now) {
        for (SagaEffect<C> effect : effects) {
            switch (effect) {
                case SagaEffect.IssueCommand<C> issue -> commands.add(issue.command());
                case SagaEffect.StartTimeout<C> start -> arm(timers, start.timerName().encode(), now.plus(start.after()).toEpochMilli());
                case SagaEffect.StartTimeoutAt<C> startAt -> arm(timers, startAt.timerName().encode(), startAt.at().toEpochMilli());
                case SagaEffect.CancelTimeout<C> cancel -> timers.remove(cancel.timerName().encode());
            }
        }
    }

    // A timer armed again for the instant it already fires at keeps its failed attempt, so an input that repeats the same
    // deadline does not move a timer that keeps failing back ahead of the instances that were due before it. Any other
    // instant is a new deadline and starts clean
    private static void arm(Map<String, TimerEntry> timers, String name, long firesAtEpochMilli) {
        TimerEntry existing = timers.get(name);
        Long nextAttemptAtEpochMilli = existing != null && existing.firesAtEpochMilli() == firesAtEpochMilli ? existing.nextAttemptAtEpochMilli() : null;
        timers.put(name, new TimerEntry(name, firesAtEpochMilli, nextAttemptAtEpochMilli));
    }
}
