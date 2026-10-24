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
     * unlucky matters here more than it would elsewhere, because quarantine has no way out. Nothing in 0.34.0 releases
     * an instance, so {@code SagaStateStore.delete(sagaId)} destroying its state is the only exit, and that would stop
     * an instance for good over a heap exhaustion it had nothing to do with.
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
     */
    public record EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position, @Nullable String eventType) {
        public static final EventMeta NONE = new EventMeta(null, null, null, null);

        public EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position) {
            this(streamId, streamVersion, position, null);
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
     * The result of processing one input. When {@link #processed()} is false the executor does nothing (the input was a
     * redelivery, the instance is terminal, or the event cannot start an instance). Otherwise it dispatches
     * {@link #commands()} in order and then saves {@link #envelope()} with {@code compareAndSave(..., expectedVersion())}.
     */
    public record Outcome<S extends @Nullable Object, C>(boolean processed,
                                                         @Nullable SagaEnvelope<S> envelope,
                                                         List<C> commands,
                                                         long expectedVersion) {
        static <S extends @Nullable Object, C> Outcome<S, C> skip() {
            return new Outcome<>(false, null, List.of(), 0);
        }

        static <S extends @Nullable Object, C> Outcome<S, C> processed(SagaEnvelope<S> envelope, List<C> commands, long expectedVersion) {
            return new Outcome<>(true, envelope, commands, expectedVersion);
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
            return Outcome.skip();
        }
        if (current != null && current.isCompleted()) {
            return Outcome.skip();
        }
        if (current != null && current.isQuarantined()) {
            // A quarantined instance is inert. It skips every input addressed to it and its watermarks stay where they are.
            // Advancing one would record an input as handled that the instance never applied, and nothing afterwards can
            // tell that the state has a gap in it.
            return Outcome.skip();
        }
        if (current != null && classify(current, meta, reorderWindow) != Delivery.NEW) {
            return Outcome.skip();
        }

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
                streamMarks);
        return Outcome.processed(next, commands, expectedVersion);
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
     * The first failure on an instance produces a record of when it started failing. A later failure of the input the record already
     * names compares the elapsed time against {@code quarantineAfter} and writes nothing while it is under it, so a
     * retry loop hammering one input costs one store write rather than one per attempt. A failure of a different input
     * does write, because the record has to name the input the instance is failing on now, so an instance where two
     * inputs fail in turn writes once per delivery. That is what it cost before this rule too. Past the budget the
     * record quarantines the instance on the input whose failure found the budget used up, if the executor writes it.
     * <p>
     * The elapsed time runs from when the instance started failing, not from when the input now failing started. A
     * different input failing rewrites the record to name that input and keeps {@code firstFailedAt} where it was, so
     * an instance where two inputs fail in turn still reaches its budget. Letting each one write the current time into
     * the record put the clock back to zero on every delivery, and such an instance never reached its budget. This is
     * the mirror of the rule below, where a successful input
     * clears the record only when it is the input the record names, and it is there for the same reason.
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
            // Neither reaches the saga at all, so neither can be what threw. Defensive rather than reachable.
            return null;
        }
        if (current != null && classify(current, meta, reorderWindow) != Delivery.NEW) {
            // The save committed and its response was lost on the way back, so the reloaded instance already counts
            // this input as handled. A record written here is never cleared, because the redelivery that would clear it
            // is skipped as one, so keep the committed outcome instead.
            return null;
        }
        SagaFailure existing = currentFailure(current);
        if (existing == null) {
            SagaFailure record = new SagaFailure(input, meta.position(), now, failure.getClass().getName(), shortened(failure.getMessage()));
            return failureRecord(saga, sagaId, current, record, SagaStatus.ACTIVE, now, false, meta, null, reorderWindow);
        }
        // Measured from when this instance started failing, whichever input it started on. An instance where two inputs
        // fail in turn, say events at positions 7 and 8 arriving 7, 8, 7, 8, used to have each one write a record
        // holding the current time, so firstFailedAt went back to now on every delivery and the elapsed time never
        // passed the budget. The instance kept blocking the saga's other instances, which is the outcome quarantine
        // exists to remove.
        boolean budgetUsedUp = Duration.between(existing.firstFailedAt(), now).compareTo(quarantineAfter) >= 0;
        if (!budgetUsedUp && existing.input().equals(input)) {
            // The same input failing again inside the budget, so the failure record stays as it is. A different input
            // failing does get written, because the record has to name the input the instance is failing on now. A
            // record written before pending existed names the input without listing it as pending, so the retry lists it,
            // and an input that is pending already writes nothing.
            return pendingRecord(saga, sagaId, current, meta, now, reorderWindow);
        }
        // Keep the instant the failing started, refresh the input and what it is failing with, because an input that
        // fails one way and then another is still the same instance failing, and the later exception is the more useful
        // one to read.
        SagaFailure record = new SagaFailure(input, meta.position(), existing.firstFailedAt(), failure.getClass().getName(), shortened(failure.getMessage()));
        return failureRecord(saga, sagaId, current, record, budgetUsedUp ? SagaStatus.QUARANTINED : SagaStatus.ACTIVE, now, budgetUsedUp, meta, existing, reorderWindow);
    }

    // Only the input the record names clears it. Letting any successful input clear it lets a saga that re-arms a timer
    // more often than the budget reset the clock forever, so an event that never succeeds never reaches the budget and
    // keeps blocking every other instance. A timer has no redelivery key, so it never matches.
    // An exception message is application data and has no length the saga controls. A message big enough to push the
    // instance over MongoDB's document limit would fail the failure write on every retry, so the instance would never
    // quarantine and would keep blocking the subscription, which is the outcome this whole design removes. The log
    // still gets the whole throwable.
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
            SagaEnvelope<S> inserted = new SagaEnvelope<>(sagaId, saga.initialState(), status, 1, List.of(), Map.of(),
                    null, now, now, null, null, false, record, withPending(Map.of(), Map.of(), meta, reorderWindow));
            return new FailureRecord<>(inserted, 0, quarantined);
        }
        return new FailureRecord<>(withFailure(current, record, status, now, meta, replaced, reorderWindow), current.version(), quarantined);
    }

    // Deliberately copies the stream watermarks and timers over untouched. A failure advances neither, because the input
    // was not handled, and a quarantined instance's timers stop because the store's due-timer query asks for ACTIVE ones.
    // The stream marks gain the failed version as pending and nothing else. The position watermark is dropped, as in
    // process, since nothing reads it.
    private static <S extends @Nullable Object> SagaEnvelope<S> withFailure(SagaEnvelope<S> current, @Nullable SagaFailure record,
                                                                           SagaStatus status, Instant now,
                                                                           EventMeta meta, @Nullable SagaFailure replaced, int reorderWindow) {
        return new SagaEnvelope<>(current.sagaId(), current.state(), status, current.version() + 1, current.timers(),
                current.streamWatermarks(), null, current.createdAt(), now, current.completedAt(),
                current.currentStep(), current.started(), record,
                withPending(withReplacedFailure(current.streamMarks(), current.streamWatermarks(), replaced, meta, reorderWindow),
                        current.streamWatermarks(), meta, reorderWindow));
    }

    // The stream marks after the version meta describes has failed. It is listed as pending among the versions of its
    // type, ascending. More than the window many are never kept, because the state would otherwise grow with the number
    // of failures. The smallest goes, which loses that one version to the behind-window skip, and the log says which.
    private static Map<String, StreamMarks> withPending(Map<String, StreamMarks> streamMarks, Map<String, Long> streamWatermarks,
                                                       EventMeta meta, int reorderWindow) {
        String streamId = Objects.requireNonNull(meta.streamId());
        StreamMarks marks = streamMarks.get(streamId);
        String type = typeKey(meta);
        TreeSet<Long> versions = new TreeSet<>(marks == null ? List.of() : marks.pending().getOrDefault(type, List.of()));
        versions.add(Objects.requireNonNull(meta.streamVersion()));
        // More than the window can already be stored when the window was lowered after they were recorded.
        while (versions.size() > reorderWindow) {
            long dropped = versions.pollFirst();
            log.error("Stream {} has more than {} failed versions of type '{}' outstanding, so version {} is no longer remembered as failed. "
                            + "It is skipped as behind the reorder window if it arrives again after the instance moved that far ahead.",
                    streamId, reorderWindow, type, dropped);
        }
        Map<String, List<Long>> pending = marks == null ? new LinkedHashMap<>() : new LinkedHashMap<>(marks.pending());
        pending.put(type, List.copyOf(versions));
        Map<String, StreamMarks> updated = new LinkedHashMap<>(streamMarks);
        // A version that failed again under its own type is pending now, so it stops being a failure of unknown type.
        long failedVersion = meta.streamVersion();
        List<Long> legacy = marks == null ? List.of() : marks.legacyFailed().stream().filter(v -> v != failedVersion).toList();
        updated.put(streamId, new StreamMarks(floorOf(marks, streamWatermarks.get(streamId)), marks == null ? Map.of() : marks.byType(), pending, legacy));
        return updated;
    }

    // A failure record names one input, so a different input failing replaces it. A record written before pending
    // existed is then the only memory that its input was never applied, and its event type is not known. It is kept as a
    // version of the stream, which a stream version identifies exactly. Only such records can get here unlisted, because a
    // failure recorded now is pending in the same write, so this holds at most the record an instance carried over from
    // before, and the window caps it anyway.
    private static Map<String, StreamMarks> withReplacedFailure(Map<String, StreamMarks> streamMarks, Map<String, Long> streamWatermarks,
                                                               @Nullable SagaFailure replaced, EventMeta meta, int reorderWindow) {
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
        if (marks != null && (marks.legacyFailed().contains(version)
                              || marks.pending().values().stream().anyMatch(versions -> versions.contains(version)))) {
            return streamMarks;
        }
        TreeSet<Long> legacy = new TreeSet<>(marks == null ? List.of() : marks.legacyFailed());
        legacy.add(version);
        while (legacy.size() > reorderWindow) {
            long dropped = legacy.pollFirst();
            log.error("Stream {} has more than {} failures recorded before pending existed outstanding, so version {} is no longer remembered as failed. "
                            + "It is skipped as behind the reorder window if it arrives again after the instance moved that far ahead.",
                    streamId, reorderWindow, dropped);
        }
        Map<String, StreamMarks> updated = new LinkedHashMap<>(streamMarks);
        updated.put(streamId, new StreamMarks(floorOf(marks, streamWatermarks.get(streamId)), marks == null ? Map.of() : marks.byType(),
                marks == null ? Map.of() : marks.pending(), List.copyOf(legacy)));
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
     * input the instance's failure record names. Both stay new however far behind they are.
     * <p>
     * An instance stored before 0.35.0 has a watermark per stream and no marks. Everything at or below that watermark
     * was applied, so it stays skipped whatever its type, and the type marks start from the next event.
     */
    public static <S extends @Nullable Object> Delivery classify(SagaEnvelope<S> current, EventMeta meta, int reorderWindow) {
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
            return isOutstanding(marks, meta) || namesFailureOf(current, meta) ? Delivery.NEW : Delivery.REDELIVERY;
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
        if (version > Math.max(typeMark.horizon(), typeMark.mark() - reorderWindow)) {
            return Delivery.NEW;
        }
        // A failure record naming this input says it was never applied, since the save that applies it clears the record.
        // It covers an instance written before pending existed, and skipping it as behind the window would acknowledge
        // an event whose reaction never ran.
        return namesFailureOf(current, meta) ? Delivery.NEW : Delivery.BEHIND_WINDOW;
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

    // The marks of one stream after recording the version meta describes. The floor is fixed when the stream first gets
    // marks, at the watermark the instance had then, which is what the pre-0.35.0 rule would have skipped up to.
    private static StreamMarks recorded(@Nullable StreamMarks marks, @Nullable Long watermark, EventMeta meta, int reorderWindow) {
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
        long horizon = Math.max(previous == null ? Long.MIN_VALUE : previous.horizon(), mark - reorderWindow);
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
        return new StreamMarks(floor, byType, pending, legacy);
    }

    private static <C> void applyEffects(List<SagaEffect<C>> effects, List<C> commands, Map<String, TimerEntry> timers, Instant now) {
        for (SagaEffect<C> effect : effects) {
            switch (effect) {
                case SagaEffect.IssueCommand<C> issue -> commands.add(issue.command());
                case SagaEffect.StartTimeout<C> start ->
                        timers.put(start.timerName().encode(), new TimerEntry(start.timerName().encode(), now.plus(start.after()).toEpochMilli()));
                case SagaEffect.StartTimeoutAt<C> startAt ->
                        timers.put(startAt.timerName().encode(), new TimerEntry(startAt.timerName().encode(), startAt.at().toEpochMilli()));
                case SagaEffect.CancelTimeout<C> cancel -> timers.remove(cancel.timerName().encode());
            }
        }
    }
}
