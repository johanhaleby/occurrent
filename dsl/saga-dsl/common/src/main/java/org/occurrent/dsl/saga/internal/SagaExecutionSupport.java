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
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * The pure, stack-agnostic decision step of the saga executor: given the current envelope (or {@code null} for a new
 * instance), one input, and its delivery metadata, it decides whether to skip (redelivery, terminal, or an event that
 * cannot start an instance) and otherwise computes the next envelope and the commands to dispatch. It performs no I/O, so
 * a blocking or reactor runner can wrap it identically. The runner owns loading, the compare-and-set save, dispatch, and
 * timer polling.
 * <p>
 * Redelivery is deduplicated from the event's stream version (per stream) or its global position. An event that carries
 * neither, which Occurrent's own stored events always do, cannot be deduplicated and is re-folded on redelivery, so a
 * custom event source feeding a saga must carry the stream or position extension for the fold to be redelivery-safe.
 */
public final class SagaExecutionSupport {

    private static final Logger log = LoggerFactory.getLogger(SagaExecutionSupport.class);

    private SagaExecutionSupport() {
    }

    /** Delivery metadata used to deduplicate a redelivered event. All fields are {@code null} for a timer input. */
    public record EventMeta(@Nullable String streamId, @Nullable Long streamVersion, @Nullable Long position) {
        public static final EventMeta NONE = new EventMeta(null, null, null);

        /**
         * Whether this carries enough to tell a redelivery from a new event, which is a stream id together with a
         * stream version, or a position. {@code isRedelivery} looks at the same two things, so the rule lives here
         * instead of being written twice.
         */
        public boolean carriesRedeliveryKey() {
            return (streamId != null && streamVersion != null) || position != null;
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
        // Whether onStart has ever run, which is not the same question as whether a document exists. An instance whose
        // very first event failed has an envelope holding nothing but that failure. Keying on the document would leave
        // such an instance permanently "already started", so its start event would be skipped after a release and
        // onStart would never run, with nothing anywhere saying so.
        boolean hasStarted = current != null && current.started();
        @Nullable E startEvent = startEventOrNull(saga, hasStarted, input);
        if (!hasStarted && startEvent == null) {
            // A timeout never starts an instance, and a non-start event for an instance that has not started is skipped.
            return Outcome.skip();
        }
        if (current != null && current.isCompleted()) {
            return Outcome.skip();
        }
        if (current != null && current.isQuarantined() && !hasReachedReleasePosition(current, meta)) {
            // A quarantined instance is inert. It skips every input addressed to it and its watermarks stay where they are.
            // Advancing one here would make the replay after a release treat the input as already handled and skip it a
            // second time, which is the loss the quarantine exists to avoid. A released instance stays inert too until
            // the replay reaches the position it stopped at, so a live event cannot be applied across the gap first.
            return Outcome.skip();
        }
        if (current != null && isRedelivery(current, meta)) {
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
        Long positionWatermark = isNew ? null : current.positionWatermark();
        if (meta.streamId() != null && meta.streamVersion() != null) {
            streamWatermarks.merge(meta.streamId(), meta.streamVersion(), Math::max);
        } else if (meta.position() != null) {
            positionWatermark = positionWatermark == null ? meta.position() : Math.max(positionWatermark, meta.position());
        }

        SagaEnvelope<S> next = new SagaEnvelope<>(
                sagaId,
                nextState,
                terminal ? SagaStatus.COMPLETED : SagaStatus.ACTIVE,
                expectedVersion + 1,
                List.copyOf(timers.values()),
                streamWatermarks,
                positionWatermark,
                createdAt,
                now,
                terminal ? now : null,
                // Derived from nextState by the envelope's constructor; nothing sensible to pass here.
                null,
                true,
                // An input that got through clears the failure record, including the one a release was waiting on. The
                // budget is about an input that keeps failing, so any input the instance handles ends it.
                null);
        return Outcome.processed(next, commands, expectedVersion);
    }

    /**
     * What to write when an input has failed. {@link #envelope()} holds the failure record to save with
     * {@code compareAndSave(..., expectedVersion())}, and {@link #quarantined()} says whether the budget has now
     * elapsed, meaning the executor stops rethrowing and lets the subscription move past the input.
     */
    public record FailureRecord<S extends @Nullable Object>(SagaEnvelope<S> envelope, long expectedVersion, boolean quarantined) {
    }

    /**
     * Decide what a failed input costs the instance, or {@code null} when it costs it nothing and the exception should
     * simply propagate the way it always has.
     * <p>
     * The first failure of an input records when it started failing. Every later failure of the same input compares the
     * elapsed time against {@code quarantineAfter} and writes nothing while it is under it, so the cost is one store
     * write per failing input rather than one per retry. Past the budget the instance is quarantined at the failing
     * input's position.
     * <p>
     * The budget is wall-clock rather than an attempt count because the retry loop is not always Occurrent's. On the
     * MongoDB subscription models it is a {@code RetryStrategy} the user can replace, and behind a broker bridge it is
     * the broker's own redelivery. Those run at unrelated rates, so a count means a different amount of time on each
     * while five minutes means five minutes on both. It also follows that a transport which never re-offers a failing
     * input can never reach the budget, and such a saga keeps the behaviour it has always had.
     *
     * @param saga            the saga descriptor, needed for its initial state when the failing input is the one that
     *                        would have created the instance
     * @param sagaId          the correlation id of the instance
     * @param current         the stored envelope, or {@code null} when the failing input would have created it
     * @param meta            the failing event's delivery metadata
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
        Long position = meta.position();
        if (position == null) {
            // Nothing to release from later, so quarantining would acknowledge the input with no way to ask for it
            // again, which is the loss this design exists to avoid. Keep rethrowing instead.
            return null;
        }
        if (current != null && (current.isCompleted() || current.isQuarantined())) {
            // Neither reaches the saga at all, so neither can be what threw. Defensive rather than reachable.
            return null;
        }
        String input = redeliveryKeyOf(meta);
        SagaFailure existing = current == null ? null : current.failure();
        if (existing == null || !existing.input().equals(input)) {
            SagaFailure record = new SagaFailure(input, position, now, failure.getClass().getName(), failure.getMessage(), null);
            return failureRecord(saga, sagaId, current, record, SagaStatus.ACTIVE, now, false);
        }
        if (Duration.between(existing.firstFailedAt(), now).compareTo(quarantineAfter) < 0) {
            return null;
        }
        // Keep the instant the failing started, refresh what it is failing with, because an input that fails one way and then
        // another is still the same input failing, and the later exception is the more useful one to read.
        SagaFailure record = new SagaFailure(input, position, existing.firstFailedAt(), failure.getClass().getName(), failure.getMessage(), null);
        return failureRecord(saga, sagaId, current, record, SagaStatus.QUARANTINED, now, true);
    }

    /**
     * The envelope that marks {@code current} released, so it accepts an input again once a replay reaches the position
     * it stopped at, or {@code null} when the instance is not quarantined and there is nothing to release.
     */
    public static <S extends @Nullable Object> @Nullable FailureRecord<S> onRelease(SagaEnvelope<S> current, Instant now) {
        SagaFailure failure = current.failure();
        if (!current.isQuarantined() || failure == null) {
            return null;
        }
        SagaEnvelope<S> released = withFailure(current, failure.released(now), SagaStatus.QUARANTINED, now);
        return new FailureRecord<>(released, current.version(), true);
    }

    /**
     * The envelope that takes the release mark back off {@code current}, for a release whose replay could not be
     * started after all, or {@code null} when there is nothing to take back.
     */
    public static <S extends @Nullable Object> @Nullable FailureRecord<S> onReleaseUndone(SagaEnvelope<S> current, Instant now) {
        SagaFailure failure = current.failure();
        if (!current.isQuarantined() || failure == null || !failure.isReleased()) {
            return null;
        }
        SagaFailure unreleased = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(),
                failure.failureType(), failure.failureMessage(), null);
        return new FailureRecord<>(withFailure(current, unreleased, SagaStatus.QUARANTINED, now), current.version(), true);
    }

    /** The redelivery key that identifies one input, read the same way round as {@link #isRedelivery}. */
    private static String redeliveryKeyOf(EventMeta meta) {
        if (meta.streamId() != null && meta.streamVersion() != null) {
            return meta.streamId() + "@" + meta.streamVersion();
        }
        return "position:" + meta.position();
    }

    private static <E, S extends @Nullable Object, C> FailureRecord<S> failureRecord(Saga<E, S, C> saga, String sagaId,
                                                                                     @Nullable SagaEnvelope<S> current,
                                                                                     SagaFailure record, SagaStatus status,
                                                                                     Instant now, boolean quarantined) {
        if (current == null) {
            // An instance whose very first event failed has nothing to attach the record to, so the record inserts one.
            // It holds the initial state and started = false, which is honestly what it is, an instance that failed
            // before it began. Start detection reads that flag, so a release replays the start event and onStart runs.
            SagaEnvelope<S> inserted = new SagaEnvelope<>(sagaId, saga.initialState(), status, 1, List.of(), Map.of(),
                    null, now, now, null, null, false, record);
            return new FailureRecord<>(inserted, 0, quarantined);
        }
        return new FailureRecord<>(withFailure(current, record, status, now), current.version(), quarantined);
    }

    // Deliberately copies the watermarks and timers over untouched. A failure advances neither, because the input was not
    // handled, and a quarantined instance's timers stop because the store's due-timer query asks for ACTIVE ones.
    private static <S extends @Nullable Object> SagaEnvelope<S> withFailure(SagaEnvelope<S> current, SagaFailure record,
                                                                           SagaStatus status, Instant now) {
        return new SagaEnvelope<>(current.sagaId(), current.state(), status, current.version() + 1, current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), now, current.completedAt(),
                current.currentStep(), current.started(), record);
    }

    // Whether a released instance's replay has come back round to the input it stopped on. Until it has, the instance is
    // as inert as it was before the release. Clearing the record first would let a newer event be applied to state with the gap
    // still in it, and that gap is undetectable afterwards.
    private static <S extends @Nullable Object> boolean hasReachedReleasePosition(SagaEnvelope<S> current, EventMeta meta) {
        SagaFailure failure = current.failure();
        if (failure == null || !failure.isReleased()) {
            return false;
        }
        Long position = meta.position();
        return position != null && position >= failure.position();
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

    private static <S extends @Nullable Object> boolean isRedelivery(SagaEnvelope<S> current, EventMeta meta) {
        if (meta.streamId() != null && meta.streamVersion() != null) {
            Long watermark = current.streamWatermarks().get(meta.streamId());
            return watermark != null && meta.streamVersion() <= watermark;
        }
        if (meta.position() != null) {
            Long watermark = current.positionWatermark();
            return watermark != null && meta.position() <= watermark;
        }
        return false;
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
