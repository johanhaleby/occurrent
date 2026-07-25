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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.cloudevents.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        @Nullable E startEvent = startEventOrNull(saga, current, input);
        boolean starting = current == null;
        if (starting && startEvent == null) {
            // A timeout never starts an instance, and a non-start event with no instance is skipped.
            return Outcome.skip();
        }
        if (!starting && current.isCompleted()) {
            return Outcome.skip();
        }
        if (!starting && isRedelivery(current, meta)) {
            return Outcome.skip();
        }

        S previousState = starting ? saga.initialState() : current.state();
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
        if (!starting) {
            for (TimerEntry timer : current.timers()) {
                timers.put(timer.name(), timer);
            }
        }
        // A timer is one-shot: firing it consumes it, so a timeout that neither cancels its timer nor completes the
        // instance does not re-fire every poll. Recurrence is explicit, via a StartTimeout effect below (which re-adds it).
        if (input instanceof SagaInput.Timeout<E> firedTimer) {
            timers.remove(firedTimer.timeout().timerName());
        }
        applyEffects(effects, commands, timers, now);

        boolean terminal = saga.isTerminal(nextState);
        if (terminal) {
            timers.clear();
        }

        long expectedVersion = starting ? 0 : current.version();
        Instant createdAt = starting ? now : current.createdAt();
        Map<String, Long> streamWatermarks = starting ? new LinkedHashMap<>() : new LinkedHashMap<>(current.streamWatermarks());
        Long positionWatermark = starting ? null : current.positionWatermark();
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
                terminal ? now : null);
        return Outcome.processed(next, commands, expectedVersion);
    }

    private static <E, S extends @Nullable Object, C> @Nullable E startEventOrNull(Saga<E, S, C> saga, @Nullable SagaEnvelope<S> current, SagaInput<E> input) {
        if (current != null || !(input instanceof SagaInput.Event<E> ev)) {
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
                        timers.put(start.timerName(), new TimerEntry(start.timerName(), now.plus(start.after()).toEpochMilli()));
                case SagaEffect.StartTimeoutAt<C> startAt ->
                        timers.put(startAt.timerName(), new TimerEntry(startAt.timerName(), startAt.at().toEpochMilli()));
                case SagaEffect.CancelTimeout<C> cancel -> timers.remove(cancel.timerName());
            }
        }
    }
}
