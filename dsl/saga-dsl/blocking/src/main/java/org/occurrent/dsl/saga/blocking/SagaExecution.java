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

package org.occurrent.dsl.saga.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaTimeout;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, dispatches commands before saving (at-least-once), and retries a lost compare-and-set
 * save. Timeouts re-enter the same path, fenced so a timer no longer present on the (reloaded) envelope is skipped.
 * <p>
 * Dispatch amplification: commands are dispatched before the save, and a lost compare-and-set retries the whole step, so a
 * single input can re-dispatch its entire command list up to {@code maxCasAttempts} times (see {@link SagaRunnerConfig}).
 * A command receiver must therefore be idempotent <em>and</em> tolerate that multiplicity, which is stronger than plain
 * at-least-once: the same input can legitimately dispatch the same command several times within one delivery.
 */
final class SagaExecution<E, S extends @Nullable Object, C> {
    private static final Logger log = LoggerFactory.getLogger(SagaExecution.class);

    private final Saga<E, S, C> saga;
    private final SagaStateStore<S> stateStore;
    private final CommandDispatcher<C> dispatcher;
    private final CloudEventConverter<E> converter;
    private final SagaRunnerConfig config;
    // A lost compare-and-set is an optimistic-concurrency conflict, the same shape GenericApplicationService retries with
    // RetryStrategy and its WriteConditionNotFulfilledException. Retry immediately (no backoff) up to maxCasAttempts, only
    // on the internal CasConflict signal so a real dispatch/save failure still propagates on the first attempt.
    private final RetryStrategy.Retry casRetry;
    // Warn once when contention has eaten past half the retry budget, before the retries exhaust and throw. Sustained
    // contention on one instance points at a hot correlation id or an under-sized maxCasAttempts.
    private final int warnThreshold;

    SagaExecution(Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config) {
        this.saga = saga;
        this.stateStore = stateStore;
        this.dispatcher = dispatcher;
        this.converter = converter;
        this.config = config;
        this.casRetry = RetryStrategy.retry()
                .backoff(Backoff.none())
                .maxAttempts(config.maxCasAttempts())
                .retryIf(CasConflict.class::isInstance);
        this.warnThreshold = config.maxCasAttempts() / 2 + 1;
    }

    void onCloudEvent(CloudEvent cloudEvent) {
        E event = converter.toDomainEvent(cloudEvent);
        String sagaId = saga.sagaId(event);
        if (sagaId == null) {
            return;
        }
        // The full delivery metadata (stream id and version, position, and any CloudEvent extensions) rides on the input
        // so reactions can read it. The separate EventMeta drives redelivery dedup and is derived independently below, so
        // its null-tolerant watermark behaviour is unchanged.
        EventMetadata metadata = EventMetadata.from(cloudEvent);
        process(sagaId, SagaInput.event(event, metadata), extractMeta(cloudEvent), null);
    }

    void pollTimers() {
        // Catch Throwable so a failure never lets the scheduled task die and stop all future polling. The schedule stays
        // alive and the next tick recovers.
        try {
            Instant now = Instant.now();
            long nowMillis = now.toEpochMilli();
            List<SagaEnvelope<S>> due = stateStore.findWithDueTimers(now, config.timerBatchLimit());
            for (SagaEnvelope<S> envelope : due) {
                List<String> dueTimerNames = envelope.timers().stream()
                        .filter(timer -> timer.firesAtEpochMilli() <= nowMillis)
                        .map(TimerEntry::name)
                        .toList();
                for (String timerName : dueTimerNames) {
                    try {
                        process(envelope.sagaId(), SagaInput.timeout(new SagaTimeout(envelope.sagaId(), timerName)), EventMeta.NONE, timerName);
                    } catch (RuntimeException e) {
                        // Keep polling other timers/instances. This one stays due and is retried next poll unless consumed.
                        log.warn("Failed to fire saga timer '{}' for instance '{}'", timerName, envelope.sagaId(), e);
                    }
                }
            }
        } catch (Throwable t) {
            log.warn("Saga timer poll failed", t);
        }
    }

    private void process(String sagaId, SagaInput<E> input, EventMeta meta, @Nullable String requireTimerName) {
        // A lost compare-and-set is a retryable concurrency conflict: signal it with CasConflict so casRetry retries the
        // whole body, and map an exhausted retry to the public SagaConcurrencyException. Any other failure (a throwing
        // dispatcher or store) is not a CasConflict, so retryIf leaves it alone and it propagates on the first attempt.
        casRetry
                .mapError(error -> error instanceof CasConflict
                        ? new SagaConcurrencyException("Failed to save saga '" + sagaId + "' after " + config.maxCasAttempts() + " attempts due to concurrent modification")
                        : error)
                .execute((RetryInfo attempt) -> {
                    Instant now = Instant.now();
                    SagaEnvelope<S> current = stateStore.find(sagaId).orElse(null);
                    if (requireTimerName != null && !hasDueTimer(current, requireTimerName, now)) {
                        return null; // stale/superseded/rescheduled timer, or the instance completed: nothing to fire.
                    }
                    Outcome<S, C> outcome = SagaExecutionSupport.process(saga, sagaId, current, input, meta, now);
                    if (!outcome.processed()) {
                        return null;
                    }
                    // Dispatch before saving so a command is never lost. A lost compare-and-set retries this whole body,
                    // which re-dispatches the entire command list of this input (at-least-once, and up to maxCasAttempts
                    // times). Command receivers must therefore be idempotent and tolerate that multiplicity, not merely
                    // at-least-once.
                    for (C command : outcome.commands()) {
                        dispatcher.dispatch(command);
                    }
                    SagaEnvelope<S> envelope = outcome.envelope();
                    if (envelope != null && stateStore.compareAndSave(sagaId, envelope, outcome.expectedVersion())) {
                        return null;
                    }
                    if (attempt.getAttemptNumber() == warnThreshold) {
                        log.warn("Saga '{}' has lost its compare-and-set save {} times (of a maximum {}) to concurrent writers; each retry re-dispatches the input's commands. Sustained contention may exhaust the retries and raise SagaConcurrencyException.",
                                sagaId, attempt.getAttemptNumber(), config.maxCasAttempts());
                    }
                    // A lost compare-and-set, or an outcome with nothing to save: retry the whole body.
                    throw new CasConflict();
                });
    }

    // Internal signal that a compare-and-set save was lost, so casRetry retries the transition. Never escapes process:
    // an exhausted retry is mapped to SagaConcurrencyException. Carries no message or stack trace, it is control flow.
    private static final class CasConflict extends RuntimeException {
        private CasConflict() {
            super(null, null, false, false);
        }
    }

    // Fence a timeout on due-ness, not just presence: if a concurrent event rescheduled the same timer to a later time
    // (a reset-on-heartbeat pattern), the earlier poll must not fire it early.
    private boolean hasDueTimer(@Nullable SagaEnvelope<S> envelope, String timerName, Instant now) {
        if (envelope == null || envelope.isCompleted()) {
            return false;
        }
        long nowMillis = now.toEpochMilli();
        return envelope.timers().stream().anyMatch(timer -> timer.name().equals(timerName) && timer.firesAtEpochMilli() <= nowMillis);
    }

    private EventMeta extractMeta(CloudEvent cloudEvent) {
        Set<String> extensions = cloudEvent.getExtensionNames();
        String streamId = extensions.contains(OccurrentCloudEventExtension.STREAM_ID) ? OccurrentExtensionGetter.getStreamId(cloudEvent) : null;
        Long streamVersion = extensions.contains(OccurrentCloudEventExtension.STREAM_VERSION) ? OccurrentExtensionGetter.getStreamVersion(cloudEvent) : null;
        // Use the framework's own position accessor, which accepts a Number or String, rather than narrowing to Long.
        Long position = extensions.contains(OccurrentCloudEventExtension.POSITION) ? OccurrentCloudEventExtension.getPosition(cloudEvent) : null;
        return new EventMeta(streamId, streamVersion, position);
    }
}
