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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, dispatches commands before saving (at-least-once), and retries a lost compare-and-set
 * save. Timeouts re-enter the same path, fenced so a timer no longer present on the (reloaded) envelope is skipped.
 * <p>
 * An event that keeps failing for one instance is quarantined rather than retried forever. The first failure records
 * when the failing started and rethrows, which is what every version up to 0.33.0 did. Once the failing has lasted
 * at least {@link SagaRunnerConfig#quarantineAfter()}, the instance is marked
 * {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} at that event's position and this class returns normally, so the
 * subscription acknowledges the event and the saga's other instances stop waiting behind it.
 * <p>
 * Dispatch amplification: commands are dispatched before the save, and a lost compare-and-set retries the whole step, so a
 * single input can re-dispatch its entire command list up to {@code maxCasAttempts} times (see {@link SagaRunnerConfig}).
 * A command receiver must therefore be idempotent <em>and</em> tolerate that multiplicity, which is stronger than plain
 * at-least-once: the same input can legitimately dispatch the same command several times within one delivery.
 * <p>
 * A reaction's whole command list is handed to {@link CommandDispatcher#dispatchAll} in one call. What that does with
 * it is the dispatcher's business: the default still calls {@code dispatch} once per command, and only a dispatcher that
 * overrides {@code dispatchAll} writes them together.
 */
final class SagaExecution<E, S extends @Nullable Object, C> {
    private static final Logger log = LoggerFactory.getLogger(SagaExecution.class);

    private final String subscriptionId;
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
    private final AtomicBoolean dedupUnavailableWarningLogged = new AtomicBoolean();

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config) {
        this.subscriptionId = subscriptionId;
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
        EventMeta meta = extractMeta(cloudEvent);
        refuseOrWarnIfRedeliveryCannotBeDetected(meta);
        SagaInput<E> input = SagaInput.event(event, metadata);
        Duration quarantineAfter = config.quarantineAfter();
        if (quarantineAfter == null) {
            process(sagaId, input, meta, null);
            return;
        }
        try {
            process(sagaId, input, meta, null);
        } catch (RuntimeException e) {
            if (!quarantine(sagaId, meta, e, quarantineAfter)) {
                throw e;
            }
        }
    }

    /**
     * Record that this event failed for this instance, and answer whether that ended the instance's time budget. A true
     * answer means the caller returns normally, so the subscription acknowledges the event and every other instance on
     * the shared channel keeps going. A false answer means the exception propagates exactly as it always has.
     * <p>
     * Only the event path calls this. A failing timeout is already isolated per instance by the poller and blocks
     * nothing, and a timeout carries no redelivery key of its own, so there would be nothing to quarantine it on.
     */
    private boolean quarantine(String sagaId, EventMeta meta, RuntimeException failure, Duration quarantineAfter) {
        try {
            Instant now = Instant.now();
            SagaEnvelope<S> current = stateStore.find(sagaId).orElse(null);
            FailureRecord<S> record = SagaExecutionSupport.onFailure(saga, sagaId, current, meta, failure, now, quarantineAfter);
            if (record == null) {
                return false;
            }
            if (!stateStore.compareAndSave(sagaId, record.envelope(), record.expectedVersion())) {
                // Another input advanced the instance while the failing one was being retried, most likely a timer that
                // fired successfully. The failing event now meets different state and may well succeed, so discard this
                // write rather than retry one whose premise has gone. Only a first failure loses its budget that way.
                // A later one keeps the record the winning write left in place, since only the failing input clears it.
                return false;
            }
            if (!record.quarantined()) {
                log.warn("Saga '{}' instance '{}' failed on the event '{}' and is being retried by the subscription. It is quarantined if it keeps failing for {}.",
                        subscriptionId, sagaId, meta.redeliveryKey(), quarantineAfter, failure);
                return false;
            }
            log.error("Saga '{}' instance '{}' kept failing on the event '{}' for {} and is now QUARANTINED. It skips every further event and fires no timers, so the saga's other instances are no longer blocked behind it. Find it with findByStatus(QUARANTINED, ..).",
                    subscriptionId, sagaId, meta.redeliveryKey(), quarantineAfter, failure);
            return true;
        } catch (RuntimeException storeFailure) {
            // The store itself is what is failing, so the first failure write fails too. Rethrowing the original is
            // today's behaviour, and it is the right one, because a saga whose store is unreachable cannot make progress anyway.
            if (storeFailure != failure) {
                // Java refuses to suppress an exception under itself, and a store that rethrows the very object that
                // reached us here would otherwise replace the real failure with IllegalArgumentException.
                failure.addSuppressed(storeFailure);
            }
            return false;
        }
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
                        process(envelope.sagaId(), SagaInput.timeout(envelope.sagaId(), TimerName.parse(timerName)), EventMeta.NONE, timerName);
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
                    // re-dispatching the entire command list (at-least-once, up to maxCasAttempts times), so receivers
                    // must be idempotent and tolerate that multiplicity.
                    dispatcher.dispatchAll(outcome.commands());
                    SagaEnvelope<S> envelope = outcome.envelope();
                    if (envelope != null && stateStore.compareAndSave(sagaId, envelope, outcome.expectedVersion())) {
                        return null;
                    }
                    if (attempt.getAttemptNumber() == warnThreshold) {
                        log.warn("Saga '{}' has lost its compare-and-set save {} times (of a maximum {}) to concurrent writers; each retry re-dispatches the input's commands. Sustained contention may exhaust the retries and raise SagaConcurrencyException.",
                                sagaId, attempt.getAttemptNumber(), config.maxCasAttempts());
                    }
                    // Lost compare-and-set: retry the whole body. Processed outcomes always carry an envelope.
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
        // Quarantined is checked as well as completed, and it is a separate check rather than a widening of that one:
        // a quarantined instance is not finished, it is suspended, and firing its timer would advance its state across
        // the event it stopped on. The store's due-timer query already asks for ACTIVE instances only, so this is a
        // second layer over an instance quarantined between the poll and the fire.
        if (envelope == null || envelope.isCompleted() || envelope.isQuarantined()) {
            return false;
        }
        long nowMillis = now.toEpochMilli();
        return envelope.timers().stream().anyMatch(timer -> timer.name().equals(timerName) && timer.firesAtEpochMilli() <= nowMillis);
    }

    // An event carrying neither a stream id with a version nor a position leaves nothing to compare a redelivery
    // against, so the reaction would run again and issue its commands again. Occurrent's own stored events always carry
    // one, so this means a feed that dropped the extensions on the way in, which it does for every event, not just this
    // one. Under REQUIRED that is refused rather than reacted to, so the throw reaches the subscription model, the
    // event goes unacknowledged, and the feed offers it again until somebody looks. Under BEST_EFFORT the duplication
    // is accepted knowingly, so the warning says so once per runner rather than once per event.
    private void refuseOrWarnIfRedeliveryCannotBeDetected(EventMeta meta) {
        if (meta.carriesRedeliveryKey()) {
            return;
        }
        if (config.redeliveryDetection() == RedeliveryDetection.REQUIRED) {
            throw new SagaRedeliveryDetectionException(
                    "Saga subscription '" + subscriptionId + "' received an event with no streamid, streamversion or position. " +
                    "A redelivered event cannot be recognised as one, so reacting to it would run the reaction again and issue " +
                    "its commands again. Forward the Occurrent CloudEvent extensions from the listener feeding this saga, or " +
                    "set redeliveryDetection to BEST_EFFORT if the feed carries none of them and every reaction is idempotent.");
        }
        if (dedupUnavailableWarningLogged.compareAndSet(false, true)) {
            log.warn("Saga subscription '{}' received an event with no streamid, streamversion or position, and " +
                     "redeliveryDetection is BEST_EFFORT. A redelivered event cannot be recognised as one, so it will run the " +
                     "reaction again and issue its commands again.", subscriptionId);
        }
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
