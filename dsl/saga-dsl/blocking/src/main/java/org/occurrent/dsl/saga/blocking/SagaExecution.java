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
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
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

    SagaExecution(Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config) {
        this.saga = saga;
        this.stateStore = stateStore;
        this.dispatcher = dispatcher;
        this.converter = converter;
        this.config = config;
    }

    void onCloudEvent(CloudEvent cloudEvent) {
        E event = converter.toDomainEvent(cloudEvent);
        String sagaId = saga.sagaId(event);
        if (sagaId == null) {
            return;
        }
        process(sagaId, SagaInput.event(event), extractMeta(cloudEvent), null);
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
        // Warn once when contention has eaten past half the retry budget, before the loop exhausts it and throws. Sustained
        // contention on one instance points at a hot correlation id or an under-sized maxCasAttempts, and is worth surfacing
        // early rather than only as the terminal SagaConcurrencyException.
        int warnThreshold = Math.max(1, config.maxCasAttempts() / 2);
        for (int attempt = 0; attempt < config.maxCasAttempts(); attempt++) {
            Instant now = Instant.now();
            SagaEnvelope<S> current = stateStore.find(sagaId).orElse(null);
            if (requireTimerName != null && !hasDueTimer(current, requireTimerName, now)) {
                return; // stale/superseded/rescheduled timer, or the instance completed: nothing to fire.
            }
            Outcome<S, C> outcome = SagaExecutionSupport.process(saga, sagaId, current, input, meta, now);
            if (!outcome.processed()) {
                return;
            }
            // Dispatch before saving so a command is never lost. A lost compare-and-set retry re-runs this whole loop body,
            // which re-dispatches the entire command list of this input (at-least-once, and up to maxCasAttempts times).
            // Command receivers must therefore be idempotent and tolerate that multiplicity, not merely at-least-once.
            for (C command : outcome.commands()) {
                dispatcher.dispatch(command);
            }
            SagaEnvelope<S> envelope = outcome.envelope();
            if (envelope != null && stateStore.compareAndSave(sagaId, envelope, outcome.expectedVersion())) {
                return;
            }
            if (attempt + 1 == warnThreshold) {
                log.warn("Saga '{}' has lost its compare-and-set save {} times (of a maximum {}) to concurrent writers; each retry re-dispatches the input's commands. Sustained contention may exhaust the retries and raise SagaConcurrencyException.",
                        sagaId, attempt + 1, config.maxCasAttempts());
            }
        }
        throw new SagaConcurrencyException("Failed to save saga '" + sagaId + "' after " + config.maxCasAttempts()
                + " attempts due to concurrent modification");
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
