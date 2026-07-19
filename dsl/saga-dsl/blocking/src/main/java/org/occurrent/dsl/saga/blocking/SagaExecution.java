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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaTimeout;
import org.occurrent.dsl.saga.executor.SagaExecutionSupport;
import org.occurrent.dsl.saga.executor.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.executor.SagaExecutionSupport.Outcome;

import java.lang.System.Logger.Level;
import java.time.Instant;
import java.util.List;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, dispatches commands before saving (at-least-once), and retries a lost compare-and-set
 * save. Timeouts re-enter the same path, fenced so a timer no longer present on the (reloaded) envelope is skipped.
 */
final class SagaExecution<E, S extends @Nullable Object, C> {
    private static final System.Logger LOG = System.getLogger(SagaExecution.class.getName());

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
        String sagaId = saga.correlationId(event);
        if (sagaId == null) {
            return;
        }
        process(sagaId, SagaInput.event(event), extractMeta(cloudEvent), null);
    }

    void pollTimers() {
        Instant now = Instant.now();
        long nowMillis = now.toEpochMilli();
        List<SagaEnvelope<S>> due;
        try {
            due = stateStore.findWithDueTimers(now, config.timerBatchLimit());
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Failed to query saga timers", e);
            return;
        }
        for (SagaEnvelope<S> envelope : due) {
            List<String> dueTimerNames = envelope.timers().stream()
                    .filter(timer -> timer.firesAtEpochMilli() <= nowMillis)
                    .map(TimerEntry::name)
                    .toList();
            for (String timerName : dueTimerNames) {
                try {
                    process(envelope.sagaId(), SagaInput.timeout(new SagaTimeout(envelope.sagaId(), timerName)), EventMeta.NONE, timerName);
                } catch (RuntimeException e) {
                    // Keep polling other timers/instances; this one stays due and is retried next poll unless consumed.
                    LOG.log(Level.WARNING, "Failed to fire saga timer '" + timerName + "' for instance '" + envelope.sagaId() + "'", e);
                }
            }
        }
    }

    private void process(String sagaId, SagaInput<E> input, EventMeta meta, @Nullable String requireTimerName) {
        for (int attempt = 0; attempt < config.maxCasAttempts(); attempt++) {
            SagaEnvelope<S> current = stateStore.find(sagaId).orElse(null);
            if (requireTimerName != null && !hasPendingTimer(current, requireTimerName)) {
                return; // stale/superseded timer, or the instance completed: nothing to fire.
            }
            Outcome<S, C> outcome = SagaExecutionSupport.process(saga, sagaId, current, input, meta, Instant.now());
            if (!outcome.processed()) {
                return;
            }
            // Dispatch before saving so a command is never lost; a lost compare-and-set retry may re-dispatch (at-least-once).
            for (C command : outcome.commands()) {
                dispatcher.dispatch(command);
            }
            SagaEnvelope<S> envelope = outcome.envelope();
            if (envelope != null && stateStore.compareAndSave(sagaId, envelope, outcome.expectedVersion())) {
                return;
            }
        }
        throw new SagaConcurrencyException("Failed to save saga '" + sagaId + "' after " + config.maxCasAttempts()
                + " attempts due to concurrent modification");
    }

    private boolean hasPendingTimer(@Nullable SagaEnvelope<S> envelope, String timerName) {
        return envelope != null && !envelope.isCompleted()
                && envelope.timers().stream().anyMatch(timer -> timer.name().equals(timerName));
    }

    private EventMeta extractMeta(CloudEvent cloudEvent) {
        String streamId = cloudEvent.getExtension(OccurrentCloudEventExtension.STREAM_ID) instanceof String s ? s : null;
        Long streamVersion = cloudEvent.getExtension(OccurrentCloudEventExtension.STREAM_VERSION) instanceof Long v ? v : null;
        Long position = cloudEvent.getExtension(OccurrentCloudEventExtension.POSITION) instanceof Long p ? p : null;
        return new EventMeta(streamId, streamVersion, position);
    }
}
