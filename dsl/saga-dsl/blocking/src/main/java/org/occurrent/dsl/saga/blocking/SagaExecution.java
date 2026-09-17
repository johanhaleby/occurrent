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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, dispatches commands before saving (at-least-once), and retries a lost compare-and-set
 * save. Timeouts re-enter the same path, fenced so a timer no longer present on the (reloaded) envelope is skipped.
 * <p>
 * An instance that keeps failing is quarantined rather than retried forever. Its first failure records when the
 * failing started and rethrows, which is what every version up to 0.33.0 did. Once the instance has been failing for
 * at least {@link SagaRunnerConfig#quarantineAfter()}, it is marked
 * {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} on whichever event it is failing on then and this class returns
 * normally, so the subscription acknowledges that event and the saga's other instances stop waiting behind it.
 * <p>
 * Every step from reading the CloudEvent to saving the result runs inside one {@code try} that catches
 * {@link Throwable}, so once an event has reached an instance, what failed and where it was thrown decide nothing
 * about its budget. A delivery that fails before the saga can say which instance it
 * belongs to has no instance to charge and no instance to hold it, so it is never let past. It is refused on every
 * redelivery and the saga waits behind it. See {@link #letTheSubscriptionPast} for the conditions and
 * {@link #refuseUnroutableDelivery} for why the unroutable delivery is the exception.
 * <p>
 * The budget is the instance's rather than one event's. An instance where two events both fail keeps the instant it
 * started failing, so a second event can reach the budget on its first failure, and the record names the event the
 * instance stopped on rather than the one the clock started with.
 * <p>
 * The quarantine decision reads and writes the instance without its application state, through
 * {@link SagaStateStore#findWithoutState} and {@link SagaStateStore#compareAndSaveWithoutState}. An instance whose state
 * no longer decodes is one of the instances most in need of quarantining, so deciding on a read that throws on it would
 * leave it blocking the saga forever. A store that only reads an instance whole keeps that behaviour, because nothing
 * here can read an instance it can only hand over whole.
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
    // Answers whether acknowledging the failing event would destroy the last copy of it. Only a model that has already
    // promised to hold everything it delivers gets this far, so this is a check on that promise rather than the gate,
    // made on the one event about to be acknowledged.
    private final Predicate<CloudEvent> stillObtainable;
    // The instances that have been told about a refused quarantine. A refused instance keeps being re-offered the event
    // for as long as the source retries, so without this the refusal is logged at that cadence forever. Once per
    // instance rather than once per input, because an instance where two inputs fail in turn alternates between them and
    // keying on the input made every delivery look like the first one. Cleared as soon as the instance processes
    // anything, so a recovery is announced again if it stops a second time.
    private final Set<String> refusalAnnounced = ConcurrentHashMap.newKeySet();
    // The deliveries the saga could not work out an instance for, keyed by redelivery key, or by CloudEvent id and source
    // for an event carrying none, holding when the routing
    // started failing and when that was last logged. Only the logging reads it, so a delivery the source re-offers every
    // few milliseconds is logged once per interval rather than at that cadence. Dropped as soon as the event routes.
    private final ConcurrentHashMap<String, UnroutableDelivery> unroutableDeliveries = new ConcurrentHashMap<>();
    // The extension names already reported as unreadable, so the warning is said once per name rather than per event.
    private final Set<String> unreadableExtensionsWarned = ConcurrentHashMap.newKeySet();
    // Where this class reads the current instant from, wherever it needs one, rather than calling Instant.now()
    // directly. Real wall-clock time in production. A test supplies its own so it can cross an interval or a
    // quarantine budget without sleeping out real minutes.
    private final Supplier<Instant> clock;

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable) {
        this(subscriptionId, saga, stateStore, dispatcher, converter, config, stillObtainable, Instant::now);
    }

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable,
                  Supplier<Instant> clock) {
        this.stillObtainable = stillObtainable;
        this.subscriptionId = subscriptionId;
        this.saga = saga;
        this.stateStore = stateStore;
        this.dispatcher = dispatcher;
        this.converter = converter;
        this.config = config;
        this.clock = clock;
        this.casRetry = RetryStrategy.retry()
                .backoff(Backoff.none())
                .maxAttempts(config.maxCasAttempts())
                .retryIf(CasConflict.class::isInstance);
        this.warnThreshold = config.maxCasAttempts() / 2 + 1;
    }

    void onCloudEvent(CloudEvent cloudEvent) {
        // The try opens on the first statement of the delivery and catches Throwable, so no step can fail outside it and
        // no kind of failure goes unseen. What a failure costs is then decided in one place, by the conditions in
        // letTheSubscriptionPast, rather than by where it was thrown or what type it was, bar the one exclusion those
        // conditions name. A saga whose id extractor
        // throws on one event is the case that wrote this. It used to escape a try that opened after it, so nothing was
        // recorded, nothing was quarantined, and every other instance of the saga waited behind the redelivery forever.
        EventMeta meta = EventMeta.NONE;
        String sagaId = null;
        try {
            // NONE until this returns, so an event whose extensions cannot be read is an event carrying no redelivery
            // key, which is what the conditions below already say about an event nothing can tell a redelivery of.
            meta = extractMeta(cloudEvent);
            E event = converter.toDomainEvent(cloudEvent);
            sagaId = saga.sagaId(event);
            // Routing worked, whatever the id turned out to be. Cleared here rather than after the delivery, because an
            // event that correlates to no instance returns below and a quarantining one returns normally.
            unroutableDeliveries.remove(unroutableKey(meta, cloudEvent));
            if (sagaId == null) {
                return;
            }
            refuseOrWarnIfRedeliveryCannotBeDetected(meta);
            // The full delivery metadata (stream id and version, position, and any CloudEvent extensions) rides on the
            // input so reactions can read it. The separate EventMeta drives redelivery dedup and is derived
            // independently above, so its null-tolerant watermark behaviour is unchanged.
            process(sagaId, SagaInput.event(event, EventMetadata.from(cloudEvent)), meta, null);
            refusalAnnounced.remove(sagaId);
        } catch (Throwable failure) {
            if (!letTheSubscriptionPast(sagaId, cloudEvent, meta, failure)) {
                throw failure;
            }
        }
    }

    /**
     * Whether the subscription may move past this delivery rather than be made to offer it again. A true answer means
     * {@link #onCloudEvent} returns normally, so the subscription acknowledges the event and every other instance on the
     * shared channel keeps going. A false answer means the failure propagates exactly as it always has.
     * <p>
     * A delivery the saga could not route, because the converter or the id extractor threw, always gets a false answer,
     * and {@link #refuseUnroutableDelivery} says why. Only a delivery that reached an instance can be let past, and
     * only by quarantining that instance.
     * <p>
     * Four conditions decide that, and they are the same four whichever step after routing threw. They are
     * also the same four whatever the failure was, with the one exclusion
     * {@link SagaExecutionSupport#isAttributableToTheInstance} names, which is a failure of the JVM rather than of this
     * instance's work.
     * <p>
     * {@link SagaRunnerConfig#quarantineAfter()} has to be set, and {@code SagaRunner} switches it off at startup for a
     * model that cannot guarantee it holds every event it delivers, so a budget that is set also means the model made
     * that guarantee. The failing delivery has to have been failing for at least that budget. The event has to
     * carry a redelivery key, meaning a stream id with its version or a global position, since without one nothing tells
     * one delivery of it from the next and the budget could never elapse. And the model has to confirm, for that one
     * event, that acknowledging it is not what would destroy the last copy of it.
     */
    private boolean letTheSubscriptionPast(@Nullable String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure) {
        if (sagaId == null) {
            refuseUnroutableDelivery(meta, cloudEvent, failure);
            return false;
        }
        Duration quarantineAfter = config.quarantineAfter();
        if (quarantineAfter == null || !meta.carriesRedeliveryKey() || !SagaExecutionSupport.isAttributableToTheInstance(failure)) {
            return false;
        }
        return quarantine(sagaId, cloudEvent, meta, failure, quarantineAfter);
    }

    // The redelivery key where the event has one, and otherwise its CloudEvent id and source, which also stay the same
    // from one delivery of the event to the next.
    private static String unroutableKey(EventMeta meta, CloudEvent cloudEvent) {
        String redeliveryKey = meta.redeliveryKey();
        return redeliveryKey != null ? redeliveryKey : cloudEvent.getId() + " from " + cloudEvent.getSource();
    }

    /**
     * Say that this delivery failed before the saga could work out which instance it belongs to, and that it is
     * refused. It is refused on every redelivery, however long it has been failing, and the pacing below changes
     * nothing about that.
     * <p>
     * Acknowledging it is what would lose it. An event the saga cannot route may still belong to an instance, and the
     * next event for that instance moves the instance's watermark past it, so feeding the repaired event to the saga
     * again afterwards is taken for a redelivery and ignored. Nothing can be written for it either, since there is no
     * instance to write on. Where the subscription offers it again, the saga waits behind it, which blocks this saga
     * and no other, and once the converter or the id extractor can read it the event is applied in the order it was
     * written, with nothing to feed again.
     * <p>
     * The first failure is a warning, and after that it is logged at ERROR once per {@link #unroutableErrorInterval()}
     * for as long as the event keeps being offered, rather than every time the source offers it. That interval is the
     * quarantine budget when one is configured, and a fixed five-minute default otherwise, so an operator alerting on
     * ERROR is paged on a subscription model this saga cannot quarantine anything on, for as long as that model keeps
     * offering the event. A broker bridge that parks the delivery instead of redelivering it gets only the first WARN.
     * See {@link #unroutableErrorInterval()} for why. An event carrying no redelivery key is named by its CloudEvent
     * id and source instead.
     */
    private void refuseUnroutableDelivery(EventMeta meta, CloudEvent cloudEvent, Throwable failure) {
        if (!SagaExecutionSupport.isAttributableToTheInstance(failure)) {
            return;
        }
        String redeliveryKey = unroutableKey(meta, cloudEvent);
        Instant now = clock.get();
        UnroutableDelivery existing = unroutableDeliveries.putIfAbsent(redeliveryKey, new UnroutableDelivery(now, now));
        if (existing == null) {
            log.warn("Saga '{}' could not work out which instance the event '{}' belongs to, so it refused the event rather than acknowledging it, because an event the saga cannot route may still belong to an instance and acknowledging it would lose it. Where the subscription offers the event again, every instance of this saga waits behind it until the converter or the id extractor can read it, and the event is then applied in the order it was written. A broker bridge that parks refused deliveries puts it in its parking destination instead.",
                    subscriptionId, redeliveryKey, failure);
            return;
        }
        Duration interval = unroutableErrorInterval();
        if (Duration.between(existing.lastLoggedAt(), now).compareTo(interval) < 0
            || !unroutableDeliveries.replace(redeliveryKey, existing, new UnroutableDelivery(existing.firstFailedAt(), now))) {
            return;
        }
        log.error("Saga '{}' has been unable to work out which instance the event '{}' belongs to for {}, and every instance of this saga is still waiting behind it. The event is refused rather than skipped, because acknowledging it would lose it, and no instance is quarantined, because the event reached none. Repair the converter or the id extractor and the event is applied in the order it was written, with nothing to feed to the saga again.",
                subscriptionId, redeliveryKey, Duration.between(existing.firstFailedAt(), now), failure);
    }

    /**
     * How often {@link #refuseUnroutableDelivery} repeats its ERROR once the first failure has already been warned
     * about. An event the saga cannot route is refused whether or not {@link SagaRunnerConfig#quarantineAfter()} is
     * set, because nothing here ever quarantines an instance for it (see {@link #refuseUnroutableDelivery}), so pacing
     * this ERROR on that budget alone would leave a saga on a subscription model {@code SagaRunner} switches quarantine
     * off for, such as a push feed or a broker bridge, warning once and then staying silent for good even while that
     * model keeps offering the event. A broker bridge that parks the delivery instead of redelivering it gets only the
     * first WARN either way. Using the configured budget when there is one keeps one number for an operator to reason
     * about, and {@link SagaRunnerConfig#DEFAULT_QUARANTINE_AFTER} otherwise gives every other model the same
     * five-minute cadence rather than inventing a second tunable for it.
     */
    Duration unroutableErrorInterval() {
        Duration quarantineAfter = config.quarantineAfter();
        return quarantineAfter != null ? quarantineAfter : SagaRunnerConfig.DEFAULT_QUARANTINE_AFTER;
    }

    /**
     * Record that this event failed for this instance, and answer whether that ended the instance's time budget. A true
     * answer means the caller returns normally, so the subscription acknowledges the event and every other instance on
     * the shared channel keeps going. A false answer means the exception propagates exactly as it always has.
     * <p>
     * The failure is a {@link Throwable} because the instance is stuck on it either way. An {@code Error} out of a
     * reaction stops the instance making progress exactly as a {@code RuntimeException} does, and an instance stopped by
     * one blocks the saga's other instances for exactly as long.
     * <p>
     * Only the event path calls this, and it is not because the timer path has nothing to gain from it. A timeout
     * has no redelivery key of its own, so there is nothing to quarantine it on.
     * <p>
     * What the timer path does instead is retry for ever, and that is not free for the saga's other instances. A poll
     * fires at most {@link SagaRunnerConfig#timerBatchLimit()} instances, and nothing in
     * {@link SagaStateStore#findWithDueTimers} requires a store to give a different instance a turn, so once that many
     * instances cannot fire their timers the saga can stop firing timers altogether. That many is enough, since a
     * batch full of them leaves no place for anything else.
     * <a href="https://github.com/johanhaleby/occurrent/issues/1003">#1003</a> is where that missing guarantee is
     * being added.
     * <p>
     * So a failing timeout does not block the poller, and it is not isolated from the saga's other instances either.
     * Earlier versions of this javadoc said it was, on an accounting that was true of one instance and false of the
     * batch they share.
     */
    private boolean quarantine(String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Duration quarantineAfter) {
        try {
            Instant now = clock.get();
            // Read without the state, because nothing on this path applies it and an instance whose state no longer
            // decodes is the one that most needs to reach its budget. Loading it whole threw, the catch below swallowed
            // that, and no failure record was ever written.
            SagaEnvelope<S> current = stateStore.findWithoutState(sagaId).orElse(null);
            FailureRecord<S> record = SagaExecutionSupport.onFailure(saga, sagaId, current, meta, failure, now, quarantineAfter);
            if (record == null) {
                return false;
            }
            if (record.quarantined() && !confirmedStillObtainable(cloudEvent, failure)) {
                boolean firstRefusalForThisInstance = refusalAnnounced.add(sagaId);
                // The measured time rather than the budget. A redelivery can arrive well after the budget elapsed, so
                // reporting the budget as the elapsed time understates how long the instance has been stuck.
                Duration failingFor = failingFor(record, now);
                // Checked before the write, not after, because quarantining returns normally and that acknowledges the
                // event to whatever fed it. An unconfirmed answer is treated as a no, so the instance keeps blocking
                // and the exception propagates as it did before 0.34.0. Nothing is saved, which leaves the failure
                // record the earlier attempts wrote and lets the next redelivery ask again. Retention is rechecked
                // every time so a store coming back is noticed, while the warning is said once per instance.
                if (firstRefusalForThisInstance) {
                    log.warn("Saga '{}' instance '{}' has been failing for {}, which is past its budget of {}, and is stopped on the event '{}', and it is not quarantined, because the subscription could not confirm that acknowledging the event is safe to do. Either acknowledging is what would drop the only copy of it, or the check could not be completed, and quarantining acknowledges the event. This instance keeps blocking the saga's other instances instead. https://github.com/johanhaleby/occurrent/issues/918 is the path to closing that.",
                            subscriptionId, sagaId, failingFor, quarantineAfter, meta.redeliveryKey(), failure);
                }
                return false;
            }
            // A refusal that later turns into a quarantine never reaches the clearing in onCloudEvent, because the
            // quarantine happens on this path and the instance takes no further event afterwards.
            refusalAnnounced.remove(sagaId);
            // Written without the state for the same reason it was read without it. The envelope carries whatever the
            // read gave, so saving it whole would erase the state somebody needs once the converter is repaired.
            if (!stateStore.compareAndSaveWithoutState(sagaId, record.envelope(), record.expectedVersion())) {
                // Another input advanced the instance while the failing one was being retried, most likely a timer that
                // fired successfully. The failing event now meets different state and may well succeed, so discard this
                // write rather than retry one whose premise has gone. Only a first failure loses its budget that way.
                // A later one keeps the record the winning write left in place, since only the failing input clears it.
                return false;
            }
            if (!record.quarantined()) {
                log.warn("Saga '{}' instance '{}' failed on the event '{}' and is being retried by the subscription. It is quarantined once it has been failing for {}, measured from this first failure rather than from any one event.",
                        subscriptionId, sagaId, meta.redeliveryKey(), quarantineAfter, failure);
                return false;
            }
            log.error("Saga '{}' instance '{}' has been failing for {}, past its budget of {}, and is now QUARANTINED, stopped on the event '{}'. The time is how long the instance has been failing, which can be longer than this one event has. It skips every further event and fires no timers, so the saga's other instances are no longer blocked behind it. Find it with findByStatus(QUARANTINED, ..).",
                    subscriptionId, sagaId, failingFor(record, now), quarantineAfter, meta.redeliveryKey(), failure);
            return true;
        } catch (Throwable storeFailure) {
            // What reaches here is the store being unreachable, or a store that can only read an instance whole
            // failing to decode one. Rethrowing the original is today's behaviour and it is the right one for both,
            // because a saga that cannot read its own instance cannot make progress on it either way. The retention
            // check no longer reaches here, because a check that cannot answer is a refused quarantine with a warning
            // rather than a silent one.
            rethrowIfNotTheInstances(storeFailure);
            if (storeFailure != failure) {
                // Java refuses to suppress an exception under itself, and a store that rethrows the very object that
                // reached us here would otherwise replace the real failure with IllegalArgumentException.
                failure.addSuppressed(storeFailure);
            }
            return false;
        }
    }

    /**
     * Let a failure of the JVM out of a recovery step instead of absorbing it, because every catch below this class's
     * main one swallows what it caught and rethrows the original delivery failure instead.
     * <p>
     * {@link SagaExecutionSupport#isAttributableToTheInstance} keeps an {@link OutOfMemoryError} from quarantining the
     * instance that happened to be running, and a store read or a retention check raising one is the same condition
     * arriving one level down. Absorbing it there would report the saga's own exception to the subscription while the
     * process is out of heap, and would let the instance's budget keep running on that basis.
     */
    private static void rethrowIfNotTheInstances(Throwable recoveryFailure) {
        if (SagaExecutionSupport.isAttributableToTheInstance(recoveryFailure)) {
            return;
        }
        // Both shapes rather than a cast to Error, so this keeps working if the predicate ever excludes something that
        // is not one.
        if (recoveryFailure instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) recoveryFailure;
    }

    // How long the instance has actually been failing, taken from the record rather than from the budget, because a
    // redelivery can arrive well after the budget elapsed and the two then differ by however late it was.
    private static <S extends @Nullable Object> Duration failingFor(FailureRecord<S> record, Instant now) {
        SagaFailure failure = record.envelope().failure();
        return failure == null ? Duration.ZERO : Duration.between(failure.firstFailedAt(), now);
    }

    // A retention check that throws has not said yes, and the design treats an answer it did not get as a no. Answered
    // here rather than left to the catch above, so a check that could not run gets the same warning a no gets instead
    // of disappearing into the silent return every store failure produces.
    private boolean confirmedStillObtainable(CloudEvent cloudEvent, Throwable failure) {
        try {
            return stillObtainable.test(cloudEvent);
        } catch (Throwable checkFailure) {
            rethrowIfNotTheInstances(checkFailure);
            if (checkFailure != failure) {
                // Guarded like the catch above, because a check that rethrows the very object that reached us here would
                // otherwise have addSuppressed throw IllegalArgumentException and skip the refusal warning below.
                failure.addSuppressed(checkFailure);
            }
            return false;
        }
    }

    void pollTimers() {
        // Catch Throwable so a failure never lets the scheduled task die and stop all future polling. The schedule stays
        // alive and the next tick recovers.
        try {
            Instant now = clock.get();
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
                    } catch (Throwable t) {
                        // Throwable rather than RuntimeException, because a StackOverflowError out of a recursive
                        // evolve or a NoClassDefFoundError out of a reaction used to unwind the loop into the catch
                        // below, so every instance after this one in the batch lost its turn. An
                        // OutOfMemoryError still does, because it is the JVM's condition and not this instance's.
                        rethrowIfNotTheInstances(t);
                        // Keep polling other timers/instances. This one stays due, so a later poll retries it unless
                        // it is consumed first. Which poll is not promised, since nothing requires a store to give a
                        // different instance a turn, see #1003.
                        log.warn("Failed to fire saga timer '{}' for instance '{}'", timerName, envelope.sagaId(), t);
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
                    Instant now = clock.get();
                    SagaEnvelope<S> current;
                    try {
                        current = stateStore.find(sagaId).orElse(null);
                    } catch (Throwable loadFailure) {
                        // Asked before the skip, because a skip returns normally and that acknowledges the event. A
                        // load that failed because the process is out of heap says nothing about whether this input
                        // would have been skipped, and answering yes on that basis would acknowledge it silently.
                        rethrowIfNotTheInstances(loadFailure);
                        if (wouldHaveSkippedThisInput(sagaId, meta, loadFailure)) {
                            return null;
                        }
                        throw loadFailure;
                    }
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

    // Whether this input would have been skipped even with the state loaded, asked only when loading it failed. A
    // completed instance and a quarantined one skip every input addressed to them, and any instance skips an input its
    // watermarks say it has already handled, so a state that no longer decodes must not be what decides whether the
    // subscription may move past one of those. The redelivery answer reuses SagaExecutionSupport's own rule rather than
    // restating it, and a timer carries EventMeta.NONE, which that rule answers no for.
    // Asked through findWithoutState, so a store that can only read an instance whole says no and the load failure
    // propagates as it did before.
    private boolean wouldHaveSkippedThisInput(String sagaId, EventMeta meta, Throwable loadFailure) {
        try {
            SagaEnvelope<S> withoutState = stateStore.findWithoutState(sagaId).orElse(null);
            if (withoutState == null) {
                return false;
            }
            return withoutState.isCompleted() || withoutState.isQuarantined()
                   || SagaExecutionSupport.isRedelivery(withoutState, meta);
        } catch (Throwable secondFailure) {
            rethrowIfNotTheInstances(secondFailure);
            if (secondFailure != loadFailure) {
                loadFailure.addSuppressed(secondFailure);
            }
            return false;
        }
    }

    // When one delivery started failing to reach an instance, and when that was last logged.
    private record UnroutableDelivery(Instant firstFailedAt, Instant lastLoggedAt) {
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
    // one. Under REQUIRED that is refused rather than reacted to, so the throw reaches the subscription model and the
    // feed offers the event again until somebody looks. A broker bridge that parks the delivery instead of redelivering
    // it moves the event to its parking destination and does not offer it again. Under BEST_EFFORT the duplication is
    // accepted knowingly, so the warning says so once per runner rather than once per event.
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

    // Each extension is read on its own, because one that cannot be read must not take the others with it. An event
    // carrying a good streamid and streamversion beside a position that is not a number has a redelivery key, and
    // reading the three together threw before either of the first two reached the record, so the event looked like one
    // carrying no key at all and blocked the channel with nothing able to budget it.
    private EventMeta extractMeta(CloudEvent cloudEvent) {
        Set<String> extensions = cloudEvent.getExtensionNames();
        String streamId = readExtension(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_ID, OccurrentExtensionGetter::getStreamId);
        Long streamVersion = readExtension(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_VERSION, OccurrentExtensionGetter::getStreamVersion);
        // Use the framework's own position accessor, which accepts a Number or String, rather than narrowing to Long.
        Long position = readExtension(cloudEvent, extensions, OccurrentCloudEventExtension.POSITION, OccurrentCloudEventExtension::getPosition);
        return new EventMeta(streamId, streamVersion, position);
    }

    // An extension that is absent and one that cannot be read both answer null, and the difference is said once per
    // runner rather than per event, because a feed writing one badly writes every one of them badly.
    // RuntimeException rather than Throwable on purpose. This is the one catch on the delivery path that is meant to
    // absorb what it caught, since an unreadable extension is a fact about the event rather than a failure to deliver
    // it, and keeping it narrow is what lets a failure of the process reach the delivery's own catch.
    private <T> @Nullable T readExtension(CloudEvent cloudEvent, Set<String> extensions, String name, Function<CloudEvent, @Nullable T> read) {
        if (!extensions.contains(name)) {
            return null;
        }
        try {
            return read.apply(cloudEvent);
        } catch (RuntimeException e) {
            if (unreadableExtensionsWarned.add(name)) {
                log.warn("Saga subscription '{}' received an event whose '{}' extension could not be read, so it is treated as absent. A redelivery is still recognised from whichever of streamid with streamversion, or position, this event does carry, and the event is refused or warned about as one carrying nothing if it carries neither.",
                        subscriptionId, name, e);
            }
            return null;
        }
    }
}
