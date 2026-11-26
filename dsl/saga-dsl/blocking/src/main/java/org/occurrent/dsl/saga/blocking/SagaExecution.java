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
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey.RecordKey;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.SkipToRecord;
import org.occurrent.dsl.saga.internal.SkippedInputKey;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, asks the store whether it can take the instance the step produces, dispatches
 * commands before saving (at-least-once), and retries a lost compare-and-set save. Timeouts re-enter the same path,
 * fenced so a timer no longer present on the (reloaded) envelope is skipped.
 * <p>
 * An instance that keeps failing can be quarantined rather than left to fail for as long as the subscription model
 * offers the event again. Where a quarantine budget is in force, its first failing event can write down when the
 * failing started, and it rethrows whether or not that write succeeds, which is what every version up to 0.33.0 did. Where nothing was recorded, the next delivery
 * decides on whatever the store holds then. Whether a later failure is considered for quarantine, and whether a
 * considered one is then quarantined, are separate conditions, and
 * {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} lists both. When the instance is quarantined, this class
 * returns normally instead of rethrowing.
 * <p>
 * A quarantined instance records the inputs it skips with their events, and the write that quarantines it records the
 * input it was quarantined on the same way. {@link #release} replays those events in the order they were recorded, from
 * the records themselves, as ADR 178 describes.
 * <p>
 * Every step from reading the CloudEvent to saving the result runs inside one {@code try} that catches
 * {@link Throwable}, so once an event has reached an instance, what failed and where it was thrown decide nothing
 * about its budget. A delivery that fails before the saga can say which instance it
 * belongs to has no instance to charge and no instance to hold it, so it is never let past. It is refused on every
 * redelivery. See
 * {@link #letTheSubscriptionPast} for the conditions and
 * {@link #refuseUnroutableDelivery} for why the unroutable delivery is the exception.
 * <p>
 * Each failing event keeps its own clock. An instance where two events fail in turn measures each from its own first
 * failure, and the record names the event the instance stopped on, with the instant that event first failed.
 * <p>
 * The quarantine decision reads and writes the instance without its application state, through
 * {@link SagaStateStore#findWithoutState} and {@link SagaStateStore#compareAndSaveWithoutState}. An instance whose state
 * no longer decodes is one of the instances most in need of quarantining, so deciding on a read that throws on it would
 * leave it failing on every delivery without ever reaching its budget.
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
    static final Duration UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE = Duration.ofMinutes(5);
    static final int UNROUTABLE_FORGOTTEN_AFTER_INTERVALS = 2;
    // The two ERRORs paced per instance by unwritableErrorDue, the failure record and the pending listing
    private static final String UNWRITABLE_FAILURE = "failure";
    private static final String UNWRITABLE_PENDING = "pending";

    private final String subscriptionId;
    private final Saga<E, S, C> saga;
    private final SagaStateStore<S> stateStore;
    // Every read of the store decodes and every write encodes through this
    private final SagaEnvelopes<S> envelopes;
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
    // The deliveries the saga could not work out an instance for, keyed by redelivery key, or by CloudEvent id and source
    // for an event carrying none. The logging reads it, so a delivery the source re-offers every few milliseconds is
    // logged once per interval rather than at that cadence. Dropped when the event routes. One not offered for
    // UNROUTABLE_FORGOTTEN_AFTER_INTERVALS intervals is dropped by a later refusal instead, since one repaired on another
    // node never routes here. So what stays after the last refusal is what was refused in the three intervals before it.
    private final ConcurrentHashMap<String, UnroutableDelivery> unroutableDeliveries = new ConcurrentHashMap<>();
    // When a refusal last dropped the forgotten deliveries, so the map is scanned at most once per interval. A source
    // that parks every event it cannot deliver gives a broken converter one entry per event, and scanning all of them
    // on every refusal costs the square of that.
    private final AtomicReference<@Nullable Instant> unroutableSweptAt = new AtomicReference<>();
    // When the ERROR that the store cannot write the failure of an input, or its pending listing, was last logged, keyed
    // by which of the two and the saga id. The state behind it does not change on a redelivery, so a source offering the
    // event every few milliseconds is told once per interval rather than at that cadence. An entry a whole interval old
    // is due again anyway, so dropping it changes nothing, and the map is swept of those at most once per interval.
    private final ConcurrentHashMap<List<String>, Instant> unwritableErrorLoggedAt = new ConcurrentHashMap<>();
    private final AtomicReference<@Nullable Instant> unwritableErrorSweptAt = new AtomicReference<>();
    // The extension names already reported as unreadable, so the warning is said once per name rather than per event.
    private final Set<String> unreadableExtensionsWarned = ConcurrentHashMap.newKeySet();
    // The streams already warned about, so a stream replaying thousands of old events says it once. Capped, because a
    // runner sees as many streams as the application has and this must not grow with them. A stream evicted from here
    // is warned about again, which costs a line.
    private final Set<List<String>> behindWindowWarned = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<List<String>, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<List<String>, Boolean> eldest) {
            return size() > 1024;
        }
    }));
    // Where this class reads the current instant from, wherever it needs one, rather than calling Instant.now()
    // directly. Real wall-clock time in production. A test supplies its own so it can cross an interval or a
    // quarantine budget without sleeping out real minutes.
    private final Supplier<Instant> clock;
    // The state store's skip records, or null when it does not implement SagaStateStoreSkippedInputs. The runner refuses
    // quarantine on without it, so with null the poller has nothing to continue and release refuses
    private final @Nullable SagaStateStoreSkippedInputs skippedInputs;
    // True when the store has skip records but cannot write a skip and its record in one atomic write, as
    // SpringMongoSagaStateStore on a standalone server. The runner refuses quarantine on there, yet an instance quarantined
    // earlier can still be in its data, such as data restored from a replica set. An input such an instance has to record
    // as skipped is refused, since skipping it unrecorded would acknowledge an input no release replays
    private final boolean cannotRecordSkips;
    // The last id the previous release poll listed, null when that poll reached the end
    private final AtomicReference<@Nullable String> releasingCursor = new AtomicReference<>();
    // True once the poller has said that a store with cannotRecordSkips holds releasing instances it does not go on with
    private final AtomicBoolean releasingStaySaid = new AtomicBoolean();

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config) {
        this(subscriptionId, saga, stateStore, dispatcher, converter, config, Instant::now);
    }

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Supplier<Instant> clock) {
        this.subscriptionId = subscriptionId;
        this.saga = saga;
        this.stateStore = stateStore;
        // A FlowState that is not the executor's own can't be stored, and a delivery dispatches its commands before the
        // save, so it is refused here, before the saga subscribes
        Object initialState = saga.initialState();
        if (initialState instanceof FlowState<?> && !(initialState instanceof FlowStateImpl<?>)) {
            throw new IllegalArgumentException("Saga '%s' starts from a %s, but a saga whose state is a FlowState can only be run with the flow executor's own FlowState (FlowStateImpl). Build it with the flow DSL."
                    .formatted(subscriptionId, initialState.getClass().getName()));
        }
        // A flow saga's received events are written through the runner's converter. Any other saga has none to write
        this.envelopes = SagaEnvelopes.of(stateStore, initialState instanceof FlowState<?> ? converter : null);
        this.skippedInputs = stateStore instanceof SagaStateStoreSkippedInputs records ? records : null;
        this.cannotRecordSkips = skippedInputs != null && !writesAtomically(skippedInputs);
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
        // recorded, nothing was quarantined, and nothing bounded how long the event kept failing.
        EventMeta meta = EventMeta.NONE;
        String sagaId = null;
        try {
            // NONE until this returns, so a failure to read the extensions at all is an event carrying no redelivery
            // key, which is what the conditions below already say about an event nothing can tell a redelivery of.
            ExtractedMeta extracted = extractMeta(cloudEvent);
            meta = extracted.meta();
            E event = converter.toDomainEvent(cloudEvent);
            sagaId = saga.sagaId(event);
            // Routing worked, whatever the id turned out to be. Cleared here rather than after the delivery, because an
            // event that correlates to no instance returns below and a quarantining one returns normally.
            unroutableDeliveries.remove(unroutableKey(meta, cloudEvent));
            if (sagaId == null) {
                return;
            }
            refuseIfStreamKeyIsUnreadable(extracted);
            refuseOrWarnIfRedeliveryCannotBeDetected(meta);
            // The full delivery metadata (stream id and version, position, and any CloudEvent extensions) rides on the
            // input so reactions can read it. The separate EventMeta drives redelivery dedup and is derived
            // independently above, so its null-tolerant watermark behaviour is unchanged.
            process(sagaId, SagaInput.event(event, EventMetadata.from(cloudEvent)), meta, cloudEvent, null);
        } catch (Throwable failure) {
            if (!letTheSubscriptionPast(sagaId, cloudEvent, meta, failure)) {
                throw failure;
            }
        }
    }

    /**
     * Whether the subscription may move past this delivery rather than be made to offer it again. A true answer means
     * {@link #onCloudEvent} returns normally. A false answer means the failure propagates exactly as it always has.
     * <p>
     * A delivery the saga could not route, because the converter or the id extractor threw, always gets a false answer,
     * and {@link #refuseUnroutableDelivery} says why. Only a delivery that reached an instance can be let past, and
     * only by quarantining that instance.
     * <p>
     * The conditions {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} lists decide that, and they are the same
     * whichever step after routing threw. This method checks the ones that need no store read, and {@link #quarantine}
     * checks the rest.
     */
    private boolean letTheSubscriptionPast(@Nullable String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure) {
        if (sagaId == null) {
            refuseUnroutableDelivery(meta, cloudEvent, failure);
            return false;
        }
        if (!meta.carriesRedeliveryKey() || !SagaExecutionSupport.isAttributableToTheInstance(failure)) {
            return false;
        }
        Optional<Duration> quarantineAfter = config.quarantineAfter();
        if (quarantineAfter.isEmpty()) {
            // Nothing is quarantined, but the version did not apply, and the source may park it and move on. It is
            // listed on the instance before the exception reaches the source, so a replay long after is still applied.
            recordPending(sagaId, meta, failure);
            return false;
        }
        return quarantine(sagaId, cloudEvent, meta, failure, quarantineAfter.get());
    }

    /**
     * List the stream version of the failed delivery as pending on the instance, without a failure record, so quarantine
     * never counts it. Written without the state like the failure record is, and never throws what the store throws,
     * because the exception that matters is the one that made the delivery fail.
     * <p>
     * An input that is pending already costs one read and no write, so a source that offers a failing event again every
     * few milliseconds reads the instance each time, as the quarantine path does. A lost compare-and-set is read again and retried up to {@link SagaRunnerConfig#maxCasAttempts()} times. A write
     * that cannot be made leaves the version unrecorded, and the log says so at ERROR with the key to replay from. When
     * the store cannot write the instance with the version listed, which no redelivery changes, that ERROR is logged at
     * most once per {@link #unroutableErrorInterval()} for the instance. Such a
     * version is applied on a replay only while the instance has not moved the reorder window ahead of it.
     */
    private void recordPending(String sagaId, EventMeta meta, Throwable failure) {
        try {
            for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
                SagaEnvelope<S> current = envelopes.findWithoutState(sagaId).orElse(null);
                FailureRecord<S> record = SagaExecutionSupport.pendingRecord(saga, sagaId, current, meta, clock.get(), config.reorderWindow());
                if (record != null && !fits(record)) {
                    if (unwritableErrorDue(UNWRITABLE_PENDING, sagaId)) {
                        log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because its state store {} cannot write the instance with it listed. "
                                        + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window. This is logged at most once per {} for the instance.",
                                subscriptionId, sagaId, meta.redeliveryKey(), stateStore.getClass().getName(), config.reorderWindow(), unroutableErrorInterval());
                    }
                    return;
                }
                if (record == null || envelopes.compareAndSaveWithoutState(sagaId, record.envelope(), record.expectedVersion())) {
                    return;
                }
            }
            log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the instance kept changing under {} attempts. "
                            + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window.",
                    subscriptionId, sagaId, meta.redeliveryKey(), config.maxCasAttempts(), config.reorderWindow());
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the state store failed. "
                            + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window.",
                    subscriptionId, sagaId, meta.redeliveryKey(), config.reorderWindow(), storeFailure);
            if (storeFailure != failure) {
                failure.addSuppressed(storeFailure);
            }
        }
    }

    // The redelivery key where the event has one, and otherwise its CloudEvent id and source, which also stay the same
    // from one delivery of the event to the next.
    private static String unroutableKey(EventMeta meta, CloudEvent cloudEvent) {
        String redeliveryKey = meta.redeliveryKey();
        return redeliveryKey != null ? redeliveryKey : cloudEvent.getId() + " from " + cloudEvent.getSource();
    }

    // The input process(..) runs, named the way the logs name it. A timeout has no CloudEvent
    private static String inputName(EventMeta meta, @Nullable CloudEvent cloudEvent, @Nullable String timerName) {
        return cloudEvent == null ? "the timer '" + timerName + "'" : "the event '" + unroutableKey(meta, cloudEvent) + "'";
    }

    // What an attempt the store refused says about the dispatch. The store is asked once per attempt, so an earlier
    // attempt that lost its compare-and-set dispatched its commands before the store refused this attempt
    private static String dispatchedBefore(int attemptNumber, int dispatchedByEarlierAttempts) {
        if (dispatchedByEarlierAttempts == 0) {
            return "No command of it was dispatched.";
        }
        int earlierAttempts = attemptNumber - 1;
        return "This attempt dispatched none of its commands, but " + dispatchedByEarlierAttempts + (dispatchedByEarlierAttempts == 1 ? " command was" : " commands were")
               + " dispatched by " + (earlierAttempts == 1 ? "the earlier attempt, which lost its" : "the " + earlierAttempts + " earlier attempts, which lost their")
               + " write to another writer.";
    }

    /**
     * Say that this delivery failed before the saga could work out which instance it belongs to, and that it is
     * refused. It is refused on every redelivery, however long it has been failing, and the pacing below changes
     * nothing about that.
     * <p>
     * Acknowledging it is what would lose it. An event the saga cannot route may still belong to an instance, and the
     * next event for that instance moves the instance's watermark past it, so feeding the repaired event to the saga
     * again afterwards is taken for a redelivery and ignored. Nothing can be written for it either, since there is no
     * instance to write on. Where the subscription offers it again, once the converter or the id extractor can read it
     * the event is applied in the order it was written, with nothing to feed again. What it holds up in the meantime is
     * decided by whatever feeds the subscription, and {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} says what
     * that can be.
     * <p>
     * The first failure is a warning, and after that it is logged at ERROR at most once per
     * {@link #unroutableErrorInterval()} rather than every time the source offers it, as long as each offer comes within
     * twice that interval of the one before. That interval is the quarantine budget when one is configured, and a fixed
     * five-minute default otherwise, so an operator alerting on ERROR is paged on a subscription model this saga cannot
     * quarantine anything on, for as long as that model keeps offering the event that often. A model that offers it more
     * than twice the interval apart gets a WARN on every offer, and one that does not offer a refused delivery again
     * gets only the first WARN. See {@link #unroutableErrorInterval()} for why. An event with no redelivery key is named
     * by its CloudEvent id and source instead.
     * <p>
     * An event that is not offered again for more than twice that interval is forgotten, because one repaired on another
     * node after a competing consumer's lease moved, or given up on by a retry strategy, never routes here to be
     * dropped. If it does come back after that, it is warned about as a new failure, with its time measured from then. A
     * model that offers the event a little less often than the interval still gets an ERROR on every offer. The other
     * forgotten events are dropped at most once per interval, by whichever refusal comes first after it.
     */
    private void refuseUnroutableDelivery(EventMeta meta, CloudEvent cloudEvent, Throwable failure) {
        if (!SagaExecutionSupport.isAttributableToTheInstance(failure)) {
            return;
        }
        String redeliveryKey = unroutableKey(meta, cloudEvent);
        Instant now = clock.get();
        Duration interval = unroutableErrorInterval();
        Instant sweptAt = unroutableSweptAt.get();
        if ((sweptAt == null || Duration.between(sweptAt, now).compareTo(interval) >= 0) && unroutableSweptAt.compareAndSet(sweptAt, now)) {
            unroutableDeliveries.values().removeIf(delivery -> forgotten(delivery, now, interval));
        }
        // Atomic per key, so two concurrent refusals of one event log one WARN and at most one ERROR per interval
        UnroutableDelivery[] before = new UnroutableDelivery[1];
        boolean[] errorDue = new boolean[1];
        unroutableDeliveries.compute(redeliveryKey, (key, existing) -> {
            // Checked here as well, because the forgotten entries are dropped at most once per interval, so the refused event's own entry can still be here
            if (existing == null || forgotten(existing, now, interval)) {
                return new UnroutableDelivery(now, now, now);
            }
            before[0] = existing;
            errorDue[0] = Duration.between(existing.lastLoggedAt(), now).compareTo(interval) >= 0;
            return new UnroutableDelivery(existing.firstFailedAt(), errorDue[0] ? now : existing.lastLoggedAt(), now);
        });
        UnroutableDelivery existing = before[0];
        if (existing == null) {
            log.warn("Saga '{}' could not work out which instance the event '{}' belongs to, so it refused the event rather than acknowledging it, because an event the saga cannot route may still belong to an instance and acknowledging it would lose it. Where the subscription offers the event again, it is applied in the order it was written once the converter or the id extractor can read it. What it holds up until then is decided by whatever feeds this subscription. Whether it is offered again is up to the subscription model, and DeliveryFailurePolicy is where a broker source's choice is configured.",
                    subscriptionId, redeliveryKey, failure);
            return;
        }
        if (!errorDue[0]) {
            return;
        }
        log.error("Saga '{}' has been unable to work out which instance the event '{}' belongs to for {}. What it holds up is decided by whatever feeds this subscription. The event is refused rather than skipped, because acknowledging it would lose it, and no instance is quarantined, because the event reached none. Where the event is offered again, repair the converter or the id extractor and it is applied in the order it was written, with nothing to feed to the saga again.",
                subscriptionId, redeliveryKey, Duration.between(existing.firstFailedAt(), now), failure);
    }

    /**
     * How often {@link #refuseUnroutableDelivery} repeats its ERROR once the first failure has already been warned
     * about. An event the saga cannot route is refused whether or not {@link SagaRunnerConfig#quarantineAfter()} is
     * set, because nothing here ever quarantines an instance for it (see {@link #refuseUnroutableDelivery}). Pacing
     * this ERROR on that budget alone would leave a saga without one warning once and then silent for good, even while
     * the model keeps offering the event. That is every saga with quarantine off. A model that does not offer a refused
     * delivery again gets only the first WARN. Using
     * the configured budget when there is one keeps one number for an operator to reason about, and
     * {@link #UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE} otherwise gives every other saga the same five-minute
     * cadence rather than inventing a second tunable for it.
     * <p>
     * The ERROR that the store cannot write the failure record of an instance, or list its input as pending, repeats at
     * the same interval per instance, for the same reason, see {@link #unwritableErrorDue}.
     */
    Duration unroutableErrorInterval() {
        return config.quarantineAfter().orElse(UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE);
    }

    // Halves the elapsed time rather than doubling the interval, which overflows for a very long budget
    private static boolean forgotten(UnroutableDelivery delivery, Instant now, Duration interval) {
        return Duration.between(delivery.lastOfferedAt(), now).dividedBy(UNROUTABLE_FORGOTTEN_AFTER_INTERVALS).compareTo(interval) > 0;
    }

    int rememberedUnroutableDeliveries() {
        return unroutableDeliveries.size();
    }

    // Whether the ERROR of the kind given for sagaId is due, which it is at most once per unroutableErrorInterval(),
    // atomically per instance so two concurrent deliveries log it once
    private boolean unwritableErrorDue(String kind, String sagaId) {
        Instant now = clock.get();
        Duration interval = unroutableErrorInterval();
        Instant sweptAt = unwritableErrorSweptAt.get();
        if ((sweptAt == null || Duration.between(sweptAt, now).compareTo(interval) >= 0) && unwritableErrorSweptAt.compareAndSet(sweptAt, now)) {
            unwritableErrorLoggedAt.values().removeIf(loggedAt -> Duration.between(loggedAt, now).compareTo(interval) >= 0);
        }
        boolean[] due = new boolean[1];
        unwritableErrorLoggedAt.compute(List.of(kind, sagaId), (key, loggedAt) -> {
            due[0] = loggedAt == null || Duration.between(loggedAt, now).compareTo(interval) >= 0;
            return due[0] ? now : loggedAt;
        });
        return due[0];
    }

    /**
     * Record that this event failed for this instance, and answer whether that ended the instance's time budget. A true
     * answer means the caller returns normally. A false answer means the exception propagates exactly as it always has.
     * <p>
     * The failure is a {@link Throwable} because the instance is stuck on it either way. An {@code Error} out of a
     * reaction stops the instance making progress exactly as a {@code RuntimeException} does, and it costs the saga's
     * other instances exactly the same.
     * <p>
     * A record whose write loses to another write on the instance is worked out again from what the instance holds now
     * and written again, in {@link SagaRunnerConfig#maxCasAttempts()} attempts at most. Discarding it instead let a
     * timer that fired between every read and write keep the instance from ever being quarantined, which ADR 157
     * records. An event past its budget, on an instance another writer changed between the record's read and its write,
     * is then quarantined without being tried against that change.
     * <p>
     * Only the event path calls this, and it is not because the timer path has nothing to gain from it. A timeout
     * has no redelivery key of its own, so there is nothing to quarantine it on.
     * <p>
     * The timer path retries a failing timeout on a later poll instead, and {@link #markFailedAttempt} makes the timer
     * due again one millisecond after each poll that failed to fire it. {@link SagaStateStore#findWithDueTimers}
     * returns the instances that have been due longest first, so an instance whose timer keeps failing is tried again
     * only after the instances that were due at its last attempt.
     */
    private boolean quarantine(String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Duration quarantineAfter) {
        try {
            Instant now = clock.get();
            for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
                // Read without the state, because nothing on this path applies it and an instance whose state no longer
                // decodes is the one that most needs to reach its budget. Loading it whole threw, the catch below
                // swallowed that, and no failure record was ever written.
                SagaEnvelope<S> current = envelopes.findWithoutState(sagaId).orElse(null);
                FailureRecord<S> record = SagaExecutionSupport.onFailure(saga, sagaId, current, meta, failure, now, quarantineAfter, config.reorderWindow());
                if (record == null) {
                    return false;
                }
                // An instance close to what its store writes, such as one whose event failed because the store cannot
                // write it with the event applied, gets the bare record, which keeps the event's clock. Null when the
                // stored record names the event already, so there is nothing to write
                if (!fits(record)) {
                    FailureRecord<S> bare = SagaExecutionSupport.onFailureBare(current, meta, record);
                    if (bare != null && !fits(bare)) {
                        if (unwritableErrorDue(UNWRITABLE_FAILURE, sagaId)) {
                            log.error("Saga '{}' instance '{}' failed on the event '{}' and could not record the failure, because its state store {} cannot write the instance with the failure recorded, even with no failure message. "
                                      + "The exception goes back to whatever feeds this subscription, and the instance is never quarantined on the event, so a subscription that delivers the event again before any later one holds up every later event of the saga. "
                                      + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window. "
                                      + "Delete the instance with SagaStateStore.delete(\"{}\"), which throws its state away, or handle the event by hand. This is logged at most once per {} for the instance.",
                                    subscriptionId, sagaId, meta.redeliveryKey(), stateStore.getClass().getName(), config.reorderWindow(), sagaId, unroutableErrorInterval(), failure);
                        }
                        return false;
                    }
                    record = bare;
                }
                // Written without the state for the same reason it was read without it. The envelope carries whatever
                // the read gave, so saving it whole would erase the state somebody needs once the converter is repaired.
                // The write that quarantines also records the failing event, so a release replays it from there. A
                // failure of it leaves the event unacknowledged, since the delivery's own failure is still thrown.
                boolean saved = record == null || (record.quarantined()
                        ? envelopes.compareAndSaveSkipped(skippedInputs(sagaId), record.envelope(), record.expectedVersion(),
                                Objects.requireNonNull(record.envelope().quarantineId(), "quarantineId"),
                                SagaExecutionSupport.quarantiningInput(record.envelope(), cloudEvent))
                        : envelopes.compareAndSaveWithoutState(sagaId, record.envelope(), record.expectedVersion()));
                if (!saved) {
                    // Another input advanced the instance, most likely a timer that fired. Worked out again from what is
                    // stored now, since a write dropped here can be dropped on every delivery and never quarantine.
                    continue;
                }
                if (record == null || !record.quarantined()) {
                    log.warn("Saga '{}' instance '{}' failed on the event '{}'. Whether the event is offered again is for whatever feeds this subscription to decide. The instance can be quarantined once this event has been failing for {}, measured from when this event first failed, even if that was before it was parked and offered again, but that is not the only condition, so read the instance's status to know whether it was.",
                            subscriptionId, sagaId, meta.redeliveryKey(), quarantineAfter, failure);
                    return false;
                }
                log.error("Saga '{}' instance '{}' is now QUARANTINED, stopped on the event '{}', which has been failing for {}, past its budget of {}. The time runs from when this event first failed, which can be before it was parked and offered again. It skips every further event and fires no timers. Find it with findByStatus(QUARANTINED, ..).",
                        subscriptionId, sagaId, meta.redeliveryKey(), failingFor(record, now), quarantineAfter, failure);
                return true;
            }
            log.warn("Saga '{}' instance '{}' failed on the event '{}' and could not record the failure, because the instance kept changing under {} attempts. "
                            + "The exception goes back to whatever feeds this subscription, and the next failure of the event tries again.",
                    subscriptionId, sagaId, meta.redeliveryKey(), config.maxCasAttempts(), failure);
            // Still listed as pending, so the event keeps its clock and a replay long after is applied
            recordPending(sagaId, meta, failure);
            return false;
        } catch (Throwable storeFailure) {
            // What reaches here is the store failing, the codec refusing an instance the store read back, or the codec
            // failing to encode the saga's initial state when the failure record is the instance's first write.
            // Rethrowing the original is today's behaviour and it is the right one for each, because a saga that cannot
            // read or write its own instance cannot make progress on it either way.
            rethrowIfNotTheInstances(storeFailure);
            // Only an attributable failure with a redelivery key gets here, and the version could not be listed as pending
            // either, which is what a replay long after depends on. The store's own message goes in the line, since a store
            // that refuses to hold the event, rather than failing, names the part it refuses there
            log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the state store threw {}. "
                            + "A store that refuses to hold the event says so in that message, and refuses it again on every delivery. "
                            + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window.",
                    subscriptionId, sagaId, meta.redeliveryKey(), storeFailure, config.reorderWindow(), storeFailure);
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
     * instance that happened to be running, and a store read raising one is the same condition
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

    // Whether the store can take the failure write record, or true when the store cannot say, and then a write it
    // cannot take throws. An insert writes the whole instance
    private boolean fits(FailureRecord<S> record) {
        SagaStateStoreSkippedInputs records = skippedInputs;
        if (records == null) {
            return true;
        }
        return record.expectedVersion() == 0 ? envelopes.fits(records, record.envelope())
                : envelopes.fitsWithoutState(records, record.envelope(), record.expectedVersion());
    }

    // How long the event has actually been failing, taken from the record rather than from the budget, because a
    // redelivery can arrive well after the budget elapsed and the two then differ by however late it was. The record
    // names this event, with the instant it first failed, whenever it quarantines.
    private static <S extends @Nullable Object> Duration failingFor(FailureRecord<S> record, Instant now) {
        SagaFailure failure = record.envelope().failure();
        return failure == null ? Duration.ZERO : Duration.between(failure.firstFailedAt(), now);
    }

    void pollTimers() {
        // Catch Throwable so a failure never lets the scheduled task die and stop all future polling. The schedule stays
        // alive and the next tick recovers.
        try {
            Instant now = clock.get();
            long nowMillis = now.toEpochMilli();
            List<SagaEnvelope<S>> due = envelopes.findWithDueTimers(now, config.timerBatchLimit());
            for (SagaEnvelope<S> envelope : due) {
                List<TimerEntry> dueTimers = envelope.timers().stream()
                        // Due time rather than firesAt, so a failed timer is not tried again ahead of its turn when another
                        // timer on the same instance brought the instance into the batch
                        .filter(timer -> timer.dueAtEpochMilli() <= nowMillis)
                        .toList();
                for (TimerEntry timer : dueTimers) {
                    String timerName = timer.name();
                    try {
                        process(envelope.sagaId(), SagaInput.timeout(envelope.sagaId(), TimerName.parse(timerName)), EventMeta.NONE, null, timerName);
                    } catch (Throwable t) {
                        // Throwable rather than RuntimeException, because a StackOverflowError out of a recursive
                        // evolve or a NoClassDefFoundError out of a reaction used to unwind the loop into the catch
                        // below, so every instance after this one in the batch lost its turn. An
                        // OutOfMemoryError still does, because it is the JVM's condition and not this instance's.
                        rethrowIfNotTheInstances(t);
                        // Keep polling other timers/instances. This one stays due, so a later poll retries it unless
                        // it is consumed first.
                        log.warn("Failed to fire saga timer '{}' for instance '{}'", timerName, envelope.sagaId(), t);
                        markFailedAttempt(envelope.sagaId(), timer, now);
                    }
                }
            }
        } catch (Throwable t) {
            log.warn("Saga timer poll failed", t);
        }
        continueReleases();
    }

    /**
     * Go on with the release of every instance that is releasing, which retries an input that failed during a release
     * until it succeeds or its budget runs out, and resumes a release a crash interrupted. Asked on every poll, so with
     * a competing-consumer strategy only the node holding the saga's timer lease asks.
     * <p>
     * A release writes the instance for every record it replays and the first time an input fails, so an instance written
     * within the last {@code timerPollInterval} has a release still running, on this node or another, and the poll
     * skips it. Polls are a {@code timerPollInterval} apart from the end of one to the start of the next, so a
     * release this poller went on with is older than that by the next poll. A release that stopped, after a crash or a
     * failed input, is gone on with by the first poll a {@code timerPollInterval} after its last write. The poll and a
     * release can still replay the same record, and both dispatch its commands, which delivery at least once allows,
     * in five cases. A release takes longer than that over one record. A node's clock is that far ahead of the
     * writer's. A {@code release} call comes while a poll replays the instance, which is releasing already, so the
     * call goes on beside the poll without the check. A release works through records the stream marks show as
     * applied, which it deletes in writes that leave the instance's {@code updatedAt} as it is, for longer than that.
     * It dispatches nothing for those records, so only the first record that isn't applied can be dispatched twice. Two
     * polls read the instance at the same time, once it is that old, and both go on with it. Without a
     * competing-consumer strategy every node polls, and with one, two nodes can poll at once while the timer lease
     * moves from one to the other. A retry that fails again on the same input within its budget writes nothing, so
     * while that input keeps failing, every node that polls retries it. A store that does not implement
     * {@link SagaStateStoreSkippedInputs} has no releasing instances to list, so it does not ask.
     * <p>
     * A store whose {@link SagaStateStoreSkippedInputs#requireAtomicTransitions()} throws, as
     * {@code SpringMongoSagaStateStore} on a standalone server, refuses the writes that apply a replayed record, so the
     * poll replays nothing there and dispatches nothing. An instance marked releasing in such a store, such as one in data
     * restored from a replica set, stays as it is, and the first poll that lists one logs an {@code ERROR} naming the
     * instances it listed, once per runner.
     * <p>
     * One poll replays at most {@code timerBatchLimit} records, shared evenly among the instances it lists and at least
     * one each, and the next poll goes on where this poll stopped. It lists at most {@code timerBatchLimit} instances,
     * in id order, and the next poll lists the ones after the last of them. A poll that lists fewer reached the end, so
     * the poll after it starts from the first again, and every releasing instance gets a turn however many sit at the
     * front.
     */
    private void continueReleases() {
        SagaStateStoreSkippedInputs records = skippedInputs;
        if (records == null) {
            return;
        }
        if (cannotRecordSkips) {
            sayReleasingInstancesStay(records);
            return;
        }
        List<String> releasing;
        try {
            releasing = records.findReleasingSagaIds(releasingCursor.get(), config.timerBatchLimit());
        } catch (Throwable t) {
            log.warn("Saga '{}' could not list the instances it is releasing", subscriptionId, t);
            return;
        }
        releasingCursor.set(releasing.size() < config.timerBatchLimit() ? null : releasing.getLast());
        // Shared evenly so an instance with many records does not hold back the others. At most timerBatchLimit ids
        // are listed, so the shares add up to at most timerBatchLimit
        int share = releasing.isEmpty() ? 0 : Math.max(1, config.timerBatchLimit() / releasing.size());
        for (String sagaId : releasing) {
            try {
                continueRelease(sagaId, records, share, clock.get().minus(config.timerPollInterval()));
            } catch (Throwable t) {
                log.warn("Saga '{}' could not go on with the release of instance '{}'. The next poll tries again.", subscriptionId, sagaId, t);
                if (!SagaExecutionSupport.isAttributableToTheInstance(t)) {
                    // An OutOfMemoryError is the JVM's condition and not this instance's, so the rest of the poll waits
                    return;
                }
            }
        }
    }

    // Lists the releasing instances without going on with them, and says the first time it finds one. Listing stops once
    // it has said so, since nothing on this store changes what it would say
    private void sayReleasingInstancesStay(SagaStateStoreSkippedInputs records) {
        if (releasingStaySaid.get()) {
            return;
        }
        List<String> releasing;
        try {
            releasing = records.findReleasingSagaIds(null, config.timerBatchLimit());
        } catch (Throwable t) {
            log.warn("Saga '{}' could not list the instances it is releasing", subscriptionId, t);
            return;
        }
        if (!releasing.isEmpty() && releasingStaySaid.compareAndSet(false, true)) {
            log.error("Saga '{}' does not go on with the release of instances {}, because its state store {} cannot write a replayed input and the removal of its record in one atomic write. "
                      + "They stay releasing, and the delivery of an input they would record as skipped fails, so the input is delivered again. A subscription that delivers an event again before any later one, as a MongoDB subscription does, holds up every later event meanwhile, for every instance of the saga. "
                      + "Move the saga's data back to a store that records skips, such as MongoDB on a replica set, where the timer poller goes on with them, or resolve them by hand, such as by deleting them from the database.",
                    subscriptionId, releasing, stateStore.getClass().getName());
        }
    }

    /**
     * Release the quarantined instance {@code sagaId}, as {@link SagaSubscription#release(String)} describes.
     */
    SagaSubscription.ReleaseResult release(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        SagaStateStoreSkippedInputs records = skippedInputs(sagaId);
        requireReleasable(sagaId);
        for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
            SagaEnvelopes.WithFirstSkippedInput<S> read = findToRelease(records, sagaId);
            if (read == null) {
                return SagaSubscription.ReleaseResult.NOT_FOUND;
            }
            SagaEnvelope<S> current = read.envelope();
            if (!current.isQuarantined()) {
                return SagaSubscription.ReleaseResult.NOT_QUARANTINED;
            }
            Instant now = clock.get();
            SagaEnvelope<S> releasing = SagaExecutionSupport.startRelease(current, now);
            if (releasing == null) {
                // Released already, by another call or another node, so this one goes on with it
                return continueReleaseToTheEnd(sagaId, records);
            }
            requireTheStoppedOnInput(current, read.first());
            if (!envelopes.fitsWithoutState(records, releasing, current.version())) {
                // The instance as stored can leave too little room for the write that starts the release
                releasing = Objects.requireNonNull(SagaExecutionSupport.startReleaseTrimmed(current, now, config.reorderWindow()));
                if (!envelopes.fitsWithoutState(records, releasing, current.version())) {
                    IllegalStateException refused = SagaExecutionSupport.cannotRelease(current, "its state store " + records.getClass().getName()
                            + " cannot write it with the release started, which sets SagaFailure.releasedAt, even with its marks trimmed to the reorder window. "
                            + "Deleting the instance deletes the inputs it recorded too, so handle them by hand first");
                    log.error("Saga '{}' did not start the release of instance '{}'.", subscriptionId, sagaId, refused);
                    throw refused;
                }
            }
            // No record is written, since the input the instance was quarantined on has the first record already
            if (!envelopes.compareAndSaveWithoutState(sagaId, releasing, current.version())) {
                continue;
            }
            log.info("Saga '{}' instance '{}' is releasing. It replays the event '{}' it was quarantined on and then the inputs it skipped. "
                     + "A release that ends QUARANTINED_AGAIN without replaying anything can also mean that the state store does not persist SagaFailure.releasedAt.",
                    subscriptionId, sagaId, Objects.requireNonNull(current.failure()).input());
            return continueReleaseToTheEnd(sagaId, records);
        }
        throw new SagaConcurrencyException("Failed to start the release of saga '" + sagaId + "' after " + config.maxCasAttempts() + " attempts due to concurrent modification");
    }

    // A store that cannot write a transition atomically refuses the writes that apply a replayed record, while the write
    // that starts a release asks for no transaction. So a release there would mark the instance releasing and dispatch the
    // first record's commands before the write that applies it failed, and every poll would send them again. Refused
    // before anything is read, even for an instance the store does not hold, as skippedInputs refuses
    private void requireReleasable(String sagaId) {
        if (cannotRecordSkips) {
            throw new IllegalStateException(("Saga '%s' cannot release instance '%s', because its state store %s cannot write a replayed input and the removal of its record in one atomic write, "
                                             + "so a release would dispatch the commands of an input it then fails to record as applied. Nothing was read or written. "
                                             + "Move the saga's data back to a store that records skips, such as MongoDB on a replica set, and release the instance there, or resolve the instance by hand, such as by deleting it from the database.")
                    .formatted(subscriptionId, sagaId, stateStore.getClass().getName()));
        }
    }

    // The quarantining write records the input the instance stopped on as its first record, in the write that sets the
    // failure, and so does a release that quarantines it again. The instance and its first record come from one read, so
    // a first record under another key, or none, means the records were lost outside the saga, such as an instance
    // restored without them. Releasing would make it ACTIVE without that input ever applied
    private void requireTheStoppedOnInput(SagaEnvelope<S> current, @Nullable SkippedInput first) {
        String stoppedOn = Objects.requireNonNull(current.failure()).input();
        if (first != null && first.key().equals(stoppedOn)) {
            return;
        }
        throw SagaExecutionSupport.cannotRelease(current, "the state store returned no record of the input '" + stoppedOn
                + "' it was quarantined on, so a release would make it ACTIVE without ever applying that input. "
                + "The records are lost when the instance is restored without the records of its quarantine, and restoring them from the same backup lets it be released. "
                + "A state store whose findWithFirstSkippedInput returns the instance without the records stored with it gives the same answer");
    }

    // Read whole, so an instance whose state does not decode is refused here rather than marked releasing with nothing
    // able to apply to it. A read that fails on an instance a release of is running already, read without its state,
    // returns that instance, and the release goes on with it and counts the failure against the budget of its first
    // record, or ends the quarantine when no record is left, see recordReadFailure. That holds whether the state or the
    // record failed to decode. Otherwise a read that fails while the instance reads without its state but not with it
    // failed on decoding the state, and is refused with the decoding failure named. Any other failure of the read, such
    // as a record that doesn't decode, propagates as it is, and the instance stays quarantined. A store that refuses to
    // read where it is called read nothing, and the reads that check the decoding would run in the same place, such as
    // inside a transaction of the caller's
    private SagaEnvelopes.@Nullable WithFirstSkippedInput<S> findToRelease(SagaStateStoreSkippedInputs records, String sagaId) {
        try {
            return envelopes.findWithFirstSkippedInput(records, sagaId).orElse(null);
        } catch (SagaReadRefusedException refused) {
            throw refused;
        } catch (Throwable readFailure) {
            rethrowIfNotTheInstances(readFailure);
            SagaEnvelope<S> withoutState = null;
            try {
                withoutState = envelopes.findWithoutState(sagaId).orElse(null);
            } catch (Throwable secondFailure) {
                rethrowIfNotTheInstances(secondFailure);
                readFailure.addSuppressed(secondFailure);
            }
            if (withoutState != null && withoutState.isReleasing()) {
                return new SagaEnvelopes.WithFirstSkippedInput<>(withoutState, null);
            }
            if (withoutState == null || !failsToRead(sagaId)) {
                rethrowAsIs(readFailure);
            }
            throw new IllegalStateException("Saga '" + subscriptionId + "' cannot release instance '" + sagaId + "', because reading its state failed with " + readFailure + ", while reading it without its state worked. "
                                            + "Release the instance again if that error doesn't last. If its state no longer decodes, fix how it decodes and release the instance again, or delete the instance with SagaStateStore.delete(\"" + sagaId + "\"), which throws its state away.", readFailure);
        }
    }

    private boolean failsToRead(String sagaId) {
        try {
            envelopes.find(sagaId);
            return false;
        } catch (Throwable failure) {
            rethrowIfNotTheInstances(failure);
            return true;
        }
    }

    private static void rethrowAsIs(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new IllegalStateException(failure);
    }

    private SagaSubscription.ReleaseResult continueReleaseToTheEnd(String sagaId, SagaStateStoreSkippedInputs records) {
        return Objects.requireNonNull(continueRelease(sagaId, records, Integer.MAX_VALUE, null));
    }

    /**
     * Apply the event of the first record of a releasing instance, and repeat until no record is left, a record fails,
     * or the instance stops releasing. The write that applies a record deletes it too. The event is replayed with its own
     * stream id and stream version, so a record whose input the instance applied already is recognised by the stream
     * marks and deleted in a write that raises only the version.
     * <p>
     * The instance and its first record come from one read, see
     * {@link SagaStateStoreSkippedInputs#findWithFirstSkippedInput}, so the record is the first one under the
     * instance's quarantine id at the version read. The writes that applied the records before it are all in that
     * version, so a replay computes and dispatches from the state those records left, such as the state after a start
     * event. Every write a release makes for the read expects its version, the one that ends the quarantine, the one
     * that applies the record, the one that deletes it, the failure record and the one that quarantines the instance
     * again. A skip recorded since, or another release that applied the record, raised the version, so such a write
     * loses and the loop reads both again.
     * <p>
     * A release dispatches a record's commands before the write that applies it, so like a live input it dispatches them
     * at least once. A write that loses only to skipped inputs recorded in between is written again without
     * dispatching, see {@link #saveReplayed}. A write that loses to anything else reads the instance and its first record
     * again and goes on
     * from what it finds, so two nodes releasing the same instance compete for each record.
     * <p>
     * The store is asked before every write whether it can take it, see {@link SagaStateStoreSkippedInputs#fits}, and
     * for the instance with a record applied that is before the record's commands are dispatched. When it cannot take
     * that write, or the failure record of a record that failed, the record is not applied and its commands are not
     * dispatched. The instance is quarantined again on it with its marks trimmed, see
     * {@link SagaExecutionSupport#onReleaseTooLarge}, or with
     * {@link SagaExecutionSupport#onReleaseTooLargeBare} when the store cannot take that either, and this returns
     * {@code QUARANTINED_AGAIN}. The ERROR says whether a further release gets past the record, which it does when the
     * store can write the record applied to the trimmed instance. When the store cannot take any write that moves the
     * release on, nothing is written, the instance stays releasing, this returns {@code STILL_RELEASING}, and an ERROR
     * names the instance and says to delete it.
     * <p>
     * At most {@code maxRecords} records are replayed. When a further record is there after that, this returns
     * {@code null} with the instance still releasing, and the next call goes on from that record. With
     * {@code leaveIfWrittenSince}, an instance first read with an {@code updatedAt} after it is left as it is, and this
     * returns {@code null}.
     * <p>
     * A read of the instance and its first record that fails, such as on a state a replay saved that doesn't decode any
     * more or on a record whose event doesn't, counts against the budget of that record the way a replay that fails
     * does. With no record left the quarantine ends instead, and the stored state stays as it is. Nothing the release
     * reads or checks after such a read decodes the stored state, the size checks included. The event of the record is
     * decoded only to name the input in the failure record, from the record read alone, and a throw there names it by
     * its key instead. So no decoding stops the failure from being counted or the quarantine from ending. See
     * {@link #recordReadFailure}.
     */
    private SagaSubscription.@Nullable ReleaseResult continueRelease(String sagaId, SagaStateStoreSkippedInputs records, int maxRecords,
                                                                     @Nullable Instant leaveIfWrittenSince) {
        int lostInARow = 0;
        int replayed = 0;
        boolean firstRead = true;
        while (true) {
            SagaEnvelopes.WithFirstSkippedInput<S> read;
            try {
                read = envelopes.findWithFirstSkippedInput(records, sagaId).orElse(null);
            } catch (SagaReadRefusedException refused) {
                throw refused;
            } catch (Throwable readFailure) {
                rethrowIfNotTheInstances(readFailure);
                SagaSubscription.ReleaseResult afterReadFailure = recordReadFailure(records, sagaId, readFailure);
                if (afterReadFailure != null) {
                    return afterReadFailure;
                }
                lostInARow = lostAgain(sagaId, lostInARow);
                continue;
            }
            SagaEnvelope<S> current = read == null ? null : read.envelope();
            SagaSubscription.ReleaseResult ended = resultUnlessReleasing(current);
            if (ended != null) {
                return ended;
            }
            current = Objects.requireNonNull(current);
            Instant updatedAt = current.updatedAt();
            if (firstRead && leaveIfWrittenSince != null && updatedAt != null && updatedAt.isAfter(leaveIfWrittenSince)) {
                return null;
            }
            firstRead = false;
            String quarantineId = current.quarantineId();
            if (quarantineId == null) {
                throw new IllegalStateException("Saga '" + subscriptionId + "' instance '" + sagaId + "' is releasing with no quarantine id, so there are no recorded inputs to replay.");
            }
            SkippedInput record = Objects.requireNonNull(read).first();
            Instant now = clock.get();
            if (record == null) {
                SagaSubscription.ReleaseResult released = endQuarantine(records, sagaId, current, quarantineId, now);
                if (released != null) {
                    return released;
                }
                lostInARow = lostAgain(sagaId, lostInARow);
                continue;
            }
            if (replayed >= maxRecords) {
                return null;
            }
            CloudEvent cloudEvent = record.event();
            EventMeta meta = extractMeta(cloudEvent).meta();
            boolean saved;
            SagaEnvelope<S> tooLarge = null;
            boolean deleteTooLarge = false;
            try {
                E event = converter.toDomainEvent(cloudEvent);
                SagaInput<E> input = SagaInput.event(event, EventMetadata.from(cloudEvent));
                Outcome<S, C> outcome = SagaExecutionSupport.replay(saga, sagaId, current, record, input, meta, now, config.reorderWindow());
                if (outcome.processed()) {
                    SagaEnvelope<S> applied = Objects.requireNonNull(outcome.envelope());
                    // A release keeps every version it applies until it ends, so a long backlog can grow the instance
                    // past what its store writes. Asked before the dispatch, so a write the store refuses dispatches
                    // nothing
                    if (envelopes.fits(records, applied)) {
                        dispatcher.dispatchAll(outcome.commands());
                        saved = saveReplayed(records, sagaId, quarantineId, record, current, outcome);
                    } else {
                        tooLarge = applied;
                        saved = false;
                    }
                } else {
                    // Applied already, or not a start event for an instance that never started, so it is only deleted
                    SagaEnvelope<S> deleted = SagaExecutionSupport.oneVersionUp(current);
                    deleteTooLarge = !envelopes.fits(records, deleted);
                    saved = !deleteTooLarge && envelopes.compareAndSaveReplayed(records, deleted, current.version(), quarantineId, record);
                }
            } catch (Throwable failure) {
                rethrowIfNotTheInstances(failure);
                return recordReleaseFailure(records, sagaId, quarantineId, RecordKey.of(record), meta, failure, now);
            }
            if (deleteTooLarge) {
                // The instance applied the record's input already, so quarantining it again on that input is not an option
                return writesNothing(records, sagaId, record.key(), "the instance with the record deleted");
            }
            if (tooLarge != null) {
                SagaSubscription.ReleaseResult stopped = quarantineAgainTooLarge(records, sagaId, record.key(), meta, current, tooLarge, "the instance with the event applied", null, now);
                if (stopped != null) {
                    return stopped;
                }
                lostInARow = lostAgain(sagaId, lostInARow);
                continue;
            }
            if (saved) {
                lostInARow = 0;
                replayed++;
            } else {
                lostInARow = lostAgain(sagaId, lostInARow);
            }
        }
    }

    // Quarantine the instance again on record, because the store cannot take the write named by cannotWrite, without
    // dispatching its commands. That write is the instance with the event applied, which applied holds, or the failure
    // record of the event when the event failed with failure. The write that quarantines it is checked too, first the
    // one with the record's pending entry and the reason, then the bare one. When neither fits, nothing
    // is written. The ERROR says whether a further release gets past the record, which it does when the store can take
    // applied with the marks trimmed. Null when the instance changed since current was read, and the caller reads it again.
    // The caller read the record of recordKey as the first record under the quarantine id of current, in the read that returned current,
    // and the write expects current's version, so it never quarantines the instance on a record another release deleted
    private SagaSubscription.@Nullable ReleaseResult quarantineAgainTooLarge(SagaStateStoreSkippedInputs records, String sagaId, String recordKey, EventMeta meta,
                                                                           SagaEnvelope<S> current, @Nullable SagaEnvelope<S> applied, String cannotWrite,
                                                                           @Nullable Throwable failure, Instant now) {
        IllegalStateException reason = new IllegalStateException("The release of saga instance '" + sagaId + "' stopped on the event '" + recordKey
                                                                 + "', because its state store " + records.getClass().getName() + " cannot write " + cannotWrite + ".");
        FailureRecord<S> requarantine = SagaExecutionSupport.onReleaseTooLarge(current, meta, reason, now, config.reorderWindow());
        if (requarantine == null) {
            // Only when another writer applied the input since current was read
            return null;
        }
        if (!envelopes.fitsWithoutState(records, requarantine.envelope(), requarantine.expectedVersion())) {
            requarantine = Objects.requireNonNull(SagaExecutionSupport.onReleaseTooLargeBare(current, meta, reason, now, config.reorderWindow()));
            if (!envelopes.fitsWithoutState(records, requarantine.envelope(), requarantine.expectedVersion())) {
                return writesNothing(records, sagaId, recordKey, cannotWrite + ", nor the instance quarantined again on it");
            }
        }
        boolean fitsWhenReleasedAgain = applied == null || envelopes.fits(records,
                SagaExecutionSupport.appliedAfterReleasingAgain(applied, requarantine.envelope(), meta, now, config.reorderWindow()));
        if (!envelopes.compareAndSaveWithoutState(sagaId, requarantine.envelope(), requarantine.expectedVersion())) {
            return null;
        }
        if (applied == null) {
            log.error("Saga '{}' instance '{}' is QUARANTINED again on the event '{}' without applying it, because the event failed while it was releasing and its state store cannot write the failure record. Release the instance again to try the event again. The event and the inputs after it stay recorded.",
                    subscriptionId, sagaId, recordKey, failure);
        } else if (fitsWhenReleasedAgain) {
            log.error("Saga '{}' instance '{}' is QUARANTINED again on the event '{}' without applying it, because its state store cannot write the instance with the event applied. A release keeps every stream version it applies until it ends, and quarantining the instance again trimmed them. The store can write the event applied to the trimmed instance, so release the instance again to go on from that event. The event and the inputs after it stay recorded.",
                    subscriptionId, sagaId, recordKey);
        } else {
            log.error("Saga '{}' instance '{}' is QUARANTINED again on the event '{}' without applying it, because its state store cannot write the instance with the event applied, even with the stream versions the release applied trimmed. A further release stops on this event too. Delete the instance, or handle the event and the inputs after it by hand. They stay recorded.",
                    subscriptionId, sagaId, recordKey);
        }
        return SagaSubscription.ReleaseResult.QUARANTINED_AGAIN;
    }

    // The store cannot take any write that moves the release on, so nothing is written, nothing is dispatched, and the
    // instance stays releasing. The timer poller tries it again on every poll, and logs this again
    private SagaSubscription.ReleaseResult writesNothing(SagaStateStoreSkippedInputs records, String sagaId, @Nullable String recordKey, String cannotWrite) {
        log.error("Saga '{}' instance '{}' stays releasing{} and nothing is dispatched, because its state store {} cannot write {}. The timer poller tries it again on every poll. "
                  + "Delete the instance with SagaStateStore.delete(\"{}\"), which throws its state away and deletes the inputs it recorded, so export or handle those by hand first.",
                subscriptionId, sagaId, recordKey == null ? "" : " on the event '" + recordKey + "'", records.getClass().getName(), cannotWrite, sagaId);
        return SagaSubscription.ReleaseResult.STILL_RELEASING;
    }

    // What a release ended as, read off the instance, or null when the instance is still releasing. A release another call
    // or node ended is read this way too
    private static <S extends @Nullable Object> SagaSubscription.@Nullable ReleaseResult resultUnlessReleasing(@Nullable SagaEnvelope<S> current) {
        if (current == null) {
            return SagaSubscription.ReleaseResult.NOT_FOUND;
        }
        if (!current.isQuarantined()) {
            return SagaSubscription.ReleaseResult.RELEASED;
        }
        if (!current.isReleasing()) {
            return SagaSubscription.ReleaseResult.QUARANTINED_AGAIN;
        }
        return null;
    }

    private int lostAgain(String sagaId, int lostInARow) {
        if (lostInARow + 1 >= config.maxCasAttempts()) {
            throw new SagaConcurrencyException("Failed to go on with the release of saga '" + sagaId + "' after " + config.maxCasAttempts() + " attempts due to concurrent modification");
        }
        return lostInARow + 1;
    }

    // Save the write that applies a record and deletes it, or that deletes every record when the input completed the
    // instance. When it loses, and the instance read again differs only by skipped inputs recorded in between with the
    // same record first, the loss was to those records, and the same result is written again on top of them without
    // dispatching the commands a second time. Anything else returns false, and the caller reads the instance again and
    // recomputes
    private boolean saveReplayed(SagaStateStoreSkippedInputs records, String sagaId, String quarantineId, SkippedInput record, SagaEnvelope<S> read, Outcome<S, C> outcome) {
        SagaEnvelope<S> envelope = Objects.requireNonNull(outcome.envelope());
        long expectedVersion = outcome.expectedVersion();
        SagaEnvelope<S> basis = read;
        for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
            if (envelopes.compareAndSaveReplayed(records, envelope, expectedVersion, quarantineId, record)) {
                return true;
            }
            SagaEnvelopes.WithFirstSkippedInput<S> reread = envelopes.findWithFirstSkippedInput(records, sagaId).orElse(null);
            SagaEnvelope<S> after = reread == null ? null : reread.envelope();
            if (after == null || !SagaExecutionSupport.differsOnlyByRecordedSkips(basis, after)
                || !quarantineId.equals(after.quarantineId()) || !isSameRecord(reread.first(), record)) {
                return false;
            }
            envelope = SagaExecutionSupport.rebased(envelope, after);
            if (!envelopes.fits(records, envelope)) {
                // The record's commands went out with the first write. The caller reads the instance again and computes the
                // record again, which asks the store before it dispatches them a second time
                return false;
            }
            expectedVersion = after.version();
            basis = after;
        }
        return false;
    }

    // The store's skip records, looked up once per release and once per skip. A release on a store that does not
    // implement SagaStateStoreSkippedInputs throws here, even for an instance the store does not hold. A skip throws here
    // only for an instance quarantined while the store had the capability, such as a store now wrapped by a decorator
    // that implements SagaStateStore alone
    private SagaStateStoreSkippedInputs skippedInputs(String sagaId) {
        SagaStateStoreSkippedInputs records = skippedInputs;
        if (records == null) {
            throw new IllegalStateException("Saga '" + subscriptionId + "' cannot record or release the inputs instance '" + sagaId + "' skips, because its state store "
                                            + stateStore.getClass().getName() + " does not implement SagaStateStoreSkippedInputs.");
        }
        return records;
    }

    // Record that the record a release was applying failed, the way any failure is recorded, and say what that left. The
    // instance stays releasing until the input's budget runs out, and is quarantined on it again then. With quarantine
    // off there is no budget, so the release stays on the input until it succeeds. The instance is read again without its
    // state and with only the seq and key of its first record, so a state or an event that no longer decodes doesn't
    // keep the failure from being recorded
    private SagaSubscription.ReleaseResult recordReleaseFailure(SagaStateStoreSkippedInputs records, String sagaId, String quarantineId, RecordKey record,
                                                                EventMeta meta, Throwable failure, Instant now) {
        Duration quarantineAfter = config.quarantineAfter().orElse(ChronoUnit.FOREVER.getDuration());
        try {
            for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
                SagaEnvelopes.WithFirstSkippedInputKey<S> read = envelopes.findWithFirstSkippedInputKey(records, sagaId).orElse(null);
                if (read == null) {
                    return SagaSubscription.ReleaseResult.NOT_FOUND;
                }
                SagaEnvelope<S> current = read.envelope();
                // Another release applied the record since it was read, or ended the quarantine it was read under
                if (!quarantineId.equals(current.quarantineId()) || !isSameRecord(read.first(), record)) {
                    SagaSubscription.ReleaseResult moved = resultUnlessReleasing(current);
                    if (moved != null) {
                        logMovedOn(sagaId, record, moved, failure);
                        return moved;
                    }
                    break;
                }
                FailureRecord<S> failureRecord = SagaExecutionSupport.onReleaseFailure(current, meta, failure, now, quarantineAfter, config.reorderWindow());
                if (failureRecord == null) {
                    SagaSubscription.ReleaseResult moved = resultUnlessReleasing(envelopes.findWithoutState(sagaId).orElse(null));
                    if (moved != null) {
                        logMovedOn(sagaId, record, moved, failure);
                        return moved;
                    }
                    break;
                }
                if (!envelopes.fitsWithoutState(records, failureRecord.envelope(), failureRecord.expectedVersion())) {
                    SagaSubscription.ReleaseResult stopped = quarantineAgainTooLarge(records, sagaId, record.key(), meta, current, null,
                            "the failure record of the event", failure, now);
                    if (stopped != null) {
                        return stopped;
                    }
                    continue;
                }
                if (!envelopes.compareAndSaveWithoutState(sagaId, failureRecord.envelope(), failureRecord.expectedVersion())) {
                    continue;
                }
                if (failureRecord.quarantined()) {
                    // The record stays, so the next release replays it first
                    log.error("Saga '{}' instance '{}' is QUARANTINED again, stopped on the event '{}' while it was releasing, which has been failing for {}, past its budget of {}. The inputs after it stay recorded, and a later release replays this event first.",
                            subscriptionId, sagaId, record.key(), Duration.between(Objects.requireNonNull(failureRecord.envelope().failure()).firstFailedAt(), now), quarantineAfter, failure);
                    return SagaSubscription.ReleaseResult.QUARANTINED_AGAIN;
                }
                break;
            }
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            if (storeFailure != failure) {
                failure.addSuppressed(storeFailure);
            }
        }
        log.warn("Saga '{}' instance '{}' failed on the event '{}' while it was releasing. It stays releasing, and the timer poller tries the event again.",
                subscriptionId, sagaId, record.key(), failure);
        return SagaSubscription.ReleaseResult.STILL_RELEASING;
    }

    // Record that reading a releasing instance and its first record failed against the budget of that record, the way a
    // failed replay is recorded. That covers a state a replay saved that doesn't decode any more, and a record whose
    // event doesn't. The instance is read again without its state and with only the seq and key of its first record.
    // The failure record names the input the way a failed replay names it when that record reads alone with its event,
    // and by its seq and key alone when it doesn't. An instance with no record left has nothing more to replay, so its quarantine ends with the stored state
    // left as it is, and a state that doesn't decode fails the next input the way it fails any input of an active
    // instance. When the read by seq and key fails too, the store read nothing to charge and the failure propagates as
    // it is, and the next poll tries again. Null when the write that ends the quarantine lost, and the caller reads again
    private SagaSubscription.@Nullable ReleaseResult recordReadFailure(SagaStateStoreSkippedInputs records, String sagaId, Throwable readFailure) {
        SagaEnvelopes.WithFirstSkippedInputKey<S> read = null;
        try {
            read = envelopes.findWithFirstSkippedInputKey(records, sagaId).orElse(null);
        } catch (Throwable secondFailure) {
            rethrowIfNotTheInstances(secondFailure);
            readFailure.addSuppressed(secondFailure);
            rethrowAsIs(readFailure);
        }
        SagaEnvelope<S> current = read == null ? null : read.envelope();
        SagaSubscription.ReleaseResult ended = resultUnlessReleasing(current);
        if (ended != null) {
            log.info("Saga '{}' failed to read instance '{}' while it was releasing, but another call or node moved the release on and it is {} now.",
                    subscriptionId, sagaId, ended, readFailure);
            return ended;
        }
        // SagaCodec refuses to read a releasing instance without a quarantine id
        String quarantineId = Objects.requireNonNull(Objects.requireNonNull(current).quarantineId(), "quarantineId");
        RecordKey first = Objects.requireNonNull(read).first();
        if (first == null) {
            log.warn("Saga '{}' failed to read instance '{}' while it was releasing, with no recorded input left to replay. Its quarantine ends with the stored state left as it is.",
                    subscriptionId, sagaId, readFailure);
            return endQuarantine(records, sagaId, current, quarantineId, clock.get());
        }
        return recordReleaseFailure(records, sagaId, quarantineId, first, metaOfRecord(records, sagaId, quarantineId, first, readFailure), readFailure, clock.get());
    }

    // End the quarantine of current, which the read that returned it found with no record left, and leave the stored
    // state as it is, so a state that no longer decodes doesn't keep a release from ending. Null when the write lost, and
    // the caller reads again
    private SagaSubscription.@Nullable ReleaseResult endQuarantine(SagaStateStoreSkippedInputs records, String sagaId, SagaEnvelope<S> current, String quarantineId, Instant now) {
        SagaEnvelope<S> released = SagaExecutionSupport.withReleased(current, now, config.reorderWindow());
        if (!envelopes.fitsWithoutState(records, released, current.version())) {
            return writesNothing(records, sagaId, null, "the instance released");
        }
        if (!envelopes.compareAndSaveQuarantineEnded(records, released, current.version(), quarantineId)) {
            return null;
        }
        log.info("Saga '{}' instance '{}' is released and ACTIVE again.", subscriptionId, sagaId);
        return SagaSubscription.ReleaseResult.RELEASED;
    }

    // The delivery metadata of the record first, read off its event when the record reads with it, and otherwise built
    // from its key, which names the stream id and version, or the CloudEvent id and source, its budget is counted under.
    // The event type and the position are known only from the event, so a record whose event doesn't decode is counted
    // without them. The record is read alone, without the instance, so a state that doesn't decode leaves the event to
    // name the input, and a throw from the read only falls back to the key
    private EventMeta metaOfRecord(SagaStateStoreSkippedInputs records, String sagaId, String quarantineId, RecordKey first, Throwable readFailure) {
        try {
            SkippedInput read = records.findSkippedInputs(sagaId, quarantineId, 1).stream().findFirst().orElse(null);
            if (read != null && isSameRecord(RecordKey.of(read), first)) {
                return extractMeta(read.event()).meta();
            }
        } catch (Throwable recordFailure) {
            rethrowIfNotTheInstances(recordFailure);
            if (recordFailure != readFailure) {
                readFailure.addSuppressed(recordFailure);
            }
        }
        SkippedInputKey key;
        try {
            key = SkippedInputKey.parse(first.key());
        } catch (IllegalArgumentException notAKeyTheSagaWrote) {
            // A key the saga never writes names no input to count against, as for a store that read nothing
            readFailure.addSuppressed(notAKeyTheSagaWrote);
            rethrowAsIs(readFailure);
            throw notAKeyTheSagaWrote;
        }
        return switch (key) {
            case SkippedInputKey.StreamKey stream -> new EventMeta(stream.streamId(), stream.streamVersion(), null, null);
            case SkippedInputKey.CloudEventKey cloudEvent -> new EventMeta(null, null, null, null, cloudEvent.id(), cloudEvent.source());
        };
    }

    // Matched by seq and key, not by the event, since a store reads a record back as a new event and a byte array in it
    // never equals the one read before
    private static boolean isSameRecord(@Nullable RecordKey first, RecordKey record) {
        return first != null && first.equals(record);
    }

    private static boolean isSameRecord(@Nullable SkippedInput first, SkippedInput record) {
        return first != null && isSameRecord(RecordKey.of(first), RecordKey.of(record));
    }

    private void logMovedOn(String sagaId, RecordKey record, SagaSubscription.ReleaseResult result, Throwable failure) {
        log.info("Saga '{}' instance '{}' failed on the event '{}' while it was releasing, but another call or node moved the release on and it is {} now.",
                subscriptionId, sagaId, record.key(), result, failure);
    }

    /**
     * Make a timer whose fire threw due again one millisecond after {@code polledAt}, so later polls order the instance
     * behind every instance that was due at {@code polledAt}, including one due at exactly that instant. Without it a
     * batch of instances whose timers keep failing is returned first on every poll and the saga's other instances never
     * get their timers fired.
     * <p>
     * Read without the state and written without it, so an instance whose state no longer decodes is marked too. A lost
     * compare-and-set is read again and retried up to {@link SagaRunnerConfig#maxCasAttempts()} times, and not at all
     * when the read finds the version the write lost at, since only a store that refuses a save at the same version
     * loses that way. The write keeps
     * the instance's version, so a slow event delivery that read the instance before the mark still saves. When the
     * write cannot be made, the timer keeps the due time it had and the next poll tries it as early as before. Nothing is
     * thrown, because the exception that matters is the one the fire threw.
     */
    private void markFailedAttempt(String sagaId, TimerEntry timer, Instant polledAt) {
        Instant nextAttemptAt = polledAt.plusMillis(1);
        try {
            @Nullable Long lostAtVersion = null;
            for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
                SagaEnvelope<S> current = envelopes.findWithoutState(sagaId).orElse(null);
                // Lost at a version that is still stored, so nothing changed it and the store refused a save at the same
                // version. Every further attempt would be refused too
                if (current != null && lostAtVersion != null && current.version() == lostAtVersion) {
                    log.warn("Saga '{}' instance '{}' could not record that its timer '{}' failed, because the state store refuses a save "
                             + "that keeps the version. The timer keeps its due time, so the next poll tries it before instances that "
                             + "became due after it.",
                            subscriptionId, sagaId, timer.name());
                    return;
                }
                SagaEnvelope<S> marked = SagaExecutionSupport.withFailedTimerAttempt(current, timer, nextAttemptAt);
                if (current == null || marked == null || envelopes.compareAndSaveWithoutState(sagaId, marked, current.version())) {
                    return;
                }
                lostAtVersion = current.version();
            }
            log.warn("Saga '{}' instance '{}' could not record that its timer '{}' failed, because the instance kept changing under {} attempts. "
                     + "The timer keeps its due time, so the next poll tries it before instances that became due after it.",
                    subscriptionId, sagaId, timer.name(), config.maxCasAttempts());
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            log.warn("Saga '{}' instance '{}' could not record that its timer '{}' failed, because the state store failed. "
                     + "The timer keeps its due time, so the next poll tries it before instances that became due after it.",
                    subscriptionId, sagaId, timer.name(), storeFailure);
        }
    }

    private void process(String sagaId, SagaInput<E> input, EventMeta meta, @Nullable CloudEvent cloudEvent, @Nullable String requireTimerName) {
        // A lost compare-and-set is a retryable concurrency conflict: signal it with CasConflict so casRetry retries the
        // whole body, and map an exhausted retry to the public SagaConcurrencyException. Any other failure (a throwing
        // dispatcher or store) is not a CasConflict, so retryIf leaves it alone and it propagates on the first attempt.
        // The commands the attempts that lost their compare-and-set dispatched, so a refusal names them
        AtomicInteger dispatchedByEarlierAttempts = new AtomicInteger();
        casRetry
                .mapError(error -> error instanceof CasConflict
                        ? new SagaConcurrencyException("Failed to save saga '" + sagaId + "' after " + config.maxCasAttempts() + " attempts due to concurrent modification")
                        : error)
                .execute((RetryInfo attempt) -> {
                    Instant now = clock.get();
                    SagaEnvelope<S> current;
                    try {
                        current = envelopes.find(sagaId).orElse(null);
                    } catch (Throwable loadFailure) {
                        // Asked before the skip, because a skip returns normally and that acknowledges the event. A
                        // load that failed because the process is out of heap says nothing about whether this input
                        // would have been skipped, and answering yes on that basis would acknowledge it silently.
                        rethrowIfNotTheInstances(loadFailure);
                        if (wouldHaveSkippedThisInput(sagaId, meta, cloudEvent, loadFailure)) {
                            return null;
                        }
                        throw loadFailure;
                    }
                    if (requireTimerName != null && !hasDueTimer(current, requireTimerName, now)) {
                        return null; // stale/superseded/rescheduled timer, or the instance completed: nothing to fire.
                    }
                    Outcome<S, C> outcome = SagaExecutionSupport.process(saga, sagaId, current, input, meta, now, config.reorderWindow());
                    if (!outcome.processed()) {
                        SkipToRecord toRecord = outcome.skipToRecord();
                        if (toRecord == null) {
                            warnIfBehindTheReorderWindow(sagaId, current, meta);
                            return null;
                        }
                        // A quarantined instance recording the input it skips. A lost write is retried like any other
                        // save, deciding again on what it reads, and one that never lands propagates, so the input is
                        // not skipped unrecorded.
                        if (recordedSkip(Objects.requireNonNull(current), toRecord.of(Objects.requireNonNull(cloudEvent, "cloudEvent")))) {
                            return null;
                        }
                        throw new CasConflict();
                    }
                    // Encoded once, so the store is asked about the bytes the save then writes. Asked before the dispatch,
                    // so an attempt whose instance the store cannot write dispatches nothing and the input fails like any
                    // other. An earlier attempt that lost its compare-and-set has dispatched its commands already
                    SagaEnvelope<S> envelope = outcome.envelope();
                    StoredSaga write = envelope == null ? null : envelopes.encode(sagaId, envelope, outcome.expectedVersion());
                    SagaStateStoreSkippedInputs records = skippedInputs;
                    if (write != null && records != null && !envelopes.fits(records, write)) {
                        throw new IllegalStateException("Saga '" + subscriptionId + "' instance '" + sagaId + "' cannot apply " + inputName(meta, cloudEvent, requireTimerName)
                                                        + ", because its state store " + records.getClass().getName() + " cannot write the instance with it applied. "
                                                        + dispatchedBefore(attempt.getAttemptNumber(), dispatchedByEarlierAttempts.get()));
                    }
                    // Dispatch before saving so a command is never lost. A lost compare-and-set retries this whole body,
                    // re-dispatching the entire command list (at-least-once, up to maxCasAttempts times), so receivers
                    // must be idempotent and tolerate that multiplicity.
                    dispatcher.dispatchAll(outcome.commands());
                    dispatchedByEarlierAttempts.addAndGet(outcome.commands().size());
                    if (write != null && envelopes.compareAndSave(write, outcome.expectedVersion())) {
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
    // Asked through findWithoutState, which never decodes the state, so the state that failed to load isn't read again
    // here.
    private boolean wouldHaveSkippedThisInput(String sagaId, EventMeta meta, @Nullable CloudEvent cloudEvent, Throwable loadFailure) {
        try {
            SagaEnvelope<S> withoutState = envelopes.findWithoutState(sagaId).orElse(null);
            if (withoutState == null) {
                return false;
            }
            if (withoutState.isCompleted()) {
                return true;
            }
            if (withoutState.isQuarantined()) {
                return recordedTheSkip(sagaId, withoutState, meta, cloudEvent);
            }
            if (!SagaExecutionSupport.isRedelivery(withoutState, meta, config.reorderWindow())) {
                return false;
            }
            warnIfBehindTheReorderWindow(sagaId, withoutState, meta);
            return true;
        } catch (Throwable secondFailure) {
            rethrowIfNotTheInstances(secondFailure);
            if (secondFailure != loadFailure) {
                loadFailure.addSuppressed(secondFailure);
            }
            return false;
        }
    }

    // A quarantined instance skips the input whether or not its state decodes, and records it like the skip in
    // SagaExecutionSupport.process does, through a write that leaves the undecodable state alone. True once the input is
    // recorded or needs no record. False when the write kept losing or the instance stopped being quarantined, so the
    // load failure propagates rather than the input being skipped unrecorded. Throws when the store cannot record the
    // skip, and the caller adds that to the load failure as suppressed.
    private boolean recordedTheSkip(String sagaId, SagaEnvelope<S> quarantined, EventMeta meta, @Nullable CloudEvent cloudEvent) {
        SagaEnvelope<S> current = quarantined;
        for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
            if (current.isCompleted()) {
                return true;
            }
            if (!current.isQuarantined()) {
                return false;
            }
            SkipToRecord toRecord = SagaExecutionSupport.skipToRecord(current, meta, config.reorderWindow());
            if (toRecord == null) {
                warnIfBehindTheReorderWindow(sagaId, current, meta);
                return true;
            }
            if (recordedSkip(current, toRecord.of(Objects.requireNonNull(cloudEvent, "cloudEvent")))) {
                return true;
            }
            SagaEnvelope<S> reread = envelopes.findWithoutState(sagaId).orElse(null);
            if (reread == null) {
                return false;
            }
            current = reread;
        }
        return false;
    }

    // The skip of an input by the quarantined instance current, in one write that leaves the state alone. It raises the
    // skip count, which takes toRecord's seq for it alone, and records toRecord under the quarantine id. A key recorded
    // already keeps its first record, so its new seq goes unused. False when the write lost, and the caller decides
    // again on what it reads. Throws when the store cannot record the skip, so the input is not acknowledged
    private boolean recordedSkip(SagaEnvelope<S> current, SkippedInput toRecord) {
        if (cannotRecordSkips) {
            throw new IllegalStateException(("Saga '%s' instance '%s' is quarantined and cannot skip the input '%s', because its state store cannot write a skip and its record in one atomic write, "
                                             + "and a skip it does not record is never replayed by a release. The input is not acknowledged, so it is delivered again. A subscription that delivers an event again before any later one, as a MongoDB subscription does, holds up every later event meanwhile, for every instance of the saga. "
                                             + "Move the saga's data back to a store that records skips, such as MongoDB on a replica set, and release the instance there, or resolve the instance by hand, such as by deleting it from the database.")
                    .formatted(subscriptionId, current.sagaId(), toRecord.key()));
        }
        SagaStateStoreSkippedInputs records = skippedInputs(current.sagaId());
        String quarantineId = Objects.requireNonNull(current.quarantineId());
        return envelopes.compareAndSaveSkipped(records, SagaExecutionSupport.withSkipCounted(current), current.version(), quarantineId, toRecord);
    }

    // The store's answer to requireAtomicTransitions, whose contract is to throw IllegalStateException when it cannot
    private static boolean writesAtomically(SagaStateStoreSkippedInputs records) {
        try {
            records.requireAtomicTransitions();
            return true;
        } catch (IllegalStateException cannot) {
            return false;
        }
    }

    // A skipped stream event further behind than the reorder window cannot be told from a redelivery, and a late arrival
    // is the case where skipping it loses the event, so it is said once per stream rather than skipped silently. A
    // quarantined instance records every other input it skips, so the skip it leaves unrecorded is said too. One
    // quarantined without a quarantine id records nothing and skips every input, so being behind is not why it skipped.
    private void warnIfBehindTheReorderWindow(String sagaId, @Nullable SagaEnvelope<S> current, EventMeta meta) {
        if (current == null || current.isCompleted() || (current.isQuarantined() && current.quarantineId() == null)
            || SagaExecutionSupport.classify(current, meta, config.reorderWindow()) != SagaExecutionSupport.Delivery.BEHIND_WINDOW) {
            return;
        }
        if (behindWindowWarned.add(List.of(sagaId, meta.streamId()))) {
            log.warn("Saga '{}' skipped version {} of stream '{}', event type '{}', because it is further behind than the instance remembers, which is the reorder window of {} or a smaller window it used earlier. It is either a redelivery or an event that arrived later than that, and the saga cannot tell which. Raise reorderWindow on the SagaRunnerConfig if the feed reorders further than that. A raised window does not bring back versions the instance has already forgotten. This is logged once per stream.",
                    sagaId, meta.streamVersion(), meta.streamId(), meta.eventType(), config.reorderWindow());
        }
    }

    // When one delivery started failing to reach an instance, when that was last logged, and when it was last offered.
    private record UnroutableDelivery(Instant firstFailedAt, Instant lastLoggedAt, Instant lastOfferedAt) {
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
        return envelope.timers().stream().anyMatch(timer -> timer.name().equals(timerName) && timer.dueAtEpochMilli() <= nowMillis);
    }

    // An event without a stream id and a stream version leaves nothing to compare a redelivery against, so the reaction
    // would run again and issue its commands again. A position does not help, because events can reach the saga in a
    // different order than their positions. Occurrent's own stored events always carry both extensions, so this means a
    // feed that dropped them on the way in, which it does for every event, not just this one. Under REQUIRED that is
    // refused rather than reacted to, so the throw reaches the subscription model.
    // Whether the event comes back until somebody looks is the model's own business. On a push feed the listener
    // decides, and on a broker source the choice is set with DeliveryFailurePolicy. Under BEST_EFFORT the duplication
    // is accepted knowingly, so the warning says so once per runner rather than once per event.
    private void refuseOrWarnIfRedeliveryCannotBeDetected(EventMeta meta) {
        if (meta.carriesRedeliveryKey()) {
            return;
        }
        if (config.redeliveryDetection() == RedeliveryDetection.REQUIRED) {
            throw new SagaRedeliveryDetectionException(
                    "Saga subscription '" + subscriptionId + "' received an event without both a streamid and a streamversion. " +
                    "A redelivered event cannot be recognised as one, and a position does not help because events can arrive out of " +
                    "position order, so reacting to it would run the reaction again and issue its commands again. Forward the " +
                    "Occurrent CloudEvent extensions streamid and streamversion from the listener feeding this saga, or " +
                    "set redeliveryDetection to BEST_EFFORT if the feed carries no stream key and every reaction is idempotent.");
        }
        if (dedupUnavailableWarningLogged.compareAndSet(false, true)) {
            log.warn("Saga subscription '{}' received an event without both a streamid and a streamversion, and " +
                     "redeliveryDetection is BEST_EFFORT. A redelivered event cannot be recognised as one, so it will run the " +
                     "reaction again and issue its commands again.", subscriptionId);
        }
    }

    // A stream key extension that is present and cannot be read is refused under both settings. BEST_EFFORT accepts a
    // feed that carries no key, and this feed wrote one that nothing can compare, so reacting to the event would hide
    // that behind duplicate commands. Checked after routing like the refusal above, so an event that belongs to no
    // instance of this saga is still ignored. Never quarantined, because with no key there is nothing to record a
    // failure against.
    private void refuseIfStreamKeyIsUnreadable(ExtractedMeta extracted) {
        UnreadableExtension unreadable = extracted.unreadableStreamKey();
        if (unreadable == null) {
            return;
        }
        throw new SagaRedeliveryDetectionException(
                "Saga subscription '" + subscriptionId + "' received an event whose '" + unreadable.name() + "' extension " +
                "could not be read, so the saga cannot tell whether the event is a redelivery. Reacting to it would run the " +
                "reaction again and issue its commands again on every redelivery. Fix the feed that writes the extension. " +
                "redeliveryDetection does not apply, because the event does carry a stream key.", unreadable.cause());
    }

    // The delivery metadata, and the first stream key extension that was present and unreadable, if there was one.
    private record ExtractedMeta(EventMeta meta, @Nullable UnreadableExtension unreadableStreamKey) {
    }

    private record UnreadableExtension(String name, RuntimeException cause) {
    }

    // Each extension is read on its own, because one that cannot be read must not take the others with it. An event
    // carrying a good streamid and streamversion beside a position that is not a number has a redelivery key, and
    // reading the three together threw before either of the first two reached the record, so the event looked like one
    // with no key at all, which nothing can budget.
    // An unreadable stream key extension is handed back rather than read as absent, because an event with a bad
    // streamversion would otherwise be handled as one that carries no key.
    private ExtractedMeta extractMeta(CloudEvent cloudEvent) {
        Set<String> extensions = cloudEvent.getExtensionNames();
        List<UnreadableExtension> unreadableStreamKey = new ArrayList<>();
        String streamId = readStreamKey(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_ID, OccurrentExtensionGetter::getStreamId, unreadableStreamKey);
        Long streamVersion = readStreamKey(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_VERSION, OccurrentExtensionGetter::getStreamVersion, unreadableStreamKey);
        // Use the framework's own position accessor, which accepts a Number or String, rather than narrowing to Long.
        Long position = readPosition(cloudEvent, extensions);
        return new ExtractedMeta(new EventMeta(streamId, streamVersion, position, cloudEvent.getType(), cloudEvent.getId(), cloudEvent.getSource() == null ? null : cloudEvent.getSource().toString()), unreadableStreamKey.isEmpty() ? null : unreadableStreamKey.getFirst());
    }

    // RuntimeException rather than Throwable on purpose. An unreadable extension is a fact about the event rather than
    // a failure to deliver it, and keeping the catch narrow is what lets a failure of the process reach the delivery's
    // own catch.
    private <T> @Nullable T readStreamKey(CloudEvent cloudEvent, Set<String> extensions, String name, Function<CloudEvent, @Nullable T> read,
                                          List<UnreadableExtension> unreadable) {
        if (!extensions.contains(name)) {
            return null;
        }
        try {
            return read.apply(cloudEvent);
        } catch (RuntimeException e) {
            unreadable.add(new UnreadableExtension(name, e));
            return null;
        }
    }

    // The position does not identify the event, so an unreadable one is treated as absent and said once per runner
    // rather than per event, because a feed writing one badly writes every one of them badly. It still goes on
    // SagaFailure.position() and the event metadata when it can be read.
    private @Nullable Long readPosition(CloudEvent cloudEvent, Set<String> extensions) {
        if (!extensions.contains(OccurrentCloudEventExtension.POSITION)) {
            return null;
        }
        try {
            return OccurrentCloudEventExtension.getPosition(cloudEvent);
        } catch (RuntimeException e) {
            if (unreadableExtensionsWarned.add(OccurrentCloudEventExtension.POSITION)) {
                log.warn("Saga subscription '{}' received an event whose '{}' extension could not be read, so it is treated as absent. The event is still recognised as a redelivery from its streamid with its streamversion.",
                        subscriptionId, OccurrentCloudEventExtension.POSITION, e);
            }
            return null;
        }
    }
}
