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
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Delivery;
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
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Drives one saga against one subscription and its own timer poller: it loads the instance, runs the pure
 * {@link SagaExecutionSupport} step, dispatches commands before saving (at-least-once), and retries a lost compare-and-set
 * save. Timeouts re-enter the same path, fenced so a timer no longer present on the (reloaded) envelope is skipped.
 * <p>
 * An instance that keeps failing can be quarantined rather than left to fail for as long as the subscription model
 * offers the event again. Where a quarantine budget is in force, its first failing event can write down when the
 * failing started, and it rethrows whether or not that write succeeds, which is what every version up to 0.33.0 did. Where nothing was recorded, the next delivery
 * decides on whatever the store holds then. Whether a later failure is considered for quarantine, and whether a
 * considered one is then quarantined, are separate conditions, and
 * {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} lists both. When the instance is quarantined, this class
 * returns normally instead of rethrowing.
 * <p>
 * Every step from reading the CloudEvent to saving the result runs inside one {@code try} that catches
 * {@link Throwable}, so once an event has reached an instance, what failed and where it was thrown decide nothing
 * about its budget. A delivery that fails before the saga can say which instance it
 * belongs to has no instance to charge and no instance to hold it, so it is never let past. It is refused on every
 * redelivery. See
 * {@link #letTheSubscriptionPast} for the conditions and
 * {@link #refuseUnroutableDelivery} for why the unroutable delivery is the exception.
 * <p>
 * The budget is the instance's rather than one event's. An instance where two events both fail keeps the instant it
 * started failing, so a second event can reach the budget on its first failure, and the record names the event the
 * instance stopped on rather than the one the clock started with.
 * <p>
 * The quarantine decision reads and writes the instance without its application state, through
 * {@link SagaStateStore#findWithoutState} and {@link SagaStateStore#compareAndSaveWithoutState}. An instance whose state
 * no longer decodes is one of the instances most in need of quarantining, so deciding on a read that throws on it would
 * leave it failing on every delivery without ever reaching its budget. A store that only reads an instance whole keeps that behaviour, because nothing
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
    static final Duration UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE = Duration.ofMinutes(5);

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
    // Where a failed version the instance cannot list as pending is kept instead, absent for a store that cannot hold
    // events.
    private final @Nullable SagaHeldEvents<S> heldEvents;
    // Whether the delivery in progress may hold events, asked on the delivering thread. SagaRunner decides it, and a
    // feed that does not declare commit order holds.
    private final BooleanSupplier holds;

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable) {
        this(subscriptionId, saga, stateStore, dispatcher, converter, config, stillObtainable, Instant::now);
    }

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable,
                  Supplier<Instant> clock) {
        this(subscriptionId, saga, stateStore, dispatcher, converter, config, stillObtainable, clock, () -> true);
    }

    SagaExecution(String subscriptionId, Saga<E, S, C> saga, SagaStateStore<S> stateStore, CommandDispatcher<C> dispatcher,
                  CloudEventConverter<E> converter, SagaRunnerConfig config, Predicate<CloudEvent> stillObtainable,
                  Supplier<Instant> clock, BooleanSupplier holds) {
        this.heldEvents = heldEventsOf(stateStore);
        this.holds = holds;
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
        // The redelivery keys this delivery saw as new and has not yet seen applied. A local rather than a field,
        // because deliveries on other threads share this object, and a set rather than a flag, because a flag raised
        // by one key would let another key be taken as overtaken.
        Set<String> keysSeenNew = new HashSet<>();
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
            process(sagaId, SagaInput.event(event, EventMetadata.from(cloudEvent)), meta, null, keysSeenNew);
            refusalAnnounced.remove(sagaId);
        } catch (Throwable failure) {
            if (!letTheSubscriptionPast(sagaId, cloudEvent, meta, failure, keysSeenNew)) {
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
    private boolean letTheSubscriptionPast(@Nullable String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Set<String> keysSeenNew) {
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
            recordPending(sagaId, cloudEvent, meta, failure, keysSeenNew);
            return false;
        }
        return quarantine(sagaId, cloudEvent, meta, failure, quarantineAfter.get(), keysSeenNew);
    }

    /**
     * List the stream version of the failed delivery as pending on the instance, without a failure record, so quarantine
     * never counts it. Written without the state like the failure record is, and never throws what the store throws,
     * because the exception that matters is the one that made the delivery fail.
     * <p>
     * An input that is pending already costs one read and no write, so a source that offers a failing event again every
     * few milliseconds reads the instance each time, as the quarantine path does. A lost compare-and-set is read again and retried up to {@link SagaRunnerConfig#maxCasAttempts()} times. A write
     * that cannot be made, and a version the instance cannot list, is kept by {@link #keepUnlisted} instead.
     */
    private void recordPending(String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Set<String> keysSeenNew) {
        try {
            for (int attempt = 1; attempt <= config.maxCasAttempts(); attempt++) {
                SagaEnvelope<S> current = stateStore.findWithoutState(sagaId).orElse(null);
                Unlisted unlisted = unlisted(current, meta, keysSeenNew);
                if (unlisted != null) {
                    keepUnlisted(sagaId, cloudEvent, meta, failure, unlisted, null);
                    return;
                }
                FailureRecord<S> record = SagaExecutionSupport.pendingRecord(saga, sagaId, current, meta, clock.get(), config.reorderWindow());
                if (record == null || stateStore.compareAndSaveWithoutState(sagaId, record.envelope(), record.expectedVersion())) {
                    return;
                }
            }
            keepUnlisted(sagaId, cloudEvent, meta, failure, Unlisted.KEPT_CHANGING, null);
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            keepUnlisted(sagaId, cloudEvent, meta, failure, Unlisted.STORE_FAILED, storeFailure);
        }
    }

    // Why a failed version that the read cannot list as pending still needs keeping, or null when the read lists it,
    // or would, or the instance does not need it. Asked of the same read the listing uses, because a null pending
    // record means a redelivery, a completed or quarantined instance and a version overtaken in flight alike, and only
    // the last of those is a loss.
    private @Nullable Unlisted unlisted(@Nullable SagaEnvelope<S> current, EventMeta meta, Set<String> keysSeenNew) {
        if (current == null || current.isCompleted() || current.isQuarantined() || !meta.carriesRedeliveryKey()) {
            return null;
        }
        Delivery delivery = SagaExecutionSupport.classify(current, meta, config.reorderWindow());
        if (delivery == Delivery.BEHIND_WINDOW) {
            return keysSeenNew.contains(meta.redeliveryKey()) ? Unlisted.OVERTAKEN : null;
        }
        if (delivery == Delivery.NEW && !SagaExecutionSupport.listsPending(current, meta) && SagaExecutionSupport.isAtPendingCap(current)) {
            return Unlisted.AT_CAP;
        }
        return null;
    }

    // The ways a failed version can end up not listed as pending.
    private enum Unlisted {
        AT_CAP, KEPT_CHANGING, STORE_FAILED, OVERTAKEN
    }

    /**
     * Keep a failed version the instance could not list as pending, so that it is not acknowledged unapplied. Where the
     * delivery may hold events, it is self-held. Anywhere else it is logged at ERROR, and the original failure is
     * rethrown either way, so the source retries it in place, which is what a delivery in commit order does. Never
     * throws what the store throws.
     */
    private void keepUnlisted(String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Unlisted unlisted, @Nullable Throwable storeFailure) {
        if (storeFailure != null && storeFailure != failure) {
            failure.addSuppressed(storeFailure);
        }
        SagaHeldEvents<S> heldEvents = this.heldEvents;
        if (heldEvents != null && holds.getAsBoolean()) {
            selfHold(heldEvents, sagaId, cloudEvent, meta, failure);
            return;
        }
        String key = meta.redeliveryKey();
        String skipped = "If the source skips the event and the instance moves " + config.reorderWindow() + " versions of its type past it, a replay of it is skipped as behind the reorder window.";
        switch (unlisted) {
            case AT_CAP -> log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the instance already lists {}, the most it lists. {}",
                    subscriptionId, sagaId, key, SagaExecutionSupport.PENDING_CAP, skipped);
            case KEPT_CHANGING -> log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the instance kept changing under {} attempts. {}",
                    subscriptionId, sagaId, key, config.maxCasAttempts(), skipped);
            case STORE_FAILED -> log.error("Saga '{}' instance '{}' failed on the event '{}' and could not list it as pending, because the state store failed. {}",
                    subscriptionId, sagaId, key, skipped, storeFailure);
            case OVERTAKEN -> log.error("Saga '{}' instance '{}' failed on the event '{}', which this delivery saw as new, and the instance now has it behind the reorder window, so it could not list it as pending. "
                                        + "Either another delivery applied it, or the subscription model delivered later events of its stream and type before the failed event was handled, which a model that says it delivers in commit order must not do. "
                                        + "The event is rethrown, and a redelivery of it is skipped as behind the reorder window.",
                    subscriptionId, sagaId, key);
        }
    }

    /**
     * Keep the delivery's own failed event on the instance, as a self-held key on the envelope and a held copy beside
     * it, so a later input applies it however far behind the reorder window it is by then. The original failure is
     * rethrown by the caller, so the event is never acknowledged.
     * <p>
     * The mark is written to any active started instance whatever its version, and what it found decides. When that proves
     * the event applied, or lists it pending, the key comes off again and nothing is held. Otherwise the key stays,
     * because nothing proves the event applied, and keeping it can apply an event twice where dropping it can lose one.
     */
    private void selfHold(SagaHeldEvents<S> heldEvents, String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure) {
        String key = Objects.requireNonNull(meta.redeliveryKey());
        try {
            Optional<SagaEnvelope<S>> marked;
            try {
                marked = heldEvents.markHeldEvents(sagaId, key);
            } catch (SagaHeldEvents.RefusedForSizeException refused) {
                selfHoldAtTheSizeLimit(heldEvents, sagaId, cloudEvent, meta, refused);
                return;
            }
            if (marked.isEmpty()) {
                // No active started instance. A start that failed is redelivered and runs onStart, and a completed or
                // quarantined instance skips the event, as it did before.
                return;
            }
            // Decided without the key, which a concurrent self-hold of the same event, or a store's retry of a mark that
            // was written but not answered, may have added already.
            Set<String> others = new HashSet<>(marked.get().selfHeldKeys());
            others.remove(key);
            SagaEnvelope<S> before = SagaExecutionSupport.withSelfHeldKeys(marked.get(), others);
            if (SagaExecutionSupport.provesApplied(before, meta) || SagaExecutionSupport.listsPending(before, meta)) {
                unmarkSelfHeld(heldEvents, sagaId, key);
                return;
            }
            warnIfKeptBehindTheReorderWindow(sagaId, before, meta);
            heldEvents.hold(sagaId, heldEvent(sagaId, cloudEvent, meta, true, false));
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            logCouldNotKeep(sagaId, meta, failure, storeFailure);
        }
    }

    // The fallback for an instance at the store's document limit, where adding the key grows the document and is
    // refused. The marker that keeps the event outstanding moves to the held copy, whose flag is set in place and never
    // grows anything. Every mark here only unsets a flag and raises the version, so the store accepts it at the limit too.
    private void selfHoldAtTheSizeLimit(SagaHeldEvents<S> heldEvents, String sagaId, CloudEvent cloudEvent, EventMeta meta, RuntimeException refused) {
        Optional<SagaEnvelope<S>> marked = heldEvents.markHeldEvents(sagaId, null);
        if (marked.isEmpty()) {
            return;
        }
        if (SagaExecutionSupport.provesApplied(marked.get(), meta) || SagaExecutionSupport.listsPending(marked.get(), meta)) {
            return;
        }
        warnIfKeptBehindTheReorderWindow(sagaId, marked.get(), meta);
        if (!heldEvents.hold(sagaId, heldEvent(sagaId, cloudEvent, meta, false, true))) {
            heldEvents.markOutstanding(sagaId, Objects.requireNonNull(meta.redeliveryKey()));
        }
        // Again, because a drain that ended between the first mark and the copy would otherwise leave no drain due.
        heldEvents.markHeldEvents(sagaId, null);
        log.error("Saga '{}' instance '{}' failed on the event '{}' and has reached the state store's document size limit, so the event is kept on its held copy instead of on the instance. "
                  + "The instance refuses every input that would grow it until it is made smaller.",
                subscriptionId, sagaId, meta.redeliveryKey(), refused);
    }

    private void unmarkSelfHeld(SagaHeldEvents<S> heldEvents, String sagaId, String key) {
        try {
            heldEvents.unmarkSelfHeld(sagaId, key);
        } catch (Throwable storeFailure) {
            rethrowIfNotTheInstances(storeFailure);
            log.warn("Saga '{}' instance '{}' could not take the key '{}' off its self-held keys. A later save that applies the event, or proves it applied, takes it off instead.",
                    subscriptionId, sagaId, key, storeFailure);
        }
    }

    private void logCouldNotKeep(String sagaId, EventMeta meta, Throwable failure, Throwable storeFailure) {
        log.error("Saga '{}' instance '{}' failed on the event '{}' and could not keep it, because the state store failed while holding it. "
                  + "If the source skips the event and the instance moves {} versions of its type past it, a replay of it is skipped as behind the reorder window.",
                subscriptionId, sagaId, meta.redeliveryKey(), config.reorderWindow(), storeFailure);
        if (storeFailure != failure) {
            failure.addSuppressed(storeFailure);
        }
    }

    private void warnIfKeptBehindTheReorderWindow(String sagaId, SagaEnvelope<S> before, EventMeta meta) {
        if (SagaExecutionSupport.classify(before, meta, config.reorderWindow()) == Delivery.BEHIND_WINDOW) {
            log.warn("Saga '{}' instance '{}' keeps the event '{}' to apply it again although it is behind the reorder window, because nothing on the instance proves it was applied. If it was applied, its reaction runs a second time.",
                    subscriptionId, sagaId, meta.redeliveryKey());
        }
    }

    private HeldEvent heldEvent(String sagaId, CloudEvent cloudEvent, EventMeta meta, boolean selfHeld, boolean outstanding) {
        HeldEventRef ref = new HeldEventRef(Objects.requireNonNull(meta.redeliveryKey()), meta.streamId(), meta.streamVersion(), meta.position(),
                cloudEvent.getType(), clock.get(), selfHeld, outstanding);
        return new HeldEvent(sagaId, ref, cloudEvent);
    }

    @SuppressWarnings("unchecked")
    private static <S extends @Nullable Object> @Nullable SagaHeldEvents<S> heldEventsOf(SagaStateStore<S> stateStore) {
        return stateStore instanceof SagaHeldEvents<?> heldEvents ? (SagaHeldEvents<S>) heldEvents : null;
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
     * instance to write on. Where the subscription offers it again, once the converter or the id extractor can read it
     * the event is applied in the order it was written, with nothing to feed again. What it holds up in the meantime is
     * decided by whatever feeds the subscription, and {@link org.occurrent.dsl.saga.SagaStatus#QUARANTINED} says what
     * that can be.
     * <p>
     * The first failure is a warning, and after that it is logged at ERROR once per {@link #unroutableErrorInterval()}
     * for as long as the event keeps being offered, rather than every time the source offers it. That interval is the
     * quarantine budget when one is configured, and a fixed five-minute default otherwise, so an operator alerting on
     * ERROR is paged on a subscription model this saga cannot quarantine anything on, for as long as that model keeps
     * offering the event. A model that does not offer a refused delivery again gets only the first WARN.
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
            log.warn("Saga '{}' could not work out which instance the event '{}' belongs to, so it refused the event rather than acknowledging it, because an event the saga cannot route may still belong to an instance and acknowledging it would lose it. Where the subscription offers the event again, it is applied in the order it was written once the converter or the id extractor can read it. What it holds up until then is decided by whatever feeds this subscription. Whether it is offered again is up to the subscription model, and DeliveryFailurePolicy is where a broker source's choice is configured.",
                    subscriptionId, redeliveryKey, failure);
            return;
        }
        Duration interval = unroutableErrorInterval();
        if (Duration.between(existing.lastLoggedAt(), now).compareTo(interval) < 0
            || !unroutableDeliveries.replace(redeliveryKey, existing, new UnroutableDelivery(existing.firstFailedAt(), now))) {
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
     * the model keeps offering the event. That is every saga under the default configuration, and every saga on a
     * subscription model {@code SagaRunner} switches quarantine off for, such as a push feed, including one a broker
     * source feeds. A model that does not offer a refused delivery again gets only the first WARN either way. Using
     * the configured budget when there is one keeps one number for an operator to reason about, and
     * {@link #UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE} otherwise gives every other saga the same five-minute
     * cadence rather than inventing a second tunable for it.
     */
    Duration unroutableErrorInterval() {
        return config.quarantineAfter().orElse(UNROUTABLE_ERROR_INTERVAL_WITHOUT_QUARANTINE);
    }

    /**
     * Record that this event failed for this instance, and answer whether that ended the instance's time budget. A true
     * answer means the caller returns normally. A false answer means the exception propagates exactly as it always has.
     * <p>
     * The failure is a {@link Throwable} because the instance is stuck on it either way. An {@code Error} out of a
     * reaction stops the instance making progress exactly as a {@code RuntimeException} does, and it costs the saga's
     * other instances exactly the same.
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
    private boolean quarantine(String sagaId, CloudEvent cloudEvent, EventMeta meta, Throwable failure, Duration quarantineAfter, Set<String> keysSeenNew) {
        try {
            Instant now = clock.get();
            // Read without the state, because nothing on this path applies it and an instance whose state no longer
            // decodes is the one that most needs to reach its budget. Loading it whole threw, the catch below swallowed
            // that, and no failure record was ever written.
            SagaEnvelope<S> current = stateStore.findWithoutState(sagaId).orElse(null);
            Unlisted unlisted = unlisted(current, meta, keysSeenNew);
            if (unlisted != null) {
                keepUnlisted(sagaId, cloudEvent, meta, failure, unlisted, null);
                return false;
            }
            FailureRecord<S> record = SagaExecutionSupport.onFailure(saga, sagaId, current, meta, failure, now, quarantineAfter, config.reorderWindow());
            if (record == null) {
                return false;
            }
            if (record.quarantined() && !confirmedStillObtainable(cloudEvent, failure)) {
                boolean firstRefusalForThisInstance = refusalAnnounced.add(sagaId);
                // The measured time rather than the budget. A redelivery can arrive well after the budget elapsed, so
                // reporting the budget as the elapsed time understates how long the instance has been stuck.
                Duration failingFor = failingFor(record, now);
                // Checked before the write, not after, because quarantining returns normally and that acknowledges the
                // event to whatever fed it. An unconfirmed answer is treated as a no, so the exception propagates as
                // it did before 0.34.0 and the instance goes on failing wherever the model offers the event again.
                // Nothing is saved, which leaves the failure record the earlier attempts wrote and lets the next
                // redelivery ask again. Retention is rechecked every time so a store coming back is noticed, while
                // the warning is said once per instance.
                if (firstRefusalForThisInstance) {
                    log.warn("Saga '{}' instance '{}' has been failing for {}, which is past its budget of {}, and is stopped on the event '{}', and it is not quarantined, because the subscription could not confirm that acknowledging the event is safe to do. Either acknowledging is what would drop the only copy of it, or the check could not be completed, and quarantining acknowledges the event. This delivery is rethrown instead. What the event holds up is decided by whatever feeds this subscription. https://github.com/johanhaleby/occurrent/issues/918 is the path to closing that.",
                            subscriptionId, sagaId, failingFor, quarantineAfter, meta.redeliveryKey(), failure);
                }
                recordPending(sagaId, cloudEvent, meta, failure, keysSeenNew);
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
                // The pending version has no such premise, it is true whatever the instance looks like now.
                recordPending(sagaId, cloudEvent, meta, failure, keysSeenNew);
                return false;
            }
            if (!record.quarantined()) {
                log.warn("Saga '{}' instance '{}' failed on the event '{}'. Whether the event is offered again is for whatever feeds this subscription to decide. The instance can be quarantined once it has been failing for {}, measured from when it started failing rather than from this event, but that is not the only condition, so read the instance's status to know whether it was.",
                        subscriptionId, sagaId, meta.redeliveryKey(), quarantineAfter, failure);
                return false;
            }
            log.error("Saga '{}' instance '{}' has been failing for {}, past its budget of {}, and is now QUARANTINED, stopped on the event '{}'. The time is how long the instance has been failing, which can be longer than this one event has. It skips every further event and fires no timers. Find it with findByStatus(QUARANTINED, ..).",
                    subscriptionId, sagaId, failingFor(record, now), quarantineAfter, meta.redeliveryKey(), failure);
            return true;
        } catch (Throwable storeFailure) {
            // What reaches here is the store being unreachable, or a store that can only read an instance whole
            // failing to decode one. Rethrowing the original is today's behaviour and it is the right one for both,
            // because a saga that cannot read its own instance cannot make progress on it either way. The retention
            // check no longer reaches here, because a check that cannot answer is a refused quarantine with a warning
            // rather than a silent one.
            rethrowIfNotTheInstances(storeFailure);
            // Only an attributable failure with a redelivery key gets here, and the version could not be listed as pending
            // either, which is what a replay long after depends on. keepUnlisted suppresses the store failure under the
            // original only when they differ, because Java refuses to suppress an exception under itself.
            keepUnlisted(sagaId, cloudEvent, meta, failure, Unlisted.STORE_FAILED, storeFailure);
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
                        process(envelope.sagaId(), SagaInput.timeout(envelope.sagaId(), TimerName.parse(timerName)), EventMeta.NONE, timerName, new HashSet<>());
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

    private void process(String sagaId, SagaInput<E> input, EventMeta meta, @Nullable String requireTimerName, Set<String> keysSeenNew) {
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
                        switch (wouldHaveSkippedThisInput(sagaId, meta, loadFailure, keysSeenNew)) {
                            case SKIPPED -> {
                                return null;
                            }
                            case OVERTAKEN -> {
                                OvertakenInFlight overtaken = new OvertakenInFlight(sagaId, meta);
                                overtaken.addSuppressed(loadFailure);
                                throw overtaken;
                            }
                            case NOT_SKIPPED -> throw loadFailure;
                        }
                        throw loadFailure;
                    }
                    if (requireTimerName != null && !hasDueTimer(current, requireTimerName, now)) {
                        return null; // stale/superseded/rescheduled timer, or the instance completed: nothing to fire.
                    }
                    Outcome<S, C> outcome = SagaExecutionSupport.process(saga, sagaId, current, input, meta, now, config.reorderWindow());
                    if (!outcome.processed()) {
                        // An event this delivery saw as new on an earlier attempt, whose save lost, and which other
                        // deliveries have since moved behind the reorder window. Skipping it would acknowledge an event
                        // this delivery never applied, so it fails instead and is kept on the failure path.
                        if (isOvertakenInFlight(current, meta, keysSeenNew)) {
                            throw new OvertakenInFlight(sagaId, meta);
                        }
                        warnIfBehindTheReorderWindow(sagaId, current, meta);
                        return null;
                    }
                    if (SagaExecutionSupport.isNewByPositionAlone(current, meta, config.reorderWindow())) {
                        keysSeenNew.add(Objects.requireNonNull(meta.redeliveryKey()));
                    }
                    // Dispatch before saving so a command is never lost. A lost compare-and-set retries this whole body,
                    // re-dispatching the entire command list (at-least-once, up to maxCasAttempts times), so receivers
                    // must be idempotent and tolerate that multiplicity.
                    dispatcher.dispatchAll(outcome.commands());
                    SagaEnvelope<S> envelope = outcome.envelope();
                    if (envelope != null && stateStore.compareAndSave(sagaId, envelope, outcome.expectedVersion())) {
                        keysSeenNew.remove(meta.redeliveryKey());
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
    // A third answer, overtaken, is an input this delivery saw as new on an earlier attempt and that the read now finds
    // behind the reorder window, which process answers the same way.
    private Skip wouldHaveSkippedThisInput(String sagaId, EventMeta meta, Throwable loadFailure, Set<String> keysSeenNew) {
        try {
            SagaEnvelope<S> withoutState = stateStore.findWithoutState(sagaId).orElse(null);
            if (withoutState != null && (withoutState.isCompleted() || withoutState.isQuarantined())) {
                return Skip.SKIPPED;
            }
            if (withoutState == null || !SagaExecutionSupport.isRedelivery(withoutState, meta, config.reorderWindow())) {
                if (SagaExecutionSupport.isNewByPositionAlone(withoutState, meta, config.reorderWindow())) {
                    keysSeenNew.add(Objects.requireNonNull(meta.redeliveryKey()));
                }
                return Skip.NOT_SKIPPED;
            }
            if (isOvertakenInFlight(withoutState, meta, keysSeenNew)) {
                return Skip.OVERTAKEN;
            }
            warnIfBehindTheReorderWindow(sagaId, withoutState, meta);
            return Skip.SKIPPED;
        } catch (Throwable secondFailure) {
            rethrowIfNotTheInstances(secondFailure);
            if (secondFailure != loadFailure) {
                loadFailure.addSuppressed(secondFailure);
            }
            return Skip.NOT_SKIPPED;
        }
    }

    private enum Skip {
        NOT_SKIPPED, SKIPPED, OVERTAKEN
    }

    private boolean isOvertakenInFlight(@Nullable SagaEnvelope<S> current, EventMeta meta, Set<String> keysSeenNew) {
        return current != null && !current.isCompleted() && !current.isQuarantined()
               && keysSeenNew.contains(meta.redeliveryKey())
               && SagaExecutionSupport.classify(current, meta, config.reorderWindow()) == Delivery.BEHIND_WINDOW;
    }

    // A skipped stream event further behind than the reorder window cannot be told from a redelivery, and a late arrival
    // is the case where skipping it loses the event, so it is said once per stream rather than skipped silently.
    private void warnIfBehindTheReorderWindow(String sagaId, @Nullable SagaEnvelope<S> current, EventMeta meta) {
        if (current == null || current.isCompleted() || current.isQuarantined()
            || SagaExecutionSupport.classify(current, meta, config.reorderWindow()) != SagaExecutionSupport.Delivery.BEHIND_WINDOW) {
            return;
        }
        if (behindWindowWarned.add(List.of(sagaId, meta.streamId()))) {
            log.warn("Saga '{}' skipped version {} of stream '{}', event type '{}', because it is further behind than the instance remembers, which is the reorder window of {} or a smaller window it used earlier. It is either a redelivery or an event that arrived later than that, and the saga cannot tell which. Raise reorderWindow on the SagaRunnerConfig if the feed reorders further than that. A raised window does not bring back versions the instance has already forgotten. This is logged once per stream.",
                    sagaId, meta.streamVersion(), meta.streamId(), meta.eventType(), config.reorderWindow());
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

    // A stream event this delivery saw as new, whose save lost, and that a later read of the same delivery finds behind
    // the reorder window. Beside CasConflict, so casRetry does not retry it, and the failure path keeps the event
    // instead of letting the skip acknowledge it.
    private static final class OvertakenInFlight extends RuntimeException {
        private OvertakenInFlight(String sagaId, EventMeta meta) {
            super("Saga instance '" + sagaId + "' saw the event '" + meta.redeliveryKey() + "' as new and lost its save, and other deliveries have since moved the instance "
                  + "past it by more than the reorder window, so this delivery did not apply it and a skip would acknowledge it unapplied");
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
    // carrying no key at all and blocked the channel with nothing able to budget it.
    // An unreadable stream key extension is handed back rather than read as absent, because an event with a bad
    // streamversion would otherwise be handled as one that carries no key.
    private ExtractedMeta extractMeta(CloudEvent cloudEvent) {
        Set<String> extensions = cloudEvent.getExtensionNames();
        List<UnreadableExtension> unreadableStreamKey = new ArrayList<>();
        String streamId = readStreamKey(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_ID, OccurrentExtensionGetter::getStreamId, unreadableStreamKey);
        Long streamVersion = readStreamKey(cloudEvent, extensions, OccurrentCloudEventExtension.STREAM_VERSION, OccurrentExtensionGetter::getStreamVersion, unreadableStreamKey);
        // Use the framework's own position accessor, which accepts a Number or String, rather than narrowing to Long.
        Long position = readPosition(cloudEvent, extensions);
        return new ExtractedMeta(new EventMeta(streamId, streamVersion, position, cloudEvent.getType()), unreadableStreamKey.isEmpty() ? null : unreadableStreamKey.getFirst());
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
