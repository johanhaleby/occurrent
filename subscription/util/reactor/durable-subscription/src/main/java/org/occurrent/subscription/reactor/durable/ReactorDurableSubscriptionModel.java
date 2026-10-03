/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.*;
import org.occurrent.subscription.util.predicate.EveryN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.CheckpointAwareCloudEvent.getCheckpointOrThrowIAE;

/**
 * Wraps a {@link CheckpointAwareSubscriptionModel} and adds persistent checkpoint support, making a
 * subscription durable: it resumes from the last stored position across restarts and stores the position after each
 * successful {@code action}.
 * <p>
 * It is a transparent decorator that itself implements {@link SubscriptionModel} ({@link Subscribable} plus
 * {@link SubscriptionModelLifeCycle}) and {@link CheckpointAwareSubscriptionModel}, so a {@code Durable(delegate)} chain
 * composes uniformly and can be handed to
 * the reactive subscription DSLs and to lifecycle management, mirroring the blocking {@code DurableSubscriptionModel}.
 * The named {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} method behaves in one of two ways,
 * decided by what the wrapped model offers.
 * <p>
 * When the wrapped model manages named subscriptions of its own, in other words when it is a
 * {@link SubscriptionModel}, this model hands the subscription to it and adds only the durable position handling,
 * exactly as the blocking {@code DurableSubscriptionModel} does. Everything the wrapped model already does for a named
 * subscription therefore still applies, so an unsupported {@link SubscriptionFilter} is refused when
 * {@code subscribe(..)} is called and a failing action is retried by the wrapped model rather than ending the
 * subscription. Its life cycle is the wrapped model's life cycle, so pausing, resuming, cancelling, stopping and
 * starting are all forwarded, and so is {@link #shutdown()}. Give each durable model its own wrapped model on this
 * path: two durable models sharing one would stop and shut down each other's subscriptions.
 * <p>
 * When the wrapped model offers only the plain (cold)
 * {@link CheckpointAwareSubscriptionModel#subscribe(SubscriptionFilter, StartAt)} primitive, which is a model written
 * outside this repository since #547 and #550 made every reactor catch-up model a named one,
 * this model drives that primitive itself and manages the life cycle. A failing action is not
 * retried on that path and an unsupported filter is reported when the subscription starts rather than when it is
 * created, because there is no named subscription underneath to inherit either from. See issue #547.
 * <p>
 * Either way the start position is resolved from storage when the caller asks for the subscription-model default, and
 * the position is persisted after each event per {@link ReactorDurableSubscriptionModelConfig}.
 * <p>
 * The first position recorded for a subscription id is written with
 * {@link org.occurrent.subscription.CheckpointWriteCondition#ifAbsent() ifAbsent()}, so a registration that found
 * nothing stored, read its position and then lost that write is refused with
 * {@link StartPositionAlreadyPinnedException} rather than started from a position it never read. A position that was
 * already stored when this model read for it is taken without a word, as before, so a node joining a subscription
 * another has been running is untouched. The refusal reaches the caller wherever that registration path already
 * reports a start it could not make. It is thrown from {@link #subscribe(String, SubscriptionFilter, StartAt, Function)}
 * when the wrapped model manages named subscriptions, and signalled on {@link Subscription#waitUntilStarted()},
 * with an {@code ERROR} logged, when this model drives the cold primitive itself. A subscribe handed to such a wrapped
 * model while a delete of the id that {@link #cancelSubscription(String)} started is still under way records its first
 * position after it returned, so there the refusal is signalled on {@link Subscription#waitUntilStarted()} too, and
 * the subscription is cancelled in the wrapped model. A storage that answers {@code false}
 * from {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} cannot be written to conditionally, so that
 * write stays unconditional and is logged at {@code WARN} instead. See ADR 89. A {@code save(..)} for that first
 * position answering nothing, rather than the checkpoint it wrote, refuses the registration the same way, with
 * {@code IllegalStateException} naming the storage and the position it tried to record, since nothing then shows
 * whether the write reached storage.
 * <p>
 * A registration that asks for {@link StartAt#subscriptionModelDefault()} and has no checkpoint stored is recorded
 * from where the feed is when it registers, so that starting it later still delivers what was written while it waited.
 * A read of that position that fails, and one that answers nothing, refuse the registration the same way.
 * Answering nothing is the wrapped model's documented way of reporting a problem it cannot
 * resolve, not a position, which is why it refuses rather than falling back to
 * {@link StartAt#now()}. A wrapped model applies a start position when it opens its feed rather than when it is handed
 * one, so falling back would begin wherever the feed had reached by then and skip what the read exists to keep. That
 * holds whether this model is running or stopped, which is also how the blocking
 * {@code ManualStartSubscriptionModel} answers a {@code null} position from 0.33.0 on, and a subscription registered
 * while stopped is not read for again when it starts, since a position read then is a position later than the
 * registration.
 * <p>
 * On a thread that may block, a subscribe or a resume from the subscription-model default, or from a dynamic start
 * position that answers it, waits for its read of where the feed is however long that read takes, on a stopped model
 * too. Returning before it answered would let the subscription start from a position read after the call returned, and
 * skip what was written in between. A cancel of that id or a {@link #shutdown()} ends the wait, and so do a pause of
 * that id and a {@link #stop()} when this model drives the subscription itself, also when {@link #start(boolean)}
 * came while it waited. A read for one id does not hold up a call for another id. A subscribe with a dynamic start
 * position on a stopped model that this model drives does not wait, since its function runs only once the model is
 * started, and an event written between that subscribe and the answer of its read does not reach the subscription when
 * the function answers the subscription-model default. Where this model drives the subscription itself, nothing waits
 * for the read on a thread where Reactor does not allow blocking, with the same result. Where it hands the subscription
 * to a wrapped model that manages named subscriptions, Reactor refuses that read on such a thread.
 * <p>
 * The refusal is thrown from {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} when the wrapped model
 * manages named subscriptions of its own, which is the caller's own call and needs no log to reach anybody. When this
 * model drives the cold primitive itself it cannot throw there, so the refusal is signalled on
 * {@link Subscription#waitUntilStarted()}, on the handle {@link #resumeSubscription(String)} returns and on the
 * registration handle as well once that registration asked for the model default and storage has confirmed it holds
 * nothing. A read that fails on the way there is logged at {@code WARN}, since a subscription with a checkpoint
 * already stored, or a start position of its own, still starts fine despite it.
 * A storage that cannot be read leaves the registration handle waiting rather than reporting a refusal the start may
 * not make. Starting a refused subscription is what drops it, so it is registered again rather than resumed, and one
 * that was never started holds its id until {@link #cancelSubscription(String)} releases it.
 * {@link #start(boolean)} keeps starting the rest.
 * <p>
 * Two registrations are left alone by all of that. One that names its own {@link StartAt}, {@link StartAt#now()}
 * included, is not read for at all, since this model records no position for it and the caller has said where to
 * begin. One that already has a checkpoint stored begins from that checkpoint, which is read when the subscription
 * starts and settles the question before the registration read is consulted, so it starts even when that read could
 * not answer. A subscribe on a running model asks storage first and asks the wrapped model where its feed is only
 * when storage holds nothing.
 * <p>
 * A {@link StartAt#dynamic(java.util.function.Supplier) dynamic} start position may answer the model default too,
 * and which of the two it answers decides whether the registration is refused. When the wrapped model manages named
 * subscriptions of its own that is resolved where {@link #subscribe(String, SubscriptionFilter, StartAt, Function)}
 * is called, so a refusal is thrown from that call like any other, with no handle involved. When this model drives
 * the cold primitive itself the function is resolved only once the subscription actually starts. A registration made
  * while running starts immediately, so the refusal comes out on the handle
 * {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} itself returns. One made while stopped leaves
 * that handle waiting instead, and the refusal comes out later, when {@link #start(boolean)} or
 * {@link #resumeSubscription(String)} starts the subscription. It ends the wait of that handle, and of the handle
 * {@link #resumeSubscription(String)} returns when that call started it.
 * <p>
 * {@link ReactorDurableSubscriptionModelConfig#startWhenNoStartPositionCanBeRecorded(boolean)} turns the
 * refusals above into a start without a recorded position, accepting the loss window it documents.
 * <p>
 * Note that this implementation stores the checkpoint after _every_ action by default. If you have a lot of
 * events and duplication is not that much of a deal, consider changing this behavior by supplying an instance of
 * {@link ReactorDurableSubscriptionModelConfig}.
 */
@NullMarked
public class ReactorDurableSubscriptionModel implements CheckpointAwareSubscriptionModel, SubscriptionModel, IntrospectableSubscriptions {
    private static final Logger log = LoggerFactory.getLogger(ReactorDurableSubscriptionModel.class);
    // How long a failed delete of a cancelled subscription's checkpoint waits before it is tried again, about
    // doubling up to the longest, with jitter
    private static final Duration FIRST_DELETE_RETRY_AFTER = Duration.ofMillis(100);
    private static final Duration LONGEST_DELETE_RETRY_AFTER = Duration.ofSeconds(5);

    private final CheckpointAwareSubscriptionModel subscription;
    private final CheckpointStorage storage;
    private final ReactorDurableSubscriptionModelConfig config;
    // Set when the wrapped model manages named subscriptions of its own, which is when this model hands the
    // subscription to it instead of driving the cold primitive. Null for a model that only exposes the primitive,
    // which is what the reactor catch-up models do.
    private final @Nullable SubscriptionModel delegate;
    // Only used when delegating, and only to answer subscriptionIds() for a wrapped model that cannot be asked. Every
    // reactor model that carries a subscription id in this repository is also introspectable, so this is the answer for
    // an out-of-tree one that is not.
    private final Set<String> delegatedSubscriptionIds = ConcurrentHashMap.newKeySet();
    // Both changed under this model's monitor, which is never held while calling the storage, the wrapped model or a
    // function the caller supplies, or while disposing a subscription to the wrapped model's feed
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();
    // Held while reading or changing the four maps below and the fields of a PositionDelete, and never while calling
    // the storage, the wrapped model or a function the caller supplies. A cancel retires the writer of the
    // subscription it removes, collects the writes in flight for the id and installs its delete in one step under it.
    // A write checks its writer and is tracked in one step under it too, so each write is one the delete waits for or
    // one that never starts.
    private final Object positionLock = new Object();
    // The writer of the subscription registered under each id
    private final Map<String, PositionWriter> positionWriters = new HashMap<>();
    // The writers of subscribes still reading where to start or handing the subscription over, by subscription id.
    // They are not registered yet, so a cancel marks them overtaken, which ends the subscribe at its next check.
    private final Map<String, Set<PositionWriter>> positionWritersStarting = new HashMap<>();
    // The position writes that have started and not ended yet, by subscription id, each with the checkpoint it writes,
    // in the order they started
    private final Map<String, SequencedMap<Mono<Void>, Checkpoint>> positionWritesInFlight = new HashMap<>();
    // The latest delete of a stored position that cancelSubscription started for each id, until it has ended. A
    // subscription of the id that starts meanwhile takes it over, see takeOverPositionDelete.
    private final Map<String, PositionDelete> positionDeletes = new HashMap<>();
    // Completes when shutdown() runs, which ends a subscribe reading its start position on the caller's thread
    private final Sinks.Empty<Void> shutDown = Sinks.empty();

    private volatile boolean shutdown = false;
    private volatile boolean running = true;

    /**
     * Create a durable subscription model that stores the checkpoint after each successful call to the action.
     *
     * @param subscription The subscription model that will read events from the event store
     * @param storage      The {@link CheckpointStorage} that'll be used to persist the stream position
     */
    public ReactorDurableSubscriptionModel(CheckpointAwareSubscriptionModel subscription, CheckpointStorage storage) {
        this(subscription, storage, new ReactorDurableSubscriptionModelConfig(EveryN.everyEvent()));
    }

    /**
     * Create a durable subscription model that stores the checkpoint when the predicate defined in
     * {@link ReactorDurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled.
     *
     * @param subscription The subscription model that will read events from the event store
     * @param storage      The {@link CheckpointStorage} that'll be used to persist the stream position
     * @param config       Configures when the checkpoint is persisted
     */
    public ReactorDurableSubscriptionModel(CheckpointAwareSubscriptionModel subscription, CheckpointStorage storage,
                                           ReactorDurableSubscriptionModelConfig config) {
        this.subscription = requireNonNull(subscription, CheckpointAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.storage = requireNonNull(storage, CheckpointStorage.class.getSimpleName() + " cannot be null");
        this.config = requireNonNull(config, ReactorDurableSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
        this.delegate = subscription instanceof SubscriptionModel subscriptionModel ? subscriptionModel : null;
    }

    /**
     * The plain (cold) subscription-model primitive. It is a straight pass-through to the wrapped model and does
     * <em>not</em> persist the checkpoint, since position storage is keyed by subscription id and this
     * primitive has none. Use the named {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} method for a
     * durable subscription.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        return subscription.subscribe(filter, startAt);
    }

    @Override
    public Mono<Checkpoint> globalCheckpoint() {
        return subscription.globalCheckpoint();
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (delegate != null) {
            // Refused here rather than left to the wrapped model, which may accept the subscribe and fail it only
            // later, a catch-up model replaying history and then failing at the handover for example.
            if (shutdown) {
                throw new SubscriptionModelShutdownException();
            }
            // Deliberately outside this model's monitor, which no step of subscribeByDelegating takes. Reading the start
            // position waits on the checkpoint store and handing the subscription over calls the wrapped model, and
            // holding the monitor across either would let one slow subscribe block every life cycle call of every
            // other id, shutdown included.
            return subscribeByDelegating(delegate, subscriptionId, filter, startAt, action);
        }

        // Checked before the read below, so a duplicate subscribe and one on a shut-down model do not ask the wrapped
        // model where the feed is, and checked again under the monitor where the id is taken
        synchronized (this) {
            requireIdFreeAndNotShutDown(subscriptionId);
        }
        // Read before the subscription is put where a start, a resume, a pause, a stop, a cancel or a shutdown can find
        // it, so whichever generation starts it begins from where the feed was when it was registered, and not from
        // where the feed is once a generation gets to read. Subscribed here rather than under the monitor, which is
        // never held while calling the wrapped model. Only a subscribe that a duplicate or a shutdown overtakes between
        // the check above and the one below has read for nothing. On a running model storage is asked first, and the
        // wrapped model only when nothing is stored, since a stored checkpoint is where the subscription starts and the
        // read would answer nothing it uses. A delete of the id that a cancel started is taken over before storage is
        // read, and the read does not wait for that delete, see takeOverPositionDelete.
        Sinks.Empty<Void> readsAbandoned = Sinks.empty();
        final @Nullable Mono<Checkpoint> positionNow;
        @Nullable StoredAtTheCall storedAtTheCall = null;
        TakeOver takenOverAtTheCall = TakeOver.NONE;
        if (!startAt.isDefault() && !startAt.isDynamic()) {
            positionNow = null;
        } else if (running) {
            takenOverAtTheCall = takeOverPositionDelete(subscriptionId);
            Mono<Checkpoint> read = readStoredPosition(subscriptionId, takenOverAtTheCall).cache();
            storedAtTheCall = new StoredAtTheCall(read, takenOverAtTheCall);
            positionNow = capturePositionUnlessStored(subscriptionId, read, readsAbandoned);
        } else {
            positionNow = capturePositionNow(subscriptionId, readsAbandoned);
        }
        if (positionNow != null) {
            startReading(positionNow);
        }
        // Decided under the monitor and started after it is released, so a dynamic start position, the storage read
        // and the subscribe to the wrapped model's feed never hold up a call for another id. The id is taken before the
        // function runs, so a duplicate subscribe is refused without calling it.
        final Reservation reservation;
        try {
            synchronized (this) {
                requireIdFreeAndNotShutDown(subscriptionId);
                reservation = reserveInternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action, positionNow, null, null,
                        readsAbandoned, takenOverAtTheCall == TakeOver.NONE ? List.of() : List.of(takenOverAtTheCall));
            }
        } catch (RuntimeException | Error e) {
            giveBackPositionDelete(subscriptionId, takenOverAtTheCall, null);
            throw e;
        }
        try {
            return startReserved(reservation, false, storedAtTheCall);
        } catch (RuntimeException | Error e) {
            // A dynamic start position that throws gives the id back, so subscribing again under it is not refused as
            // a duplicate
            releaseReservation(reservation);
            giveBackPositionDelete(subscriptionId, takenOverAtTheCall, null);
            throw e;
        }
    }

    // Called under the monitor, when this model drives the feed itself
    private void requireIdFreeAndNotShutDown(String subscriptionId) {
        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        if (shutdown) {
            throw new SubscriptionModelShutdownException();
        }
    }

    /**
     * Hands the subscription to the wrapped model and only adds the durable position handling on top, mirroring the
     * blocking {@code DurableSubscriptionModel}. The wrapped model keeps everything it already does for a named
     * subscription, which is what makes an unsupported filter refused here in {@code subscribe(..)} and a failing
     * action retried rather than fatal.
     * <p>
     * A cancel of the id that comes before the subscription is registered here ends it, and
     * {@link Subscription#waitUntilStarted()} then fails with {@link CancellationException}.
     */
    private Subscription subscribeByDelegating(SubscriptionModel delegate, String subscriptionId, @Nullable SubscriptionFilter filter,
                                               StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        PositionWriter writer = startingPositionWriter(subscriptionId);
        writer.takeOver = takeOverPositionDelete(subscriptionId);
        try {
            @Nullable Subscription delegated = startDelegated(delegate, subscriptionId, filter, startAt, action, writer);
            if (delegated != null) {
                return delegated;
            }
            // A cancel of the id ended the subscribe before it was registered. That cancel's own delete goes ahead, and
            // the deletes taken over no longer count this subscribe.
            giveBackPositionDelete(subscriptionId, writer.takeOver, writer);
            return new ReactorDurableSubscription(subscriptionId, Mono.error(cancelledBeforeItStarted(subscriptionId)));
        } catch (RuntimeException | Error e) {
            // Reactor refusing the read of the start position on a thread that may not block, for one
            giveBackPositionDelete(subscriptionId, writer.takeOver, writer);
            throw e;
        } finally {
            positionWriterNoLongerStarting(subscriptionId, writer);
        }
    }

    // Hands the subscription over, or answers null when a cancel of the id ended the subscribe. A cancel that comes
    // once the writer is registered as starting and before registerDelegated ends it, wherever it is by then, since
    // starting it anyway would run the action after that cancel completed.
    private @Nullable Subscription startDelegated(SubscriptionModel delegate, String subscriptionId, @Nullable SubscriptionFilter filter,
                                                  StartAt startAt, Function<CloudEvent, Mono<Void>> action, PositionWriter writer) {
        // Checked before the function runs, so a shutdown or a cancel that came first does not run it, and again under
        // positionLock below
        @Nullable Registration refusedFirst = refusedRegistrationNow(writer);
        if (refusedFirst == Registration.SHUT_DOWN) {
            throw new SubscriptionModelShutdownException();
        } else if (refusedFirst != null) {
            return null;
        }
        final @Nullable StartAt resolvedStartAt;
        try {
            resolvedStartAt = durableStartAt(subscriptionId, startAt, writer);
        } catch (OvertakenByCancel cancelled) {
            // A cancel of the id ended the wait for the model default's read, see durableStartAt
            return null;
        }
        return handOver(delegate, subscriptionId, filter, startAt, resolvedStartAt, action, writer);
    }

    // Hands the subscription to the wrapped model from startAtToUse, unless a cancel or a shutdown came first
    private @Nullable Subscription handOver(SubscriptionModel delegate, String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt,
                                            @Nullable StartAt startAtToUse, Function<CloudEvent, Mono<Void>> action, PositionWriter writer) {
        // Marked under the lock that a cancel of the id and a shutdown retire under, and released before the wrapped
        // model is called. A cancel or a shutdown that comes after this finds the hand-over, and the subscription the
        // wrapped model makes is cancelled there again below. Neither of them waits for the wrapped model to take the
        // subscribe. The shutdown flag is set before the shutdown takes the lock, and read here last.
        @Nullable Mono<Optional<Checkpoint>> settled = startAtToUse == null ? null : writer.settled;
        // Set in the same step as the mark, so a pause, a resume, a stop or a start that passes to the wrapped model
        // from here on is counted, and a start again reads the state of the wrapped model only once it has returned
        @Nullable KeptLifecycle kept = settled == null ? null : new KeptLifecycle();
        @Nullable Registration refused;
        final boolean startedAgain;
        synchronized (positionLock) {
            refused = refusedRegistration(writer);
            // The wrapped model refuses a duplicate only while it holds the subscription, which it does not for a
            // moment while startAgainInWrappedModel starts it there again
            startedAgain = refused == null && isStartedAgain(subscriptionId);
            if (refused == null && !startedAgain) {
                writer.handingOver = Sinks.empty();
                writer.kept = kept;
            }
        }
        if (startedAgain) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        if (refused == Registration.SHUT_DOWN) {
            throw new SubscriptionModelShutdownException();
        } else if (refused != null) {
            return null;
        }
        // Never runs once a cancel or a shutdown has reached the writer, so a subscription the wrapped model keeps
        // after the cancel below failed delivers nothing to the caller's action
        Function<CloudEvent, Mono<Void>> liveAction = actionWhileLive(writer, action);
        final Subscription delegated;
        try {
            // A null startAtToUse means a dynamic StartAt opted out of starting, so the wrapped model gets the
            // original position and an action that saves no position, and this model stays out of the way, exactly as
            // the blocking twin does.
            delegated = startAtToUse == null
                    ? delegate.subscribe(subscriptionId, filter, startAt, liveAction)
                    : delegate.subscribe(subscriptionId, filter, startAtToUse, settledThen(settled, persistingAction(subscriptionId, writer, liveAction)));
        } catch (RuntimeException | Error e) {
            handedOver(writer, Mono.empty());
            throw e;
        }
        Registration registration = registerDelegated(subscriptionId, writer);
        if (registration == Registration.REGISTERED) {
            handedOver(writer, Mono.empty());
            if (settled == null || kept == null) {
                return delegated;
            }
            Mono<Subscription> running = settled
                    .flatMap(startAgainFrom -> {
                        if (startAgainFrom.isEmpty()) {
                            endKeptLifecycle(writer, kept, null);
                            return Mono.just(delegated);
                        }
                        return startAgainInWrappedModel(delegate, subscriptionId, filter, startAgainFrom.get(), liveAction, writer, kept);
                    })
                    .cache();
            running.subscribe(unused -> {
            }, failure -> endUnsettled(delegate, subscriptionId, writer, failure));
            return new ReactorDurableSubscription(subscriptionId, untilStartedOrShutDown(running.flatMap(Subscription::waitUntilStarted)));
        }
        // A cancel or a shutdown came while the wrapped model took the subscribe, and may have reached that model
        // before the subscribe did, so the subscription it made is cancelled there now, unless a later subscribe of the
        // id has one there by then. registerDelegated retired its writer, so it saves no position meanwhile, and the
        // cancel that came completes only after this, or fails with what failed it.
        handedOver(writer, cancelUnlessHandedOverAgain(delegate, subscriptionId, writer));
        if (registration == Registration.SHUT_DOWN) {
            throw new SubscriptionModelShutdownException();
        }
        return null;
    }

    // The action, once settled has ended, see startAtTheCall. The action skips an event when settled failed, and when
    // the subscription starts again from an earlier position, which delivers that event again.
    private static Function<CloudEvent, Mono<Void>> settledThen(@Nullable Mono<Optional<Checkpoint>> settled, Function<CloudEvent, Mono<Void>> action) {
        if (settled == null) {
            return action;
        }
        return cloudEvent -> settled.map(Optional::isEmpty)
                .onErrorReturn(false)
                .flatMap(goesOn -> goesOn ? action.apply(cloudEvent) : Mono.empty());
    }

    // Cancels the subscription in the wrapped model, once it was handed a later position than the one recorded for it,
    // and subscribes it there again from the recorded one, so it delivers the events between the two. The first
    // subscription's action skipped every event, see settledThen. Marked as a hand-over before the wrapped model is
    // asked, as in handOver, so a cancel or a shutdown that comes meanwhile waits for it, and the subscription the
    // wrapped model makes then is cancelled there again. Subscribes on a thread that may block, since the storage call
    // this runs after can answer on one that may not.
    //
    // From the mark on, the state a pause, a resume, a stop or a start asks for is kept here instead of reaching the
    // wrapped model, see passOrKeep, and applyKeptLifecycle puts it in place once the subscription is there again.
    private Mono<Subscription> startAgainInWrappedModel(SubscriptionModel delegate, String subscriptionId, @Nullable SubscriptionFilter filter,
                                                        Checkpoint from, Function<CloudEvent, Mono<Void>> liveAction, PositionWriter writer,
                                                        KeptLifecycle kept) {
        Mono<Subscription> subscribedAgain = Mono.fromCallable(() -> {
            // A cancel of the id that came since the wrapped model's cancel has made that model cancel too, and a
            // subscribe of the id made after it can have a subscription there, which a wrapped model that lets a
            // subscribe replace another would end
            requireNotRetired(writer, subscriptionId);
            final Subscription again;
            try {
                again = delegate.subscribe(subscriptionId, filter, StartAt.checkpoint(from),
                        persistingAction(subscriptionId, writer, heldWhilePaused(kept, liveAction)));
            } catch (RuntimeException | Error e) {
                handedOver(writer, Mono.empty());
                throw e;
            }
            final boolean retired;
            synchronized (positionLock) {
                retired = writer.retired;
            }
            // A cancel that came meanwhile can have reached the wrapped model before this subscribe did, so the
            // subscription is cancelled there again, unless a subscribe of the id made after that cancel has one there
            handedOver(writer, retired ? cancelUnlessHandedOverAgain(delegate, subscriptionId, writer) : Mono.empty());
            if (retired) {
                throw cancelledBeforeItStarted(subscriptionId);
            }
            return again;
        });
        return Mono.defer(() -> {
                    final List<Mono<Void>> first = new ArrayList<>();
                    synchronized (positionLock) {
                        if (writer.retired) {
                            return Mono.error(cancelledBeforeItStarted(subscriptionId));
                        }
                        writer.handingOver = Sinks.empty();
                        kept.keeping = true;
                        if (kept.passing > 0) {
                            kept.passed = Sinks.empty();
                            first.add(kept.passed.asMono());
                        }
                        // A subscribe of the id that handOver marked before this, which the wrapped model refuses as a
                        // duplicate while it still holds the subscription. handOver refuses those that come after.
                        for (PositionWriter starting : positionWritersStarting.getOrDefault(subscriptionId, Set.of())) {
                            Sinks.@Nullable Empty<Void> handingOver = starting.handingOver;
                            if (starting != writer && handingOver != null) {
                                first.add(handingOver.asMono().onErrorResume(__ -> Mono.empty()));
                            }
                        }
                    }
                    return Mono.when(first);
                })
                // What the wait above waits for ends on the thread of a caller, which the calls below must not run on
                .publishOn(Schedulers.boundedElastic())
                .then(Mono.fromRunnable(() -> keepWrappedLifecycle(delegate, subscriptionId, kept)))
                // Checked right before the wrapped model is asked, since it cancels by id and would end a subscription
                // that a subscribe of the id made after a cancel of it has there by then. A cancel and a subscribe of
                // the id that both come while the wrapped model takes this cancel are not seen.
                .then(Mono.defer(() -> {
                    requireNotRetired(writer, subscriptionId);
                    return delegate.cancelSubscription(subscriptionId);
                }))
                .then(subscribedAgain.subscribeOn(Schedulers.boundedElastic()))
                .flatMap(again -> Mono.fromRunnable(() -> applyKeptLifecycle(delegate, subscriptionId, writer, kept)).thenReturn(again))
                // A no-op once the hand-over has ended, and ends it when the wrapped model's cancel failed
                .doOnError(__ -> handedOver(writer, Mono.empty()));
    }

    // A cancel, a shutdown, or a subscribe the wrapped model let replace this subscription, while the start again
    // waited
    private void requireNotRetired(PositionWriter writer, String subscriptionId) {
        synchronized (positionLock) {
            if (writer.retired) {
                throw cancelledBeforeItStarted(subscriptionId);
            }
        }
    }

    // The state the wrapped model has for the subscription before it is cancelled there, unless a call asked for one
    // since the mark in startAgainInWrappedModel
    private void keepWrappedLifecycle(SubscriptionModel delegate, String subscriptionId, KeptLifecycle kept) {
        boolean paused = wrappedIsPaused(delegate, subscriptionId);
        synchronized (positionLock) {
            if (kept.paused == null) {
                kept.ask(paused);
            }
        }
    }

    // Pauses or resumes the subscription in the wrapped model until it has the state last asked for, then lets the calls
    // pass to it again. Gives up at a cancel or a shutdown of the id, and at a call the wrapped model refuses, unless a
    // call asked again since. The keeping ends in the step that finds nothing left to do, so a call made before that
    // step is kept and done here, and one made after it passes to the wrapped model.
    private void applyKeptLifecycle(SubscriptionModel delegate, String subscriptionId, PositionWriter writer, KeptLifecycle kept) {
        boolean wrappedPaused = wrappedIsPaused(delegate, subscriptionId);
        @Nullable RuntimeException refusal = null;
        long refusedAsk = -1;
        final Sinks.Empty<Void> unpaused;
        final boolean gaveUp;
        while (true) {
            final boolean pause;
            final long ask;
            synchronized (positionLock) {
                pause = Boolean.TRUE.equals(kept.paused);
                ask = kept.asks;
                if (writer.retired || pause == wrappedPaused || ask == refusedAsk) {
                    gaveUp = !writer.retired && pause != wrappedPaused;
                    unpaused = stopKeeping(writer, kept);
                    break;
                }
            }
            try {
                if (pause) {
                    delegate.pauseSubscription(subscriptionId);
                } else {
                    delegate.resumeSubscription(subscriptionId);
                }
                wrappedPaused = pause;
            } catch (RuntimeException e) {
                // A stop or a start of the wrapped model can have done it first
                wrappedPaused = wrappedIsPaused(delegate, subscriptionId);
                if (wrappedPaused != pause) {
                    refusal = e;
                    refusedAsk = ask;
                }
            }
        }
        if (gaveUp) {
            log.warn("Could not {} subscription {} in the wrapped model {} after starting it there again, so it stays {}", wrappedPaused ? "resume" : "pause",
                    subscriptionId, subscription.getClass().getName(), wrappedPaused ? "paused" : "running", refusal);
        }
        keepingStopped(unpaused, kept, null);
    }

    // Ends the keeping of the state asked for, so the calls pass to the wrapped model again, and lets an action held by
    // heldWhilePaused go on. A failure is what ended the start again, which the handle of a resume made meanwhile ends
    // with.
    private void endKeptLifecycle(PositionWriter writer, KeptLifecycle kept, @Nullable Throwable failure) {
        final Sinks.Empty<Void> unpaused;
        synchronized (positionLock) {
            unpaused = stopKeeping(writer, kept);
        }
        keepingStopped(unpaused, kept, failure);
    }

    // Called under positionLock. Answers what lets an action held by heldWhilePaused go on, which keepingStopped emits
    // once the lock is released.
    private static Sinks.Empty<Void> stopKeeping(PositionWriter writer, KeptLifecycle kept) {
        kept.keeping = false;
        if (writer.kept == kept) {
            writer.kept = null;
        }
        return kept.unpaused;
    }

    private static void keepingStopped(Sinks.Empty<Void> unpaused, KeptLifecycle kept, @Nullable Throwable failure) {
        unpaused.tryEmitEmpty();
        if (failure == null) {
            kept.applied.tryEmitEmpty();
        } else {
            kept.applied.tryEmitError(failure);
        }
    }

    // A wrapped model that does not know the id, which it does not between the cancel and the subscribe in
    // startAgainInWrappedModel, may throw instead of answering false
    private static boolean wrappedIsPaused(SubscriptionModel delegate, String subscriptionId) {
        try {
            return delegate.isPaused(subscriptionId);
        } catch (RuntimeException e) {
            return false;
        }
    }

    // The caller's action, held while the state kept is paused, since a skipped event would get its position written
    // as handled. A pause in the wrapped model ends the held action and delivers that event again after the resume.
    // Goes on off the thread that resumed it.
    private Function<CloudEvent, Mono<Void>> heldWhilePaused(KeptLifecycle kept, Function<CloudEvent, Mono<Void>> action) {
        return cloudEvent -> Mono.defer(() -> {
            final Sinks.@Nullable Empty<Void> unpaused;
            synchronized (positionLock) {
                unpaused = kept.keeping && Boolean.TRUE.equals(kept.paused) ? kept.unpaused : null;
            }
            return unpaused == null
                    ? action.apply(cloudEvent)
                    : unpaused.asMono().publishOn(Schedulers.boundedElastic()).then(Mono.defer(() -> heldWhilePaused(kept, action).apply(cloudEvent)));
        });
    }

    // Ends a subscription handed to the wrapped model whose start position could not be recorded, or that could not
    // start again from the one recorded, as a subscribe refused for that reason would have ended, unless a cancel, a
    // shutdown or a subscribe of the id came first. The subscription is cancelled in the wrapped model, see
    // cancelUnlessHandedOverAgain, and the delete it took over goes ahead unless it recorded its start position, see
    // giveBackPositionDelete.
    private void endUnsettled(SubscriptionModel delegate, String subscriptionId, PositionWriter writer, Throwable failure) {
        final boolean registered;
        final @Nullable KeptLifecycle kept;
        synchronized (positionLock) {
            writer.retired = true;
            registered = positionWriters.remove(subscriptionId, writer);
            if (registered) {
                delegatedSubscriptionIds.remove(subscriptionId);
            }
            kept = writer.kept;
        }
        if (kept != null) {
            endKeptLifecycle(writer, kept, failure);
        }
        if (registered) {
            log.error("Subscription {} was cancelled in the wrapped model {}, since it could not start from a start position recorded for it", subscriptionId,
                    subscription.getClass().getName(), failure);
            cancelUnlessHandedOverAgain(delegate, subscriptionId, writer);
        }
        if (!wrotePosition(writer)) {
            giveBackPositionDelete(subscriptionId, writer.takeOver, writer);
        }
    }

    // Cancels the subscription of the id in the wrapped model, which only cancels by id, unless a later subscribe of the
    // id has registered one there by then, which that cancel would end instead. Waits for a hand-over of the id under
    // way first, since it can still register one, unless a cancel or a shutdown has ended it, since it then registers
    // nothing and two of those would otherwise wait for each other. A subscribe that only starts its hand-over after
    // the check is not seen, so a wrapped model that lets it replace the subscription still there can have it
    // cancelled. Called once, and as soon as it is called, like cancelInWrappedModel.
    private Mono<Void> cancelUnlessHandedOverAgain(SubscriptionModel delegate, String subscriptionId, PositionWriter ended) {
        final List<Mono<Void>> underWay = new ArrayList<>();
        final boolean handedOverAgain;
        synchronized (positionLock) {
            handedOverAgain = positionWriters.containsKey(subscriptionId);
            for (PositionWriter starting : positionWritersStarting.getOrDefault(subscriptionId, Set.of())) {
                Sinks.@Nullable Empty<Void> handingOver = starting.handingOver;
                if (starting != ended && handingOver != null && !starting.overtakenByCancel && !starting.retired) {
                    underWay.add(handingOver.asMono());
                }
            }
        }
        if (handedOverAgain) {
            return Mono.empty();
        }
        if (underWay.isEmpty()) {
            return cancelInWrappedModel(delegate, subscriptionId);
        }
        // The hand-over ends on the thread of the subscribe that made it, which the cancel must not run on
        Mono<Void> cancelled = Mono.when(underWay).onErrorResume(__ -> Mono.empty())
                .publishOn(Schedulers.boundedElastic())
                .then(Mono.defer(() -> cancelUnlessHandedOverAgain(delegate, subscriptionId, ended)))
                .cache();
        cancelled.subscribe(unused -> {
        }, unused -> {
        });
        return cancelled;
    }

    // Called once, and as soon as it is called. Cached, so a cancel of the id that waits for it does not cancel again.
    // A failure is not swallowed, since the subscription can then still be in the wrapped model. The cancel that
    // waits for this fails with it, and a shutdown, which returns nothing to fail, logs it.
    private Mono<Void> cancelInWrappedModel(SubscriptionModel delegate, String subscriptionId) {
        Mono<Void> cancelled = Mono.defer(() -> delegate.cancelSubscription(subscriptionId)).cache();
        cancelled.subscribe(unused -> {
        }, throwable -> {
            if (shutdown) {
                log.error("Could not cancel subscription {} in the wrapped model {} during shutdown, after that model took the subscribe. That model may still hold the subscription, though its action no longer runs.",
                        subscriptionId, subscription.getClass().getName(), throwable);
            }
        });
        return cancelled;
    }

    // Tells a cancel of the id that found this hand-over under way that it has ended, once what follows it has, with
    // the error that ended it
    private void handedOver(PositionWriter writer, Mono<Void> after) {
        final Sinks.@Nullable Empty<Void> handingOver;
        synchronized (positionLock) {
            handingOver = writer.handingOver;
            writer.handingOver = null;
        }
        if (handingOver != null) {
            after.subscribe(unused -> {
            }, handingOver::tryEmitError, handingOver::tryEmitEmpty);
        }
    }

    private static CancellationException cancelledBeforeItStarted(String subscriptionId) {
        return new CancellationException("Subscription " + subscriptionId + " was cancelled before it started");
    }

    // The caller's action, skipped for an event delivered once a cancel or a shutdown has reached the writer
    private Function<CloudEvent, Mono<Void>> actionWhileLive(PositionWriter writer, Function<CloudEvent, Mono<Void>> action) {
        return cloudEvent -> retiredOrOvertaken(writer) ? Mono.empty() : action.apply(cloudEvent);
    }

    private boolean retiredOrOvertaken(PositionWriter writer) {
        synchronized (positionLock) {
            return writer.retired || writer.overtakenByCancel;
        }
    }

    private @Nullable Registration refusedRegistrationNow(PositionWriter writer) {
        synchronized (positionLock) {
            return refusedRegistration(writer);
        }
    }

    // Registered in the same step that a cancel of the id retires the registered writer and marks the starting ones
    // overtaken, and that a shutdown retires every writer and clears the ids handed over, so each of them runs either
    // wholly before this or wholly after it. One that ran while the wrapped model took the subscribe is found here, and
    // the caller cancels the subscription there again.
    private Registration registerDelegated(String subscriptionId, PositionWriter writer) {
        synchronized (positionLock) {
            @Nullable Registration refused = refusedRegistration(writer);
            if (refused != null) {
                writer.retired = true;
                return refused;
            }
            registerWriter(subscriptionId, writer);
            delegatedSubscriptionIds.add(subscriptionId);
            return Registration.REGISTERED;
        }
    }

    // What keeps a subscribe from being handed over or registering, or null when nothing does. Called under
    // positionLock, which a cancel changes the writer under. The shutdown flag is set before a shutdown takes that lock,
    // and read here last.
    private @Nullable Registration refusedRegistration(PositionWriter writer) {
        if (writer.overtakenByCancel) {
            return Registration.OVERTAKEN;
        }
        if (shutdown) {
            return Registration.SHUT_DOWN;
        }
        return null;
    }

    // The caller's action with the checkpoint save behind it, which is the whole of what this model adds to a delivery.
    private Function<CloudEvent, Mono<Void>> persistingAction(String subscriptionId, PositionWriter writer, Function<CloudEvent, Mono<Void>> action) {
        // One per subscription, so an EveryN configured for the whole model counts this subscription's events only
        Predicate<CloudEvent> persistCheckpoint = EveryN.forOneSubscription(config.persistCloudEventPositionPredicate);
        return cloudEvent -> action.apply(cloudEvent)
                .then(Mono.defer(() -> {
                    if (!persistCheckpoint.test(cloudEvent)) {
                        return Mono.empty();
                    }
                    Checkpoint checkpoint = getCheckpointOrThrowIAE(cloudEvent);
                    return writePosition(subscriptionId, writer, checkpoint, () -> savePosition(subscriptionId, writer, checkpoint), Mono.empty()).then();
                }));
    }

    // On the condition that the takeover of a delete of the id set for the generation, see takeOverPositionDelete, and
    // as with no delete running otherwise
    private Mono<Checkpoint> savePosition(String subscriptionId, PositionWriter writer, Checkpoint checkpoint) {
        @Nullable CheckpointWriteCondition condition = writer.takeOver.writeCondition;
        return condition == null ? storage.save(subscriptionId, checkpoint) : storage.save(subscriptionId, checkpoint, condition);
    }

    /**
     * The reactor counterpart of the blocking {@code DurableSubscriptionModel#generateStartAtPositionFrom}. The
     * subscription-model default becomes a dynamic {@link StartAt} so that the wrapped model asks for the position when
     * it actually subscribes. That keeps this {@code subscribe(..)} synchronous, which is what lets the wrapped model
     * refuse an unsupported filter to the caller instead of failing later where nobody is listening.
     * <p>
     * Returns {@code null} when a dynamic {@code StartAt} opted out of starting.
     */
    private @Nullable StartAt durableStartAt(String subscriptionId, StartAt startAt, PositionWriter writer) {
        if (startAt.isDefault()) {
            // Awaited here, so that what the wrapped model receives is a position and not something it has to resolve
            // later. It re-resolves the position whenever it restarts a change stream, and that runs on a scheduler
            // thread where awaiting a reactive read is refused outright, which would leave a subscription that hit one
            // transient storage error unable to ever start. Awaiting on this thread also reads the position before the
            // subscription is registered, so one registered while the wrapped model is stopped begins from here rather
            // than from wherever the feed has reached when it is finally started. A shutdown ends the wait, since a read
            // of where the feed is may never answer, and so does a cancel of the id, with OvertakenByCancel.
            Mono<StartAt> ended = shutDown.asMono().then(Mono.error(SubscriptionModelShutdownException::new));
            // A takeover that wrote the checkpoint back at once waits for nothing, but writing the start position it
            // read still calls the storage, which startAtTheCall does after this returns
            Mono<StartAt> resolved = writer.takeOver.waitsForNothing && writer.takeOver.writtenBack == null
                    ? resolveStartAt(subscriptionId, startAt, null, null, writer, null, null)
                    : startAtTheCall(subscriptionId, startAt, writer);
            return Mono.firstWithSignal(resolved, ended, writer.overtaken.asMono().then(Mono.empty())).block();
        } else if (startAt.isDynamic()) {
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            return nextStartAt == null ? null : durableStartAt(subscriptionId, nextStartAt, writer);
        }
        return startAt;
    }

    // resolveStartAt for a subscribe handed to the wrapped model that took over a delete of the id which it would wait
    // for, or that wrote the stored checkpoint back at once. The start position is read at the call as with no delete
    // running, from storage, which the delete does not hold up, or from where the wrapped model is when storage holds
    // nothing. Recording it, and waiting for the delete, runs once the subscription is handed over, off the caller's
    // thread, and writer.settled ends when both have. The wrapped model gets a position at the call, and an event it
    // delivers before writer.settled ends waits for it, see settledThen. writer.settled answers the position to start
    // the subscription again from when the one recorded is earlier than the one handed over, and nothing otherwise.
    // A later one recorded in place of the position read from storage only means events delivered twice, so the
    // subscription keeps going.
    private Mono<StartAt> startAtTheCall(String subscriptionId, StartAt startAt, PositionWriter writer) {
        TakeOver takeOver = writer.takeOver;
        Mono<Checkpoint> stored = readStoredPosition(subscriptionId, takeOver).cache();
        Mono<Checkpoint> wrappedModelAt = Mono.defer(() -> positionOfTheWrappedModel(subscriptionId)).cache();
        Mono<Checkpoint> handedOver = stored.switchIfEmpty(wrappedModelAt);
        Mono<Optional<Checkpoint>> startsAgainFrom = stored
                .flatMap(read -> takeOver.writtenBack == null ? Mono.just(read) : holdStartPosition(subscriptionId, read, takeOver, writer))
                .map(__ -> Optional.<Checkpoint>empty())
                .switchIfEmpty(Mono.defer(() -> wrappedModelAt.flatMap(read -> pinStartPosition(subscriptionId, read, writer)
                        .map(recorded -> read.asString().equals(recorded.asString()) ? Optional.<Checkpoint>empty() : Optional.of(recorded)))));
        // A start from the present, which startWhenNoStartPositionCanBeRecorded allows, records nothing and waits for
        // nothing. One from a checkpoint waits for the deletes taken over too, so a write back that fails ends it.
        writer.settled = startsAgainFrom
                .flatMap(again -> takeOver.beforeStorage.thenReturn(again))
                .defaultIfEmpty(Optional.empty())
                // Before anything that waits for it hears of the failure, so no event reaches the action after it
                .doOnError(__ -> {
                    synchronized (positionLock) {
                        writer.retired = true;
                    }
                })
                .cache();
        Mono<StartAt> resolved = handedOver.map(StartAt::checkpoint);
        return config.startWhenNoStartPositionCanBeRecorded ? resolved.defaultIfEmpty(startAt) : resolved;
    }

    // Where the wrapped model is now, for a subscription from the model default that storage holds nothing for. An
    // empty answer is refused unless startWhenNoStartPositionCanBeRecorded lets the subscription start without it.
    private Mono<Checkpoint> positionOfTheWrappedModel(String subscriptionId) {
        return config.startWhenNoStartPositionCanBeRecorded
                ? subscription.globalCheckpoint()
                : subscription.globalCheckpoint().switchIfEmpty(Mono.error(() -> positionSourceAnsweredNothing(subscriptionId)));
    }

    // Where the feed is, read once and remembered, so a subscription that is not started yet can begin from here. A
    // read that fails, and one that answers nothing, both refuse a subscription at the model default instead, see
    // resolveStartAt. Reading again when the subscription starts would answer with wherever the feed has reached by
    // then, and starting from that skips everything written while the subscription waited, which is the whole of what
    // reading at registration is for.
    // An empty answer is the unresolvable problem the wrapped model documents rather than a position, so it refuses
    // for the same reason. Cached, so the read runs once and every subscriber sees the same outcome. Deferred, so the
    // wrapped model is asked only when the read is subscribed to, which is after the monitor is released.
    //
    // abandoned fails once a cancel of the id has ended what the read is for, and a shutdown does the same. Either
    // cancels the read in the wrapped model, which then reports nothing, and the read fails with that error.
    private Mono<Checkpoint> capturePositionNow(String subscriptionId, Sinks.Empty<Void> abandoned) {
        Mono<Checkpoint> positionNow = config.startWhenNoStartPositionCanBeRecorded
                ? Mono.defer(subscription::globalCheckpoint)
                : Mono.defer(subscription::globalCheckpoint).switchIfEmpty(Mono.error(() -> positionSourceAnsweredNothing(subscriptionId)));
        Mono<Checkpoint> reported = positionNow
                .doOnError(throwable -> log.warn("Could not read the current position for subscription {}. If its start position resolves to the subscription-model default, this failure refuses it when it starts, unless by then a checkpoint is stored for it. Any other start position is not refused by this failure", subscriptionId, throwable));
        return Mono.firstWithSignal(reported, abandoned.asMono().then(Mono.never()),
                        shutDown.asMono().then(Mono.error(SubscriptionModelShutdownException::new)))
                .cache();
    }

    // capturePositionNow for a subscribe on a running model, which asks the wrapped model only once storedAtTheCall
    // answered that storage holds nothing. When storage holds a checkpoint it fails
    // with CheckpointStored instead, since that checkpoint is where the subscription starts and nothing uses this read
    // then. A storage that fails is answered with the read, so the subscription has it where it would have without
    // asking storage first.
    private Mono<Checkpoint> capturePositionUnlessStored(String subscriptionId, Mono<Checkpoint> storedAtTheCall, Sinks.Empty<Void> abandoned) {
        Mono<Checkpoint> positionNow = capturePositionNow(subscriptionId, abandoned);
        return Mono.firstWithSignal(storedAtTheCall.hasElement().onErrorReturn(false), abandoned.asMono().then(Mono.<Boolean>never()))
                .flatMap(stored -> stored ? Mono.<Checkpoint>error(() -> new CheckpointStored(subscriptionId)) : positionNow)
                .cache();
    }

    // Starts a read from capturePositionNow, so it reads where the feed is now even when nothing else subscribes to it
    // until later. The error consumer keeps a failed read off Operators.onErrorDropped. Reporting it is
    // capturePositionNow's job, and it does it once.
    private static void startReading(Mono<Checkpoint> positionNow) {
        positionNow.subscribe(unused -> {
        }, throwable -> {
        });
    }

    // Waits on the caller's thread until a read from capturePositionNow has answered or failed, or until ended
    // signals. A failure is reported and acted on where the answer is used.
    private static void awaitAnswer(Mono<Checkpoint> position, Mono<Void> ended) {
        Mono.firstWithSignal(position.then().onErrorResume(__ -> Mono.empty()), ended).block();
    }

    // A read that could not answer does not settle the registration on its own. A checkpoint stored for this
    // subscription is where it starts, and this read is never consulted then, so asking storage is what tells a
    // subscription that is about to be refused from one that will start on what it has run before. Only storage
    // answering that it holds nothing settles it. A storage that cannot answer at all leaves this waiting, since a
    // read that failed here says nothing about what the read at start will find.
    private Mono<Void> refusalOnceNothingIsStored(String subscriptionId, Mono<Checkpoint> positionNow) {
        return readStoredPosition(subscriptionId)
                .onErrorResume(__ -> Mono.never())
                .flatMap(__ -> Mono.<Void>never())
                .switchIfEmpty(positionNow.then(Mono.<Void>never()));
    }

    // No original throwable to carry here, since answering nothing is how the wrapped model reports a problem it
    // cannot resolve, so this is what names the subscription and the way past it. Built at capturePositionNow's read
    // failure too, before storage is asked, so it cannot claim storage holds nothing: a checkpoint stored by then, or
    // a start position of its own, still lets the subscription start despite this failure, and only the caller
    // consulting storage afterwards, in refusalOnceNothingIsStored or resolveStartAt, settles whether it is refused.
    private IllegalStateException positionSourceAnsweredNothing(String subscriptionId) {
        return new IllegalStateException("The wrapped subscription model " + subscription.getClass().getName() +
                                         " answered nothing when asked for the current position for subscription " +
                                         subscriptionId + ", which is how it reports a problem it cannot resolve. A " +
                                         "checkpoint already stored for it, or a start position of its own, still lets " +
                                         "it start despite this failure; only when neither holds is the registration " +
                                         "refused rather than started from wherever the feed has reached by then, " +
                                         "which would skip whatever was written while it waited. Starting it is what " +
                                         "releases the id, so register it again after that, or after " +
                                         "cancelSubscription(String), once the model can answer. Subscribing with a " +
                                         "StartAt of your own records no position and carries no such guarantee. To " +
                                         "start anyway, accepting that loss window, configure " +
                                         "ReactorDurableSubscriptionModelConfig.startWhenNoStartPositionCanBeRecorded(true), " +
                                         "or set occurrent.subscription.start-when-no-start-position-can-be-recorded=true " +
                                         "when using the Spring Boot starter.");
    }

    // Run under the monitor, and calls nothing outside this model. Puts the subscription into the map it belongs in and
    // registers its writer, so a duplicate subscribe, a pause, a cancel or a shutdown of the id that comes after sees
    // it. startReserved does the rest once the monitor is released. positionNow is the read of where the feed was at
    // registration, already subscribed, and null for a registration with a start position of its own.
    private Reservation reserveInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt,
                                                    Function<CloudEvent, Mono<Void>> action, @Nullable Mono<Checkpoint> positionNow,
                                                    @Nullable Mono<Checkpoint> positionAtRegistration, @Nullable InternalSubscription replaced,
                                                    Sinks.Empty<Void> readsAbandoned, List<TakeOver> takenOverBefore) {
        // One stable identity for the subscription's whole lifetime, put into its map before anything subscribes and
        // never replaced, so a remove of the id with this value takes out this subscription and no other. A dispose
        // that comes before the subscribe below makes the swap dispose what it is given then.
        Disposable.Swap disposable = Disposables.swap();
        if (!running) {
            // The model is stopped, so nothing subscribes to the feed and waitUntilStarted() does not complete for a
            // subscription that won't deliver anything until start(true) or resumeSubscription starts it.
            //
            // Hold where the feed was at registration, because starting this subscription later would otherwise
            // begin wherever the feed had reached by then, skipping everything written while it waited. Nothing is
            // stored until the subscription starts, so one that never starts leaves nothing behind. A read that could
            // not answer refuses the subscription when it starts, which is where the model drops it, so getting it
            // back means registering it again rather than resuming. Only a registration that can still ask this model
            // where to begin has read for it. A concrete position is where the subscription begins whatever the feed
            // does while it waits. A dynamic one is not resolved until the subscription starts, so it is read for in
            // case it answers the model default then.
            StartAt startAtNow = currentStartAt.get();
            // A read that answered still leaves waitUntilStarted() waiting, since the subscription has not started and
            // will not until it is asked to. Only the model default is certain to begin from what was read, so only
            // that one can end the wait here with the reason it could not be read. A cancel ends it too.
            Sinks.Empty<Void> signal = Sinks.empty();
            Mono<Void> started = startAtNow.isDefault() && positionNow != null
                    ? Mono.firstWithSignal(signal.asMono(), refusalOnceNothingIsStored(subscriptionId, positionNow))
                    : signal.asMono();
            InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, signal, started,
                    replaced == null ? signal : replaced.heldSignal, positionNow, positionNow, readsAbandoned, takenOverBefore);
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            return new Reservation(subscriptionId, internalSubscription, replaced, false);
        }
        Sinks.Empty<Void> signal = Sinks.empty();
        InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, signal, signal.asMono(),
                replaced == null ? signal : replaced.heldSignal, positionNow, positionAtRegistration, readsAbandoned, takenOverBefore);
        synchronized (positionLock) {
            registerWriter(subscriptionId, internalSubscription.writer);
        }
        runningSubscriptions.put(subscriptionId, internalSubscription);
        return new Reservation(subscriptionId, internalSubscription, replaced, true);
    }

    // Run after the monitor is released. Calls the dynamic start position, which can throw, and subscribes to the
    // storage read and the wrapped model's feed. storedAtTheCall is what storage held when a subscribe asked it before
    // reading where the feed is, and null where nothing asked.
    //
    // A subscribe or a resume on a thread that may block waits here for the reads it starts from. startOfModel is true
    // for a start of the model, which starts many subscriptions on one thread and takes and waits for no read, so no
    // read holds up the start or another subscription.
    private Subscription startReserved(Reservation reservation, boolean startOfModel, @Nullable StoredAtTheCall storedAtTheCall) {
        String subscriptionId = reservation.subscriptionId();
        InternalSubscription internalSubscription = reservation.subscription();
        // A resume can run while the pause that put the subscription aside still disposes it, so it is disposed here
        // too before the same subscription starts again
        @Nullable InternalSubscription replaced = reservation.replaced();
        if (replaced != null) {
            replaced.disposable.dispose();
        }
        Subscription handle = new ReactorDurableSubscription(subscriptionId, untilStartedOrShutDown(internalSubscription.started));
        if (!reservation.running()) {
            // The model default starts from this read, and one that answers after this returned would answer with a
            // position after events written in between, so a thread that may block waits for it. The subscription
            // can never start without it anyway. A cancel or a shutdown ends the wait.
            @Nullable Mono<Checkpoint> positionAtRegistration = internalSubscription.positionNow;
            if (internalSubscription.currentStartAt.get().isDefault() && positionAtRegistration != null && !Schedulers.isInNonBlockingThread()) {
                awaitAnswer(positionAtRegistration, Mono.firstWithSignal(internalSubscription.signal.asMono().onErrorResume(__ -> Mono.empty()), shutDown.asMono()));
            }
            return handle;
        }
        // A cancel, a pause, a stop or a shutdown that came in since the monitor was released retired this generation
        // and ended its handle, so nothing starts for it
        if (retired(internalSubscription.writer)) {
            return handle;
        }
        PositionWriter writer = internalSubscription.writer;
        // Before anything of this generation reads or writes storage, see takeOverPositionDelete. Given back at once
        // when a cancel, a pause, a stop or a shutdown retired the generation meanwhile, since what retired it found
        // no takeover to give back.
        TakeOver takeOver = takeOverPositionDelete(subscriptionId);
        final boolean retiredMeanwhile;
        synchronized (positionLock) {
            retiredMeanwhile = writer.retired;
            if (!retiredMeanwhile) {
                writer.takeOver = takeOver;
            }
        }
        if (retiredMeanwhile) {
            giveBackPositionDelete(subscriptionId, takeOver, null);
            return handle;
        }
        Sinks.Empty<Void> startedSink = internalSubscription.signal;
        AtomicReference<StartAt> currentStartAt = internalSubscription.currentStartAt;
        @Nullable SubscriptionFilter filter = internalSubscription.filter;
        Function<CloudEvent, Mono<Void>> action = internalSubscription.action;
        // A cancel, a pause, a stop or a shutdown ends a wait for a read, since each ends the handle
        @Nullable Mono<Void> waitEnds = !startOfModel && !Schedulers.isInNonBlockingThread()
                ? Mono.firstWithSignal(startedSink.asMono().onErrorResume(__ -> Mono.empty()), shutDown.asMono())
                : null;
        Mono<StartAt> resolvedStartAt = resolveStartAt(subscriptionId, currentStartAt.get(), internalSubscription.positionNow,
                internalSubscription.positionAtRegistration, writer, waitEnds, storedAtTheCall);
        resolvedStartAt
                .flatMapMany(startAt -> {
                    currentStartAt.set(startAt);
                    return source(subscriptionId, filter, startAt, action, currentStartAt, true, writer, startedSink);
                })
                // An empty resolveStartAt means a dynamic StartAt opted out of starting (its function returned null),
                // so read from the original StartAt without durable position handling, mirroring the blocking model's
                // "delegate to the wrapped model" branch.
                .switchIfEmpty(Flux.defer(() -> source(subscriptionId, filter, currentStartAt.get(), action, currentStartAt, false, writer, startedSink)))
                // Last, so a pause, cancel or shutdown that disposed the subscription before this subscribe cancels it
                // before anything is requested, and one that comes after cancels it through the same swap
                .doOnSubscribe(subscription -> internalSubscription.disposable.update(subscription::cancel))
                .subscribe(unused -> {
                        }, throwable -> {
                            log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable);
                            // Told before the id is removed below, so no handle is left waiting on an id that is gone
                            boolean endedBeforeItStarted = endBeforeItStarted(internalSubscription, throwable);
                            // internalSubscription is the only entry this generation ever puts under its id, and retire
                            // takes out its writer only, so both are unambiguous whenever the error comes
                            boolean dropped = runningSubscriptions.remove(subscriptionId, internalSubscription);
                            retire(subscriptionId, internalSubscription);
                            // A generation that a pause put aside first keeps what it took over, for the resume
                            if (endedBeforeItStarted && dropped) {
                                giveBackUnlessWritten(subscriptionId, internalSubscription);
                            }
                        });
        return handle;
    }

    // Fails the generation's own handle with throwable, and the handle of the call that began its run when that one
    // still waits, which is the only handle there is for a generation a start of this model began. A generation that
    // had already started keeps that outcome, and so does the handle of its run. Answers whether the generation ended
    // here, which only one that never subscribed to the feed and was not ended before does, since subscribing
    // completes the signal.
    private static boolean endBeforeItStarted(InternalSubscription generation, Throwable throwable) {
        boolean ended = generation.signal.tryEmitError(throwable).isSuccess();
        if (ended && generation.heldSignal != generation.signal) {
            generation.heldSignal.tryEmitError(throwable);
        }
        return ended;
    }

    // Gives back the deletes of the id that a generation which ended before it started took over, and those the
    // generations before it in its run took over, see takenOverBefore, as a subscribe that throws does. No
    // subscription needs the checkpoint those deletes were to remove then. A generation that wrote its start position
    // keeps them, so that position stays stored.
    private void giveBackUnlessWritten(String subscriptionId, InternalSubscription generation) {
        if (!wrotePosition(generation.writer)) {
            giveBackTakeOvers(subscriptionId, generation);
        }
    }

    private void giveBackTakeOvers(String subscriptionId, InternalSubscription generation) {
        giveBackPositionDelete(subscriptionId, generation.writer.takeOver, generation.writer);
        generation.takenOverBefore.forEach(takeOver -> giveBackPositionDelete(subscriptionId, takeOver, null));
    }

    private boolean wrotePosition(PositionWriter writer) {
        synchronized (positionLock) {
            return writer.wrotePosition;
        }
    }

    // What a generation that a resume or a start replaces still holds of the deletes of the id, for the generation
    // that replaces it to give back should that one end before it started. Nothing when the generation replaced
    // started or wrote a position, since it then keeps them for good. A signal that has not ended yet counts as a
    // start, so nothing is given back for a generation that may still subscribe to the feed.
    private List<TakeOver> stillTakenOver(InternalSubscription replaced) {
        AtomicBoolean endedBeforeItStarted = new AtomicBoolean();
        replaced.signal.asMono().subscribe(unused -> {
        }, __ -> endedBeforeItStarted.set(true)).dispose();
        if (!endedBeforeItStarted.get() || wrotePosition(replaced.writer)) {
            return List.of();
        }
        List<TakeOver> takenOver = new ArrayList<>(replaced.takenOverBefore);
        if (replaced.writer.takeOver != TakeOver.NONE) {
            takenOver.add(replaced.writer.takeOver);
        }
        return List.copyOf(takenOver);
    }

    // Gives back what reserveInternalSubscription took when the dynamic start position threw, unless a pause, a cancel
    // or a shutdown already moved or removed it. A resume puts back what it took out of the paused subscriptions, so
    // the subscription stays paused rather than being dropped from both maps. Either way the generation that threw is
    // retired, and gives back the delete it took over.
    private void releaseReservation(Reservation reservation) {
        String subscriptionId = reservation.subscriptionId();
        @Nullable InternalSubscription replaced = reservation.replaced();
        synchronized (this) {
            if (runningSubscriptions.remove(subscriptionId, reservation.subscription()) && replaced != null) {
                pausedSubscriptions.put(subscriptionId, replaced);
            }
            retire(subscriptionId, reservation.subscription());
        }
        PositionWriter writer = reservation.subscription().writer;
        giveBackPositionDelete(subscriptionId, writer.takeOver, writer);
    }

    // A shutdown ends waitUntilStarted() of a subscription that had not started by then. One that started, or that was
    // refused, keeps that outcome, since what it already signalled comes first.
    private Mono<Void> untilStartedOrShutDown(Mono<Void> started) {
        return Mono.firstWithSignal(started, shutDown.asMono().then(Mono.error(SubscriptionModelShutdownException::new)));
    }

    // Reads events from the wrapped model's cold primitive, applies the action, then persists the position after each
    // event (per the config predicate) when persist is true. currentStartAt is advanced once the action completes, so
    // that pause/resume continues from the last delivered event rather than replaying or skipping. That is before the
    // position write, so a pause while that write runs does not resume from before an event whose action already ran.
    private Flux<Void> source(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action, AtomicReference<StartAt> currentStartAt, boolean persist,
                              PositionWriter writer, Sinks.Empty<Void> startedSink) {
        Function<CloudEvent, Mono<Void>> advancing = cloudEvent -> action.apply(cloudEvent)
                .doOnSuccess(unused -> currentStartAt.set(StartAt.checkpoint(getCheckpointOrThrowIAE(cloudEvent))));
        Function<CloudEvent, Mono<Void>> delivery = persist ? persistingAction(subscriptionId, writer, advancing) : advancing;
        // A generation retired while its start position was resolved never subscribes to the feed. The swap the
        // retiring call disposes cancels what is returned instead.
        return Flux.defer(() -> retired(writer)
                ? Flux.<Void>never()
                : subscription.subscribe(filter, startAt)
                .doOnSubscribe(__ -> startedSink.tryEmitEmpty())
                .concatMap(delivery));
    }

    // Resolve the effective StartAt, mirroring the blocking DurableSubscriptionModel#generateStartAtPositionFrom:
    // the subscription-model default reads the last stored position (initializing it from the global position when
    // absent); a dynamic StartAt is resolved against this model's context and recursed, an empty result meaning "opt
    // out"; any concrete StartAt passes through unchanged.
    // positionNow is where the feed was when the subscription was registered, which is what a first run records when
    // nothing is stored. Null for a subscription handed to the wrapped model, which reads it here, on the caller's
    // thread, before it registers.
    //
    // waitEnds is null where nothing waits on this thread for the reads, and otherwise ends that wait. Only a subscribe
    // and a resume on a thread that may block wait, and a cancel, a pause, a stop or a shutdown ends the wait.
    //
    // storedAtTheCall is what storage held when the subscribe asked it, before reading where the feed is, and is read
    // in place of storage, so storage is asked once. Null where nothing asked, and storage is read here then.
    private Mono<StartAt> resolveStartAt(String subscriptionId, StartAt startAt, @Nullable Mono<Checkpoint> positionNow,
                                         @Nullable Mono<Checkpoint> positionAtRegistration, PositionWriter writer,
                                         @Nullable Mono<Void> waitEnds, @Nullable StoredAtTheCall storedAtTheCall) {
        if (startAt.isDefault()) {
            // A stored position always wins, so this only records one the first time a subscription runs. A
            // subscription registered on a stopped model brings the position it read then, which is earlier than now,
            // and nothing falls back to a fresh read behind it. That read either answered with a position or answered
            // with the reason it could not, and taking a second one here is the substitution that would skip whatever
            // was written while the subscription waited. There is no position to record then, and the wrapped model
            // applies a start position when it opens its feed rather than when it is handed one, so falling back to
            // now would begin wherever the feed had reached by then. Recording the position can be refused too, see
            // pinStartPosition below.
            //
            // A registration without positionAtRegistration takes what storage holds as it is. Its seed is positionNow
            // when this model drives the feed, read when the subscription was registered and so before the storage
            // read, and otherwise where the wrapped model is, read below once storage answered nothing.
            // pinStartPosition writes the seed on the condition that storage still holds nothing, where the storage
            // evaluates one, so a checkpoint stored after the storage read refuses the write there. One stored between
            // reading positionNow and the storage read is where the subscription starts, as one stored just before
            // the subscribe would be. A registration carrying positionAtRegistration is different: the capture already
            // happened, at registration, possibly long before this call, so whatever storage now holds may have been
            // written since, including by a checkpoint deleted and rewritten while this subscription waited to be
            // started. resolveFirstCheckpointRace reconciles the two by position instead of trusting storage.read()
            // blindly, when the storage can. Reading storage comes first and on its own, so a stored checkpoint still
            // governs exactly as it always has even when positionAtRegistration cannot be read or the storage cannot
            // compare, which the onErrorResume and defaultIfEmpty below both fall back to. See ADR 130 and #771.
            // The read the seed comes from is awaited where a subscribe or a resume waits, so a position source that
            // answers late still answers with where the feed was before the call returned and not with a position
            // after events written once it had. On a running model that read asks storage first, so a stored
            // checkpoint never waits for the wrapped model. A thread that may not block cannot wait, and a start of
            // the model waits for no read.
            //
            // Storage is read without waiting for a delete taken over, see readStoredPosition(String, TakeOver), and
            // answers the checkpoint the delete writes back where it holds nothing. Where the takeover wrote it back at
            // once, see takeOverPositionDelete, storage can answer the checkpoint the try is about to delete, or
            // nothing before the write back reaches the store. So holdStartPosition writes what was read, or the
            // checkpoint written back, before the subscription starts, and before resolveFirstCheckpointRace for a
            // registration with positionAtRegistration.
            @Nullable Mono<Checkpoint> readAtTheCall = positionAtRegistration != null ? positionAtRegistration : positionNow;
            if (readAtTheCall != null && waitEnds != null) {
                awaitAnswer(readAtTheCall, waitEnds);
            }
            TakeOver readAfter = storedAtTheCall != null ? storedAtTheCall.takeOver() : writer.takeOver;
            Mono<Checkpoint> stored = storedAtTheCall != null ? storedAtTheCall.read() : readStoredPosition(subscriptionId, readAfter);
            Mono<Checkpoint> held = readAfter.writtenBack == null ? stored
                    : stored.flatMap(checkpoint -> holdStartPosition(subscriptionId, checkpoint, readAfter, writer));
            final Mono<Checkpoint> startsFrom;
            if (positionAtRegistration != null) {
                startsFrom = held
                        .flatMap(checkpointStored -> positionAtRegistration
                                .onErrorResume(__ -> Mono.empty())
                                .flatMap(checkpoint -> resolveFirstCheckpointRace(subscriptionId, writer, checkpoint))
                                .defaultIfEmpty(checkpointStored))
                        .switchIfEmpty(Mono.defer(() -> positionAtRegistration.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint, writer))));
            } else {
                Mono<Checkpoint> seed = positionNow != null ? positionNow : positionOfTheWrappedModel(subscriptionId);
                startsFrom = held
                        .switchIfEmpty(Mono.defer(() -> seed.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint, writer))));
            }
            // A start from a checkpoint waits for the deletes taken over, so a write back that fails refuses it, as a
            // failed read of storage would. A start from the present records nothing, and opens the feed at once.
            Mono<StartAt> resolved = startsFrom
                    .flatMap(checkpoint -> readAfter.beforeStorage.thenReturn(checkpoint))
                    .map(StartAt::checkpoint);
            // Empty here means nothing is stored and the position source answered nothing, which only the config
            // override lets through (capturePositionNow and the seed above refuse it otherwise). The original
            // default is what starts the subscription then, from wherever the feed is when it opens, with nothing
            // recorded, the loss window the override accepts.
            if (!config.startWhenNoStartPositionCanBeRecorded) {
                return resolved;
            }
            return resolved.defaultIfEmpty(startAt);
        } else if (startAt.isDynamic()) {
            // Not called for a generation already retired. The swap the retiring call disposes cancels what is
            // returned instead.
            if (retired(writer)) {
                return Mono.never();
            }
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            if (nextStartAt == null) {
                return Mono.empty();
            }
            return resolveStartAt(subscriptionId, nextStartAt, positionNow, positionAtRegistration, writer, waitEnds, storedAtTheCall);
        }
        return Mono.just(startAt);
    }

    // Deferred, so storage is asked only when the read is subscribed to, which is never under the monitor
    private Mono<Checkpoint> readStoredPosition(String subscriptionId) {
        return Mono.defer(() -> storage.read(subscriptionId));
    }

    // What storage holds for the id once the position writes in flight and the deletes taken over have ended, read
    // without waiting for them. A try under way deletes nothing a takeover does not write back, and the position writes
    // of the next subscription wait for the write back. Where storage answers nothing, the read answers the checkpoint
    // that the newest position write in flight is writing, or else the one the takeover writes back. A position write
    // in flight that reaches the store after the read means the read answered an older checkpoint, and the events after
    // it are delivered again.
    private Mono<Checkpoint> readStoredPosition(String subscriptionId, TakeOver readAfter) {
        @Nullable Checkpoint restored = readAfter.restored;
        return restored == null ? readStoredPosition(subscriptionId) : readStoredPosition(subscriptionId).defaultIfEmpty(restored);
    }

    // For a subscribe that hands the subscription to the wrapped model, which reads where to start before it registers
    private PositionWriter startingPositionWriter(String subscriptionId) {
        PositionWriter writer = new PositionWriter();
        synchronized (positionLock) {
            positionWritersStarting.computeIfAbsent(subscriptionId, __ -> new HashSet<>()).add(writer);
        }
        return writer;
    }

    // Called under positionLock. A writer still registered under the id belongs to a generation the new writer
    // replaces, so it is retired here and writes nothing after this.
    private void registerWriter(String subscriptionId, PositionWriter writer) {
        PositionWriter replaced = positionWriters.put(subscriptionId, writer);
        if (replaced != null && replaced != writer) {
            replaced.retired = true;
        }
    }

    // Retires that generation and no other, so a subscribe of the same id that reserves right after keeps its writer
    private void retire(String subscriptionId, InternalSubscription generation) {
        synchronized (positionLock) {
            generation.writer.retired = true;
            positionWriters.remove(subscriptionId, generation.writer);
        }
    }

    private boolean retired(PositionWriter writer) {
        synchronized (positionLock) {
            return writer.retired;
        }
    }

    private void positionWriterNoLongerStarting(String subscriptionId, PositionWriter writer) {
        synchronized (positionLock) {
            Set<PositionWriter> starting = positionWritersStarting.get(subscriptionId);
            if (starting != null && starting.remove(writer) && starting.isEmpty()) {
                positionWritersStarting.remove(subscriptionId);
            }
        }
    }

    // Answers whenRetired without writing once a cancel has retired or overtaken the writer. Otherwise the write starts
    // here, once what the writer's takeover of a delete of the id holds its writes for has ended, and is tracked until
    // it ends. A storage can apply a
    // write after its caller stopped waiting, so the write is subscribed here rather than by the caller, and disposing
    // the cancelled subscription does not end the tracking early. It is subscribed after the lock is released, since a
    // storage that blocks while subscribing would otherwise hold up every subscription.
    private <T> Mono<T> writePosition(String subscriptionId, PositionWriter writer, Checkpoint writing, Supplier<Mono<T>> write, Mono<T> whenRetired) {
        return writePosition(subscriptionId, writer, writing, write, whenRetired, true);
    }

    // resolveFirstCheckpointRace answers nothing when it cannot compare the two positions and wrote nothing, unlike a
    // save, where an empty answer does not say whether the store applied it
    private Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, PositionWriter writer, Checkpoint candidate) {
        return writePosition(subscriptionId, writer, candidate, () -> storage.resolveFirstCheckpointRace(subscriptionId, candidate), Mono.just(candidate), false);
    }

    private <T> Mono<T> writePosition(String subscriptionId, PositionWriter writer, Checkpoint writing, Supplier<Mono<T>> write, Mono<T> whenRetired,
                                      boolean emptyMayHaveWritten) {
        return writer.takeOver.beforeStorage.then(Mono.defer(() -> {
            Sinks.Empty<Void> ended = Sinks.empty();
            Mono<Void> writeEnded = ended.asMono();
            synchronized (positionLock) {
                // An overtaken writer belongs to a subscribe that a cancel ended before it was registered, so what it
                // would write is a position of a subscription that no longer exists
                if (writer.retired || writer.overtakenByCancel) {
                    return whenRetired;
                }
                positionWritesInFlight.computeIfAbsent(subscriptionId, __ -> new LinkedHashMap<>()).put(writeEnded, writing);
            }
            Mono<T> started = Mono.defer(write)
                    // Before the write counts as ended, so whoever waits for that end reads it
                    .doOnSuccess(answer -> {
                        if (answer != null || emptyMayHaveWritten) {
                            synchronized (positionLock) {
                                writer.wrotePosition = true;
                            }
                        }
                    })
                    .doFinally(__ -> {
                        positionWriteEnded(subscriptionId, writeEnded);
                        ended.tryEmitEmpty();
                    })
                    .cache();
            // A failed write reaches the caller through started, and only its end matters here
            started.subscribe(unused -> {
            }, throwable -> {
            });
            return started;
        }));
    }

    private void positionWriteEnded(String subscriptionId, Mono<Void> writeEnded) {
        synchronized (positionLock) {
            @Nullable SequencedMap<Mono<Void>, Checkpoint> inFlight = positionWritesInFlight.get(subscriptionId);
            if (inFlight != null && inFlight.remove(writeEnded) != null && inFlight.isEmpty()) {
                positionWritesInFlight.remove(subscriptionId);
            }
        }
    }

    // Writes the start position read after a takeover that wrote a checkpoint back at once, at the version of the
    // position writes, so neither the try of the delete nor the write back removes or replaces it. A refusal means a
    // checkpoint at a higher version is stored, and the subscription starts from that one.
    private Mono<Checkpoint> holdStartPosition(String subscriptionId, Checkpoint read, TakeOver readAfter, PositionWriter writer) {
        CheckpointWriteCondition condition = requireNonNull(readAfter.writeCondition, "writeCondition");
        return writePosition(subscriptionId, writer, read, () -> storage.save(subscriptionId, read, condition), Mono.just(read))
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Checkpoint storage " + storage.getClass().getName() +
                                                                           " answered nothing when asked to write " + read.asString() +
                                                                           " as the start position of subscription " + subscriptionId +
                                                                           ", instead of the checkpoint it wrote. Nothing here can show it " +
                                                                           "was written, and a delete of the id still under way could remove " +
                                                                           "what storage holds, so the subscription is refused. Answer with " +
                                                                           "the checkpoint that was written, which is what " +
                                                                           "CheckpointStorage.save(..) returns it for.")))
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        refused -> readStoredPosition(subscriptionId).switchIfEmpty(Mono.error(refused)));
    }

    // The read above found nothing, so this is the first position recorded for this subscription id, and the write
    // is conditional on that still being true when it reaches storage. A refused write therefore means a checkpoint
    // arrived between the read and the write, written where this model cannot order it against the position it read,
    // unless storage.resolveFirstCheckpointRace can order it after all. That position is read after the storage read
    // for a subscription handed to the wrapped model, and at registration, before the storage read, when this model
    // drives the feed. A subscription registered while the model was stopped keeps it as positionAtRegistration,
    // and resolveStartAt reconciles that one separately, against whatever storage.read() finds, since this method is
    // never reached for it when something is already stored. See ADR 130 and #771.
    private Mono<Checkpoint> pinStartPosition(String subscriptionId, Checkpoint positionRead, PositionWriter writer) {
        return recordFirstPosition(subscriptionId, positionRead, writer)
                .switchIfEmpty(Mono.error(() -> storageAnsweredNothingAboutItsOwnWrite(subscriptionId, positionRead)));
    }

    private Mono<Checkpoint> recordFirstPosition(String subscriptionId, Checkpoint positionRead, PositionWriter writer) {
        if (!storage.evaluatesWriteConditionsFor(subscriptionId)) {
            // Nothing here can make a storage that writes unconditionally do otherwise, so the write is the one
            // 0.32.0 made and two nodes recording a first position at the same moment keep the race. Logged rather
            // than refused, because refusing would take out a storage that has worked until now over a capability
            // it never claimed. A write that succeeds is not attempted again, so this reads once per subscription.
            log.warn("Checkpoint storage {} does not evaluate write conditions for subscription {}, so the first " +
                     "position recorded for it is written unconditionally. Two nodes recording a first position " +
                     "for this subscription at the same moment can then lose the events between the two positions. " +
                     "Answer true from evaluatesWriteConditionsFor(String) on a storage that does evaluate " +
                     "ifAbsent(), or use one of the storages Occurrent ships, to close that.",
                    storage.getClass().getName(), subscriptionId);
            return writePosition(subscriptionId, writer, positionRead, () -> storage.save(subscriptionId, positionRead), Mono.just(positionRead));
        }
        return writePosition(subscriptionId, writer, positionRead, () -> storage.save(subscriptionId, positionRead, CheckpointWriteCondition.ifAbsent()), Mono.just(positionRead))
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        // Asked first, because a storage able to compare the two settles this by position instead of
                        // by write order, with no exception either way. Falls through to the older, narrower rule
                        // only when the storage answers empty, meaning it cannot make that comparison.
                        __ -> resolveFirstCheckpointRace(subscriptionId, writer, positionRead)
                                .switchIfEmpty(Mono.defer(() -> refuseUnlessTheStoredPositionIsTheOneRead(subscriptionId, positionRead))));
    }

    // save is documented to hand the checkpoint back for chaining, so a storage that answers nothing has told this
    // model neither that the position was recorded nor that it was not. Refused, because the alternative is starting
    // from wherever the feed has reached and skipping whatever a recorded position would have kept.
    private IllegalStateException storageAnsweredNothingAboutItsOwnWrite(String subscriptionId, Checkpoint positionRead) {
        return new IllegalStateException("Checkpoint storage " + storage.getClass().getName() + " answered nothing when " +
                                         "asked to record " + positionRead.asString() + " as the first position for " +
                                         "subscription " + subscriptionId + ", instead of the checkpoint it wrote. " +
                                         "Nothing here can show the position was recorded, and starting the subscription " +
                                         "anyway would begin wherever the feed has reached, so the registration is " +
                                         "refused. Answer with the checkpoint that was written, which is what " +
                                         "CheckpointStorage.save(..) returns it for.");
    }

    // Reading it back answers the only question that settles the registration, whether it holds the position this
    // one read. Anything else is refused rather than started from a position this registration never read, which
    // would skip whatever lies between the two. onErrorMap sits on the read, upstream of the comparison, so it
    // cannot re-wrap the refusals below it.
    private Mono<Checkpoint> refuseUnlessTheStoredPositionIsTheOneRead(String subscriptionId, Checkpoint positionRead) {
        return readStoredPosition(subscriptionId)
                .onErrorMap(throwable -> StartPositionAlreadyPinnedException
                        .readingTheStoredPositionBackFailed(subscriptionId, positionRead, throwable))
                .flatMap(stored -> positionRead.asString().equals(stored.asString())
                        ? Mono.just(stored)
                        : Mono.error(() -> new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored)))
                .switchIfEmpty(Mono.error(() -> StartPositionAlreadyPinnedException
                        .readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead)));
    }

    /**
     * Pause a subscription.
     * <p>
     * When this model drives the subscription itself, a pause ends a subscription that is still resolving its start
     * position, in the same way that {@link #cancelSubscription(String)} does, except that its checkpoint stays stored
     * and the subscription stays paused. The
     * {@link Subscription#waitUntilStarted()} it returned fails with {@link java.util.concurrent.CancellationException},
     * and resuming it returns a new {@link Subscription} to wait on. When this model hands subscriptions to a wrapped
     * model that manages named subscriptions, that model pauses it.
     * <p>
     * Such a subscription can start again in the wrapped model from an earlier position, when the position recorded for
     * it is earlier than the one it was first handed. A pause made meanwhile succeeds, and the subscription delivers no
     * event until it is resumed.
     */
    @Override
    public void pauseSubscription(String subscriptionId) {
        if (delegate != null) {
            // Like every call this model forwards to the wrapped model, made without the monitor, which the wrapped
            // model does not need and a slow wrapped model would otherwise hold for every other id
            passOrKeep(delegate, subscriptionId, true);
            return;
        }
        pauseInternalSubscription(subscriptionId);
    }

    // Passes a pause or a resume to the wrapped model, or keeps the state asked for while startAgainInWrappedModel
    // starts the subscription there again. A call that passes is counted against every subscription of the id that can
    // still start again, from the mark of its hand-over on, so a start again that begins meanwhile reads the state of
    // the wrapped model only once the call has returned.
    private @Nullable Subscription passOrKeep(SubscriptionModel delegate, String subscriptionId, boolean pause) {
        final @Nullable KeptLifecycle keeping;
        final List<KeptLifecycle> passing = new ArrayList<>();
        synchronized (positionLock) {
            keeping = keeping(subscriptionId);
            if (keeping == null) {
                countPassing(subscriptionId, passing);
            }
        }
        if (keeping != null) {
            return keep(delegate, subscriptionId, keeping, pause);
        }
        try {
            return pass(delegate, subscriptionId, pause);
        } finally {
            passing.forEach(this::passed);
        }
    }

    // Called under positionLock. Counts a call that passes to the wrapped model against each subscription of the id
    // that can still start again, registered or still being handed over, and adds them to passing.
    private void countPassing(String subscriptionId, List<KeptLifecycle> passing) {
        Set<PositionWriter> writers = new HashSet<>(positionWritersStarting.getOrDefault(subscriptionId, Set.of()));
        @Nullable PositionWriter registered = positionWriters.get(subscriptionId);
        if (registered != null) {
            writers.add(registered);
        }
        for (PositionWriter writer : writers) {
            @Nullable KeptLifecycle kept = writer.kept;
            if (kept != null && !kept.keeping) {
                kept.passing++;
                passing.add(kept);
            }
        }
    }

    private static @Nullable Subscription pass(SubscriptionModel delegate, String subscriptionId, boolean pause) {
        if (pause) {
            delegate.pauseSubscription(subscriptionId);
            return null;
        }
        return delegate.resumeSubscription(subscriptionId);
    }

    // Keeps the state asked for, refused as the wrapped model would refuse it when it is the state kept already. Until
    // startAgainInWrappedModel has read it, that state is the one the wrapped model has, which still holds the
    // subscription then. A resume returns a handle that ends once the wrapped model has the state kept.
    private @Nullable Subscription keep(SubscriptionModel delegate, String subscriptionId, KeptLifecycle kept, boolean pause) {
        final @Nullable Boolean known;
        synchronized (positionLock) {
            known = kept.paused;
        }
        boolean wrappedPaused = known == null && wrappedIsPaused(delegate, subscriptionId);
        final boolean ended;
        Sinks.@Nullable Empty<Void> unpaused = null;
        synchronized (positionLock) {
            ended = !kept.keeping;
            if (!ended) {
                boolean paused = kept.paused != null ? kept.paused : wrappedPaused;
                if (pause && paused) {
                    throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
                } else if (!pause && !paused) {
                    throw new SubscriptionAlreadyRunningException(subscriptionId);
                }
                unpaused = kept.ask(pause);
            }
        }
        if (ended) {
            // The wrapped model has the subscription again, and the state kept until then
            return pass(delegate, subscriptionId, pause);
        }
        if (unpaused != null) {
            unpaused.tryEmitEmpty();
        }
        return pause ? null : new ReactorDurableSubscription(subscriptionId, untilStartedOrShutDown(kept.applied.asMono()));
    }

    private void pauseInternalSubscription(String subscriptionId) {
        final @Nullable InternalSubscription internalSubscription;
        synchronized (this) {
            if (shutdown) {
                throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
            }
            requireKnown(subscriptionId);
            if (isPaused(subscriptionId)) {
                throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
            } else if (!isRunning(subscriptionId)) {
                throw new SubscriptionNotRunningException(subscriptionId);
            }
            internalSubscription = runningSubscriptions.remove(subscriptionId);
            if (internalSubscription != null) {
                pausedSubscriptions.put(subscriptionId, internalSubscription);
                retire(subscriptionId, internalSubscription);
            }
        }
        // After the monitor is released, since disposing cancels the subscription to the wrapped model's feed
        if (internalSubscription != null) {
            pausedBeforeStarting(subscriptionId, internalSubscription);
        }
    }

    // Called after the monitor is released for a generation that a pause or a stop retired under it. Like
    // endBeforeItStarted, it also ends the handle of the call that began the run when that one still waits. A start of
    // this model gives the run a generation of its own, and that handle would otherwise wait for good.
    private static void pausedBeforeStarting(String subscriptionId, InternalSubscription paused) {
        paused.disposable.dispose();
        endBeforeItStarted(paused, new CancellationException("Subscription " + subscriptionId + " was paused before it started"));
    }

    /**
     * Start a subscription that was registered while this model was stopped, or resume one that was paused.
     * <p>
     * A subscription whose position could not be read when it was registered is refused here rather than started, and
     * the refusal is signalled on the returned {@link Subscription#waitUntilStarted()}. Such a subscription is dropped
     * from this model, so asking again answers with {@link UnknownSubscriptionException} and getting it back means
     * registering it again.
     * <p>
     * A resume of a subscription that is starting again in a wrapped model, as {@link #pauseSubscription(String)}
     * describes, succeeds, and the returned {@link Subscription#waitUntilStarted()} ends once the wrapped model runs it.
     *
     * @throws UnknownSubscriptionException        If this model has no such subscription.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (delegate != null) {
            return requireNonNull(passOrKeep(delegate, subscriptionId, false));
        }
        return resumeInternalSubscription(subscriptionId);
    }

    private Subscription resumeInternalSubscription(String subscriptionId) {
        final Reservation reservation;
        synchronized (this) {
            if (shutdown) {
                throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
            }
            requireKnown(subscriptionId);
            if (isRunning(subscriptionId)) {
                throw new SubscriptionAlreadyRunningException(subscriptionId);
            }
            InternalSubscription paused = pausedSubscriptions.remove(subscriptionId);
            if (paused == null) {
                throw new SubscriptionNotRunningException(subscriptionId);
            }
            running = true;
            reservation = reserveToResume(subscriptionId, paused);
        }
        try {
            return startReserved(reservation, false, null);
        } catch (RuntimeException | Error e) {
            // A dynamic StartAt that throws keeps the subscription paused rather than dropped from both maps
            releaseReservation(reservation);
            throw e;
        }
    }

    // Reuses the same currentStartAt reference so resume continues from the position of the last event delivered
    // before the subscription was paused, rather than replaying (or skipping) from the original StartAt.
    // The paused generation is retired here too, which stops a position read that its own start has not begun yet.
    private Reservation reserveToResume(String subscriptionId, InternalSubscription paused) {
        retire(subscriptionId, paused);
        return reserveInternalSubscription(subscriptionId, paused.filter, paused.currentStartAt, paused.action, paused.positionNow, paused.positionAtRegistration, paused,
                paused.readsAbandoned, stillTakenOver(paused));
    }

    /**
     * Cancel a subscription. Its action is not called for any further event, and its persisted checkpoint is deleted.
     * <p>
     * A call of the action that is already running when this is called is not waited for, by this method or by the
     * returned {@link Mono}, so it can still be running after the {@code Mono} completes. No checkpoint is written for
     * the event it handles. Since the checkpoint is deleted, a later subscribe of the id delivers that event again only
     * when its own start position begins before it. Waiting for the call would let one action that never ends hold up
     * the cancel.
     * <p>
     * The cancel takes effect when this method is called, whether or not anything subscribes to the returned {@link
     * Mono}. The {@code Mono} completes once the checkpoint is deleted, or once a subscribe of the same id has taken
     * the delete over, see below, and, when this model wraps a model that manages named subscriptions, once that
     * model's own cancel has completed, see below for a subscribe that model is taking meanwhile. A delete that fails
     * is tried again until it succeeds, a subscribe of the id takes it over or the model is shut down. The wait before
     * a try starts at 100 milliseconds and about doubles after each failure, never past 5 seconds, with some
     * randomness so that deletes failing together are not tried again together. Until then, the {@code Mono} neither
     * completes nor fails. It fails with the error of the last try when a shutdown stopped the tries, and with the
     * error of that model's cancel when that cancel fails. Once it completes with no subscribe of the id taking the
     * delete over, the store holds no checkpoint that the cancelled subscription wrote, so a later subscribe of the
     * same id does not resume from where the cancelled one got to, in this process or after a restart. The delete runs
     * after every checkpoint write the cancelled subscription had already started, and a write it had not started by
     * then never runs.
     * <p>
     * Wait for the returned {@code Mono} before subscribing the same id again, to start it clean. A subscribe of the id
     * in this process that comes before the delete has ended takes the delete over. The delete then makes no further
     * try, and the subscribe writes back the checkpoint that a try under way read, so the store holds what it held
     * before that try. The subscription starts as it would with no delete running, from the checkpoint of the
     * cancelled subscription unless an earlier try had already deleted it. A subscription from the subscription-model
     * default then resumes from that checkpoint, and so does a function that reads the checkpoint itself, as
     * {@code ResumeStartPositions.replayThenResume(..)} does. Its start position is resolved at the call, a
     * {@link StartAt#dynamic(java.util.function.Supplier) dynamic} one included. When a subscribe, a resume or a start
     * of the id fails once it has taken the delete over, as a subscribe that Reactor refuses on a thread that may not
     * block does, no subscription needs the checkpoint any more. The same goes for a subscription that took the delete
     * over and ends before it started without writing a checkpoint, as one whose start position cannot be read does.
     * The delete then goes ahead and removes the checkpoint, unless another subscription of the id is starting or
     * registered by then, or the model is shut down. A subscription that a pause ended before it started keeps the
     * delete taken over until it is resumed, and the delete goes ahead when the resumed one ends that way. When the
     * call fails, or the subscription ends, before the delete has ended, the returned {@code Mono} ends only once the
     * delete that goes ahead has ended. With a storage that evaluates no condition on a delete, the checkpoint a try
     * under way read is then not written back, when the call failed or the subscription ended before that try ended.
     * <p>
     * How long the subscription waits for the delete depends on the storage. One that evaluates a condition on a
     * delete, see {@link CheckpointStorage#evaluatesDeleteConditions()}, deletes on the condition that the stored
     * version is not above the one each try read just before. The subscribe writes the checkpoint back at once, at the
     * version after that one, so the try under way deletes nothing, or what it deleted is back. The subscription writes
     * its own checkpoints at once too, at the version after the write back, so neither the try nor the write back
     * removes or replaces them. Neither the subscribe, the delivery of an event nor a checkpoint write waits for the
     * try or the write back then.
     * <p>
     * A read of storage can then answer the checkpoint the try is about to delete, or nothing once the try deleted it
     * and before the write back reached the storage. So a subscription from the subscription-model default writes the
     * checkpoint it read, or the one written back when it read nothing, at the version of its own checkpoints before it
     * starts. A subscription registered while the model was stopped writes it the same way before {@link
     * CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint)} compares it with where the feed was at
     * registration. When that write is refused, storage holds a checkpoint at a higher version, and the subscription
     * reads storage again and starts from that one. When it fails, the subscription fails to start, as it would on a
     * failed read of storage. A write back that fails fails no subscription. When the model hands the subscription to a
     * wrapped model that manages named subscriptions, that write runs after this {@code subscribe(..)} has returned,
     * and an event that model delivers before the write has ended waits for it. A refused write keeps that
     * subscription at the position it was handed, which can make it handle events again that the checkpoint stored
     * covers.
     * <p>
     * With any other storage, nothing stops the try under way from deleting what is stored, so the checkpoint is
     * written back after that try ends. The subscribe does not wait for it. It reads storage at the call, and when the
     * try has already deleted the checkpoint, it starts from the checkpoint that try read. The subscription writes a
     * checkpoint only once the write back has ended, and a resume or a start of the id that comes before then takes the
     * delete over too. A write back that fails fails the first checkpoint write, and fails a subscription that starts
     * from a checkpoint, as a failed read of storage would.
     * <p>
     * With either storage, that read does not wait for the checkpoint writes the delete runs after. When storage holds
     * nothing, the subscription starts from the checkpoint the newest of those writes is writing, and with none of them
     * from where the feed is at the call, as it does with no delete running. A write that reaches the store after the
     * read makes the subscription start from an earlier checkpoint than the last one the cancelled subscription
     * wrote, so it handles those events again. The subscription writes a checkpoint only once those writes have ended.
     * When this model drives the feed itself, a subscription that starts from a checkpoint opens the feed once they
     * have ended, and with any other storage once the write back has ended too. One that starts from where the feed is
     * opens it at the call. When the model hands the subscription to a wrapped model that manages named subscriptions,
     * that model gets the position read at the call, and an event it delivers before the position is recorded waits for
     * that. When storage records an earlier position for the id than the one read, as
     * {@link CheckpointStorage#resolveFirstCheckpointRace(String, Checkpoint)} can answer, the subscription is
     * cancelled in that model and subscribed there again from the earlier position, so it handles the events between
     * the two. When the position cannot be recorded, or that second subscribe fails, the subscription is cancelled in
     * that model, and its {@link Subscription#waitUntilStarted()} fails with the error, since this
     * {@code subscribe(..)} has returned by then.
     * <p>
     * With a storage that evaluates no condition on a delete, the process can end after a try deleted the checkpoint
     * and before the write back reached the storage. The storage then holds no checkpoint for the id, since the
     * checkpoint writes of the subscription wait for the write back, and a later subscribe from the subscription-model
     * default starts from where the feed is then. With a storage that evaluates one, a subscription from the
     * subscription-model default writes its start position before it handles an event, so once it has handled one, the
     * next subscription from the subscription-model default resumes from that start position or a later checkpoint,
     * after a restart too. Should the process end before that write, the storage can hold no checkpoint for the id,
     * and the next one starts from where the feed is then.
     * <p>
     * This cancel ends every subscription of the id that it finds, including one that has not started yet since it is
     * still resolving its start position. Such a subscription writes no checkpoint once this is called. A step toward
     * starting it that passed its last check before this was called can still begin after this returns, since none of
     * these steps runs under a lock this method takes, and this method does not wait for a function the caller supplied
     * or for the wrapped model. The steps are resolving its dynamic start position, reading the checkpoint, subscribing
     * to the feed and handing it to a wrapped model that manages named subscriptions. Each runs to its end, and this
     * model discards its result. Its {@link Subscription#waitUntilStarted()} fails with {@link
     * java.util.concurrent.CancellationException}, unless the subscription had started by then. When this model drives
     * the subscription itself, it finds a subscribe as soon as that subscribe has taken the id, before the start
     * position is resolved. When this model hands the subscription to such a wrapped model, it finds a subscribe before
     * that subscribe reads where to start, so one still reading and one that model is taking are both ended. The
     * subscription that model makes while this runs is cancelled there once that model has taken it, and the returned
     * {@code Mono} completes only after that. When that cancel fails, the subscription can still be in that model, so
     * the returned {@code Mono} fails with that error. Either way, this model does not run the action of a subscription
     * this ends for an event that model delivers after this returns.
     * <p>
     * Neither the delete nor its tries has a time limit. The delete waits for the checkpoint writes it runs after,
     * however long the storage takes to answer them, and its tries go on until one succeeds or a subscribe of the id
     * takes the delete over. A storage that does not answer those writes holds up the checkpoint writes of a
     * subscription of the same id, and its start from a checkpoint or the events a wrapped model delivers to it, until
     * it answers, until the id is cancelled again or until the model is shut down. So does one that evaluates no
     * condition on a delete and does not answer the try under way or the write of the checkpoint back. One that
     * evaluates a condition and does not answer the write back holds up no subscription.
     * With either storage, the delete of a later cancel of the id runs only once the write back answers, so that
     * cancel's {@code Mono} does not complete until then or until the model is shut down.
     * <p>
     * The returned {@code Mono} is cached. Each failed try of the delete is also logged as a warning, whether or not
     * anything subscribes to the {@code Mono}.
     * <p>
     * When a shutdown stopped the tries, the subscription stays cancelled and the checkpoint stays stored, so call this
     * again once a model runs. Do the same after a restart when the process ended before the {@code Mono} completed.
     * It deletes the checkpoint for an id this model has never subscribed to.
     *
     * @param subscriptionId The subscription id to cancel
     * @return A {@code Mono} that completes once the checkpoint is deleted or a subscribe of the id has taken the
     * delete over
     */
    @Override
    public Mono<Void> cancelSubscription(String subscriptionId) {
        final Mono<Void> delete;
        final Mono<Void> wrappedModelCancelled;
        if (delegate != null) {
            List<Mono<Void>> handOversEnded = new ArrayList<>();
            // Installed before the wrapped model is asked, so a subscribe of the id still reading where to start is
            // ended, and one the wrapped model is taking right now is ended too, once it has cancelled the subscription
            // there itself
            delete = deleteStoredCheckpoint(subscriptionId, handOversEnded);
            try {
                // Subscribed here and cached, so the wrapped model cancels even when nothing subscribes to what this
                // returns, and a wrapped model whose Mono does nothing until subscribed is not cancelled twice
                wrappedModelCancelled = delegate.cancelSubscription(subscriptionId).cache();
                wrappedModelCancelled.subscribe(unused -> {
                }, throwable -> log.warn("Could not cancel subscription {} in the wrapped model {}. That model may still hold the subscription, though this model no longer runs its action.",
                        subscriptionId, delegate.getClass().getName(), throwable));
            } finally {
                // Started even when the wrapped model throws, since the checkpoint is deleted whatever that model answers
                startDelete(subscriptionId, delete);
            }
            return Mono.when(wrappedModelCancelled, delete, Mono.when(handOversEnded)).cache();
        } else {
            final @Nullable InternalSubscription runningSubscription;
            final @Nullable InternalSubscription pausedSubscription;
            synchronized (this) {
                runningSubscription = runningSubscriptions.remove(subscriptionId);
                // A paused subscription can hold a position read that is still in flight, which shutdown already disposes.
                pausedSubscription = pausedSubscriptions.remove(subscriptionId);
                // Under the monitor, so it retires the generations removed above and not the one a subscribe of the
                // same id reserves right after
                if (runningSubscription != null) {
                    retire(subscriptionId, runningSubscription);
                }
                if (pausedSubscription != null) {
                    retire(subscriptionId, pausedSubscription);
                }
                delete = deleteStoredCheckpoint(subscriptionId, new ArrayList<>());
            }
            // Outside the monitor, since disposing cancels the subscription to the wrapped model's feed and starting the
            // delete calls the storage. The writers are retired already, so nothing the subscription delivers until it
            // is disposed writes a position, and no step toward starting it begins.
            for (InternalSubscription cancelled : Arrays.asList(runningSubscription, pausedSubscription)) {
                if (cancelled != null) {
                    cancelled.disposable.dispose();
                    endBeforeItStarted(cancelled, cancelledBeforeItStarted(subscriptionId));
                    // After the dispose, so the generation it cancelled does not hear of the read ending
                    cancelled.readsAbandoned.tryEmitError(cancelledBeforeItStarted(subscriptionId));
                    // Before the delete of this cancel starts, so that delete is the latest of the id and the deletes
                    // given back here only stop counting the generation, see giveBackPositionDelete
                    giveBackTakeOvers(subscriptionId, cancelled);
                }
            }
            startDelete(subscriptionId, delete);
            wrappedModelCancelled = Mono.empty();
        }
        return Mono.when(wrappedModelCancelled, delete).cache();
    }

    // Started by startDelete rather than by whoever subscribes to the result, so the delete runs even when nobody waits
    // for it. Runs after the position writes already in flight and after an earlier delete of the id, since either
    // ending after this delete would leave a position stored. It is put in the map in the same step that retires the
    // writer, and completes only once it is taken out and what a takeover of it writes has ended, so a later delete of
    // the id never runs before that write.
    // Adds what completes once each hand-over of the id to the wrapped model under way has ended to handOversEnded.
    private Mono<Void> deleteStoredCheckpoint(String subscriptionId, List<Mono<Void>> handOversEnded) {
        // A cancel that comes after the shutdown still tries the delete once, and one that came before it tries no more
        // once the shutdown has come
        boolean requestedAfterShutdown = shutdown;
        // Asked before the lock is taken, which is never held while calling the storage
        boolean conditional = storage.evaluatesDeleteConditions() && storage.evaluatesWriteConditionsFor(subscriptionId);
        final PositionDelete delete;
        final List<PositionWriter> overtaken = new ArrayList<>();
        final @Nullable PositionWriter cancelled;
        synchronized (positionLock) {
            cancelled = positionWriters.remove(subscriptionId);
            if (cancelled != null) {
                cancelled.retired = true;
                // A subscription handed to the wrapped model that is being started there again, see
                // startAgainInWrappedModel
                Sinks.@Nullable Empty<Void> handingOver = cancelled.handingOver;
                if (handingOver != null) {
                    handOversEnded.add(handingOver.asMono());
                }
            }
            // A subscribe still reading where to start or being handed over is ended at its next check
            positionWritersStarting.getOrDefault(subscriptionId, Set.of()).forEach(writer -> {
                writer.overtakenByCancel = true;
                overtaken.add(writer);
                Sinks.@Nullable Empty<Void> handingOver = writer.handingOver;
                if (handingOver != null) {
                    handOversEnded.add(handingOver.asMono());
                }
            });
            delegatedSubscriptionIds.remove(subscriptionId);
            Mono<Void> writesInFlight = Mono.when(new ArrayList<>(positionWritesInFlight.getOrDefault(subscriptionId, new LinkedHashMap<>()).sequencedKeySet()));
            delete = new PositionDelete(writesInFlight, conditional, positionDeletes.get(subscriptionId), null);
            positionDeletes.put(subscriptionId, delete);
        }
        // Outside the lock, since what waits on it cancels a read in the wrapped model
        overtaken.forEach(writer -> writer.overtaken.tryEmitError(new OvertakenByCancel(subscriptionId)));
        // A subscription handed to the wrapped model no longer counts against the deletes it took over. This delete
        // is the latest of the id and has not started, so they only stop counting it, see giveBackPositionDelete.
        // A generation this model drives gives them back in cancelSubscription.
        if (cancelled != null && delegate != null) {
            giveBackPositionDelete(subscriptionId, cancelled.takeOver, null);
        }
        return runPositionDelete(subscriptionId, delete, requestedAfterShutdown);
    }

    private Mono<Void> runPositionDelete(String subscriptionId, PositionDelete delete, boolean requestedAfterShutdown) {
        // No time limit on either wait. A write this stopped waiting for could still reach the store after the delete,
        // and a delete this stopped waiting for could still remove what the next subscription of the id wrote. A
        // shutdown ends the wait of a delete requested before it, which then makes no try at all.
        @Nullable PositionDelete earlier = delete.earlier;
        @Nullable Mono<Void> runsAfter = delete.runsAfter;
        Mono<Void> endedFirst = Mono.when(delete.writesInFlight, runsAfter != null ? runsAfter : earlier == null ? Mono.empty() : earlier.ended.asMono());
        Mono<Void> tries = (requestedAfterShutdown ? endedFirst : Mono.firstWithSignal(endedFirst, shutDown.asMono()))
                .then(Mono.defer(() -> deleteUntilItSucceeds(subscriptionId, delete, requestedAfterShutdown)));
        // Taken out before the caller hears of the end, so a subscribe made once the cancel completed finds no delete. A
        // delete taken over is taken out only once what the takeover writes has ended, so a subscribe, a resume or a
        // start of the id that comes meanwhile takes it over too, and reads and writes as the takeover lets it, and a
        // later delete of the id runs after that write. A takeover write that fails reaches the subscription that took
        // the delete over, not the cancel, see writeBack.
        Mono<Void> writtenBack = Mono.defer(() -> {
                    final @Nullable Mono<Void> takeOver;
                    synchronized (positionLock) {
                        takeOver = delete.takeOver;
                        if (takeOver == null) {
                            positionDeletes.remove(subscriptionId, delete);
                            delete.earlier = null;
                        }
                    }
                    return takeOver == null ? Mono.<Void>empty() : takeOver.onErrorResume(__ -> Mono.empty()).then(Mono.fromRunnable(() -> {
                        synchronized (positionLock) {
                            positionDeletes.remove(subscriptionId, delete);
                            delete.earlier = null;
                        }
                    }));
                })
                .doFinally(__ -> delete.writtenBack.tryEmitEmpty());
        // Waits for the delete that goes ahead in place of this delete only when giveBackPositionDelete set it before
        // this point
        Mono<Void> goneAhead = Mono.defer(() -> {
            final @Nullable Mono<Void> inPlaceOfThis;
            synchronized (positionLock) {
                delete.isEnding = true;
                inPlaceOfThis = delete.goneAhead;
            }
            return inPlaceOfThis == null ? Mono.<Void>empty() : inPlaceOfThis;
        });
        return tries.then(writtenBack).onErrorResume(failure -> writtenBack.then(Mono.error(failure)))
                .then(goneAhead)
                .doFinally(__ -> delete.ended.tryEmitEmpty())
                .cache();
    }

    // A delete that fails has not removed the checkpoint, and a subscribe after it would resume from it, so it is tried
    // again until it succeeds or a subscription of the id takes it over. Otherwise only a shutdown stops that, after
    // which nothing in this process reads the checkpoint. It ends the wait before the next try at once, and the flag it
    // sets is read right before each try, so a try that has not read it by then never calls the storage. A try that
    // read it just before runs to its end, as one already under way does. The error of the last try ends the delete.
    // One that never tried ends with the last error of the delete it ran after, or with
    // SubscriptionModelShutdownException when there is none.
    private Mono<Void> deleteUntilItSucceeds(String subscriptionId, PositionDelete delete, boolean requestedAfterShutdown) {
        Mono<Void> attempt = Mono.defer(() -> shutdown && !requestedAfterShutdown
                        ? Mono.<Void>error(stoppedBeforeItsTurn(delete))
                        : tryDelete(subscriptionId, delete))
                .doOnError(delete.lastFailure::set);
        return attempt.retryWhen(Retry.from(retries -> retries.concatMap(retry -> {
            // Read here, since Reactor reuses the signal once this returns
            Throwable failure = retry.failure();
            long failedAttempts = retry.totalRetries() + 1;
            if (shutdown) {
                return Mono.error(failure);
            }
            log.warn("Failed to delete the stored checkpoint of cancelled subscription {} on attempt {}. Trying again. Until a delete succeeds, a subscribe of the id can still resume from that checkpoint.",
                    subscriptionId, failedAttempts, failure);
            // A takeover ends the wait too, and the next try then makes none
            return Mono.firstWithSignal(Mono.delay(retryAfter(failedAttempts)), delete.takenOverSignal.asMono().then(Mono.just(0L)),
                    shutDown.asMono().then(Mono.<Long>error(failure)));
        })));
    }

    // One try, which deletes nothing once a subscription of the id has taken the delete over. It reads the stored
    // checkpoint first, so a takeover while it runs knows what to write back. Where the delete is conditional, the try
    // also reads the version and deletes on the condition that the version is not above the one it read, so that
    // write can go at a higher version while the try runs. A refusal means that write came first, and ends the delete
    // as a success does. Otherwise the takeover writes the checkpoint back once the try has ended.
    private Mono<Void> tryDelete(String subscriptionId, PositionDelete delete) {
        if (!delete.conditional) {
            return Mono.defer(() -> takenOver(delete)
                    ? Mono.<Void>empty()
                    : storage.read(subscriptionId)
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(stored -> deleteUnlessTakenOver(delete, stored.orElse(null), Long.MAX_VALUE, () -> storage.delete(subscriptionId))));
        }
        return Mono.defer(() -> takenOver(delete)
                ? Mono.<Void>empty()
                : storage.writeVersion(subscriptionId)
                .map(OptionalLong::of)
                .defaultIfEmpty(OptionalLong.empty())
                .flatMap(version -> storage.read(subscriptionId)
                        .flatMap(stored -> deleteUnlessTakenOver(delete, stored, version.orElse(0),
                                () -> storage.delete(subscriptionId, CheckpointWriteCondition.notOlderThan(version.orElse(0)))
                                        .onErrorResume(CheckpointWriteConditionNotFulfilledException.class, __ -> Mono.empty())))));
    }

    // Marks the try under way, with the checkpoint and the version it read, in the same step that checks for a
    // takeover, so a takeover that comes after it knows what to write back and one that came before stops it
    private Mono<Void> deleteUnlessTakenOver(PositionDelete delete, @Nullable Checkpoint stored, long version, Supplier<Mono<Void>> deleting) {
        return Mono.defer(() -> {
            Sinks.Empty<Void> underWay = Sinks.empty();
            synchronized (positionLock) {
                if (delete.isTakenOver) {
                    return Mono.<Void>empty();
                }
                delete.tryUnderWay = underWay;
                delete.storedWhenTried = stored;
                delete.versionWhenTried = version;
            }
            return Mono.defer(deleting).doFinally(__ -> {
                synchronized (positionLock) {
                    if (delete.tryUnderWay == underWay) {
                        delete.tryUnderWay = null;
                    }
                }
                underWay.tryEmitEmpty();
            });
        });
    }

    private boolean takenOver(PositionDelete delete) {
        synchronized (positionLock) {
            return delete.isTakenOver;
        }
    }

    // A subscription of the id that starts before a delete a cancel started has ended takes that delete over, and so
    // every earlier delete of the id it runs after. A delete taken over makes no further try, and completes once the
    // try under way and what this writes have ended. This writes back the checkpoint the try under way read, so storage
    // holds what it held before the cancel, as a start position resolved at the call expects.
    //
    // Where the delete is conditional, the write back goes at once, at the version after the one the try read, so the
    // try is refused or the checkpoint it deleted is back, and nothing waits for the try or for the write back. The
    // position writes of the subscription go at the version after the write back, so neither the try nor the write back
    // removes or replaces them. A read of storage can still answer the checkpoint the try is about to delete, or
    // nothing once the try applied and before the write back reaches the store. So a start from the model default
    // writes what it read, or the checkpoint written back where it read nothing, at that version before it starts, see
    // resolveStartAt. Otherwise the subscription writes a position, and starts from a checkpoint, only once the try and
    // the write back have ended, since nothing stops the try from removing what it stores.
    //
    // Either way storage is read at the call, see readStoredPosition(String, TakeOver), and the subscription writes
    // only once the position writes the delete waits for have ended. The deletes taken over count this call against
    // them until the subscription that made it is given up, see giveBackPositionDelete.
    private TakeOver takeOverPositionDelete(String subscriptionId) {
        List<Mono<Void>> beforeStorage = new ArrayList<>();
        List<PositionDelete> counted = new ArrayList<>();
        List<PositionDelete> takenOver = new ArrayList<>();
        List<Mono<Void>> writes = new ArrayList<>();
        long writeVersion = -1;
        @Nullable Checkpoint writtenBack = null;
        boolean restoredFound = false;
        @Nullable Checkpoint restored = null;
        final @Nullable Checkpoint writing;
        synchronized (positionLock) {
            @Nullable SequencedMap<Mono<Void>, Checkpoint> inFlight = positionWritesInFlight.get(subscriptionId);
            writing = inFlight == null || inFlight.isEmpty() ? null : inFlight.lastEntry().getValue();
            for (@Nullable PositionDelete delete = positionDeletes.get(subscriptionId); delete != null; delete = delete.earlier) {
                delete.takers++;
                counted.add(delete);
                beforeStorage.add(delete.writesInFlight);
                if (!delete.isTakenOver) {
                    delete.isTakenOver = true;
                    takenOver.add(delete);
                }
                Sinks.@Nullable Empty<Void> underWay = delete.tryUnderWay;
                if (delete.takeOver == null && underWay != null) {
                    @Nullable Checkpoint stored = delete.storedWhenTried;
                    if (stored == null) {
                        delete.takeOver = underWay.asMono();
                    } else {
                        final Mono<Void> writeBack;
                        // Two versions above the one the try read, the second for the position writes
                        if (delete.conditional && delete.versionWhenTried < Long.MAX_VALUE - 1) {
                            writeBack = writeBack(subscriptionId, stored, CheckpointWriteCondition.notOlderThan(delete.versionWhenTried + 1));
                            delete.positionWritesVersion = delete.versionWhenTried + 2;
                        } else {
                            // Not written back once every call that took the delete over has given it back by the
                            // time the try ends, see giveBackPositionDelete, since what the try removed is then what
                            // the cancel asked for
                            PositionDelete triedBy = delete;
                            CheckpointWriteCondition condition = storage.evaluatesWriteConditionsFor(subscriptionId)
                                    ? CheckpointWriteCondition.ifAbsent()
                                    : CheckpointWriteCondition.any();
                            writeBack = underWay.asMono()
                                    .then(Mono.defer(() -> isTakenOverStill(triedBy) ? writeBack(subscriptionId, stored, condition) : Mono.<Void>empty()))
                                    .cache();
                        }
                        delete.takeOver = writeBack;
                        writes.add(writeBack);
                    }
                }
                @Nullable Mono<Void> takeOver = delete.takeOver;
                if (takeOver != null) {
                    // The newest delete that a try was under way for, whose try read after every earlier one ended
                    if (!restoredFound) {
                        restoredFound = true;
                        restored = delete.storedWhenTried;
                    }
                    if (delete.positionWritesVersion < 0) {
                        beforeStorage.add(takeOver);
                    } else if (delete.positionWritesVersion > writeVersion) {
                        // The newest delete written back at once, whose try read after every earlier one ended
                        writeVersion = delete.positionWritesVersion;
                        writtenBack = delete.storedWhenTried;
                    }
                }
            }
        }
        // Outside the lock, since each starts what waits on it
        takenOver.forEach(delete -> delete.takenOverSignal.tryEmitEmpty());
        writes.forEach(write -> write.subscribe(unused -> {
        }, throwable -> {
        }));
        if (counted.isEmpty()) {
            return TakeOver.NONE;
        }
        // A position write of the cancelled subscription still in flight started before any try of the delete it
        // waits for, so it writes a newer checkpoint than one a try read
        @Nullable Checkpoint storedOnceWrittenBack = writing != null ? writing : writtenBack != null ? writtenBack : restored;
        return new TakeOver(Mono.when(beforeStorage).cache(), writeVersion < 0 ? null : CheckpointWriteCondition.notOlderThan(writeVersion),
                writtenBack, storedOnceWrittenBack, counted);
    }

    // A subscribe, a resume or a start that throws once its generation took over a delete of the id has no
    // subscription that needs the checkpoint the delete was to remove, so the delete goes ahead, as it would had that
    // call never come. A new delete of the id takes its place, which runs after the tries and the write back of the
    // deletes taken over, and after the position writes in flight. When the latest of those deletes still tries or
    // writes back by then, it ends only once the new one has, so the Mono of the cancel that started it completes once
    // the checkpoint is removed. Where the write back waits for the try under way, it is not made once every call has
    // given the delete back by the time the try ends. The new delete is not started while another call counted against
    // those deletes has not given them back, while a newer delete of the id runs, while a writer of the id is
    // registered or starting, or once the model is shut down. refused is the writer of the call that threw, which is
    // retired here when that call took a delete over, so nothing it still has under way writes a position, or null.
    private void giveBackPositionDelete(String subscriptionId, TakeOver takeOver, @Nullable PositionWriter refused) {
        if (takeOver.counted.isEmpty()) {
            return;
        }
        // Asked before the lock is taken, which is never held while calling the storage
        boolean conditional = storage.evaluatesDeleteConditions() && storage.evaluatesWriteConditionsFor(subscriptionId);
        @Nullable Mono<Void> goneAhead = null;
        synchronized (positionLock) {
            if (refused != null) {
                refused.retired = true;
            }
            if (takeOver.givenBack) {
                return;
            }
            takeOver.givenBack = true;
            takeOver.counted.forEach(counted -> counted.takers--);
            @Nullable PositionDelete latest = positionDeletes.get(subscriptionId);
            boolean stillNeeded = shutdown
                    || (latest != null && !takeOver.counted.contains(latest))
                    || takeOver.counted.stream().anyMatch(counted -> counted.takers > 0)
                    || positionWriters.containsKey(subscriptionId)
                    || positionWritersStarting.getOrDefault(subscriptionId, Set.of()).stream().anyMatch(writer -> writer != refused);
            if (!stillNeeded) {
                Mono<Void> writesInFlight = Mono.when(new ArrayList<>(positionWritesInFlight.getOrDefault(subscriptionId, new LinkedHashMap<>()).sequencedKeySet()));
                // Not the end of the latest delete, which waits for the new delete
                Mono<Void> writtenBack = Mono.when(takeOver.counted.stream().map(counted -> counted.writtenBack.asMono()).toList());
                PositionDelete delete = new PositionDelete(writesInFlight, conditional, latest, writtenBack);
                positionDeletes.put(subscriptionId, delete);
                // Only assembled here, it calls nothing until it is started below
                goneAhead = runPositionDelete(subscriptionId, delete, false);
                // The first counted is the latest delete when this call took it over, and no newer one has come since
                PositionDelete newest = takeOver.counted.get(0);
                if (!newest.isEnding) {
                    newest.goneAhead = goneAhead;
                }
            }
        }
        if (goneAhead != null) {
            startDelete(subscriptionId, goneAhead);
        }
    }

    private boolean isTakenOverStill(PositionDelete delete) {
        synchronized (positionLock) {
            return delete.takers > 0;
        }
    }

    // Cached, so it writes once whoever waits for it. A refusal means a newer checkpoint is stored already, written at
    // a higher version or after the try ended, which the try does not delete. Any other failure is logged. Where the
    // write back waits for the try, a failure fails a subscription of the id that starts from a checkpoint, and its
    // first position write, as a failed read of its start position would. Where it goes at once, the subscription
    // writes its start position itself before it starts, see takeOverPositionDelete, so nothing waits for it.
    private Mono<Void> writeBack(String subscriptionId, Checkpoint stored, CheckpointWriteCondition condition) {
        return Mono.defer(() -> storage.save(subscriptionId, stored, condition))
                .then()
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class, __ -> Mono.empty())
                .doOnError(throwable -> log.warn("Could not write back the stored checkpoint of subscription {} while the delete a cancel of it started was under way. Where the subscription of the id that starts now waits for this write, it fails with this error when it starts from a checkpoint and at its first checkpoint write.",
                        subscriptionId, throwable))
                .cache();
    }

    private static Throwable stoppedBeforeItsTurn(PositionDelete delete) {
        @Nullable Throwable failure = delete.lastFailure.get();
        if (failure == null && delete.earlierFailure != null) {
            failure = delete.earlierFailure.get();
        }
        return failure != null ? failure : new SubscriptionModelShutdownException();
    }

    // About doubles with each failed attempt, between half and one and a half of that, from FIRST_DELETE_RETRY_AFTER
    // and never past LONGEST_DELETE_RETRY_AFTER, so deletes failing together are not tried again together
    private static Duration retryAfter(long failedAttempts) {
        long doubled = FIRST_DELETE_RETRY_AFTER.toMillis() << Math.min(failedAttempts - 1, 16);
        long withJitter = (long) (Math.min(doubled, LONGEST_DELETE_RETRY_AFTER.toMillis()) * ThreadLocalRandom.current().nextDouble(0.5, 1.5));
        return Duration.ofMillis(Math.max(FIRST_DELETE_RETRY_AFTER.toMillis(), Math.min(withJitter, LONGEST_DELETE_RETRY_AFTER.toMillis())));
    }

    private void startDelete(String subscriptionId, Mono<Void> delete) {
        delete.subscribe(unused -> {
        }, throwable -> log.warn("Failed to delete the stored checkpoint of cancelled subscription {} before the model was shut down. Cancel it again once it runs, or a later subscribe with the subscription-model default start position resumes from that checkpoint.", subscriptionId, throwable));
    }

    /**
     * Shut this model down. A subscription this model drives that has not started by then ends
     * {@link Subscription#waitUntilStarted()} with {@link SubscriptionModelShutdownException}. One that already started
     * keeps that outcome. Once this is called no checkpoint write begins, for those subscriptions or for one already
     * handed to such a wrapped model. A step toward starting a subscription that passed its last check before this was
     * called can still begin after this returns, which is resolving its dynamic start position, reading the checkpoint,
     * subscribing to the feed or handing it to that wrapped model. It runs to its end, and this model discards its
     * result. This method waits for none of these steps, and a subscription the wrapped model makes in one of them is
     * cancelled there once that model has taken it. A failure of that cancel is logged as an error, since this method
     * has nothing to fail. Once this returns, this model does not run the action of a subscription it handed to such a
     * wrapped model, for an event that model delivers after that.
     * <p>
     * A call of an action that is already running when this is called is not waited for, so it can still be running
     * after this returns. No checkpoint is written for the event it handles, so the checkpoint stored before that event
     * stays, and the next subscribe of the id that resumes from it, with a new model or after a restart, delivers that
     * event again. Waiting for the call would let one action that never ends hold up the shutdown of every
     * subscription.
     */
    @Override
    public void shutdown() {
        if (delegate != null) {
            // Before the writers are retired, so a subscribe that checks from here on is not handed over, and one
            // that the wrapped model is taking right now cancels the subscription there itself once it has been taken.
            // Neither is waited for.
            shutdown = true;
            shutDown.tryEmitEmpty();
            synchronized (positionLock) {
                // An action the wrapped model is still running then saves no position, and neither does a subscribe
                // still reading where to start
                positionWriters.values().forEach(writer -> writer.retired = true);
                positionWriters.clear();
                positionWritersStarting.values().forEach(writers -> writers.forEach(writer -> writer.retired = true));
                delegatedSubscriptionIds.clear();
            }
            delegate.shutdown();
            return;
        }
        final List<InternalSubscription> ended;
        synchronized (this) {
            shutdown = true;
            running = false;
            ended = new ArrayList<>(runningSubscriptions.values());
            ended.addAll(pausedSubscriptions.values());
            synchronized (positionLock) {
                for (InternalSubscription internalSubscription : ended) {
                    internalSubscription.writer.retired = true;
                }
                positionWriters.clear();
            }
            runningSubscriptions.clear();
            pausedSubscriptions.clear();
        }
        // After the flag, so a subscription that starts from here on is disposed by the swap and its waitUntilStarted()
        // ends with SubscriptionModelShutdownException
        shutDown.tryEmitEmpty();
        ended.forEach(internalSubscription -> internalSubscription.disposable.dispose());
    }

    /**
     * Stop this model. When this model drives its subscriptions itself, each running subscription is paused as
     * {@link #pauseSubscription(String)} pauses it. When it hands them to a wrapped model, that model stops, and a
     * subscription that is starting again there stays paused until a start resumes it.
     */
    @Override
    public void stop() {
        if (delegate != null) {
            List<KeptLifecycle> passing = keepOrCountForEveryId(true);
            try {
                delegate.stop();
            } finally {
                passing.forEach(this::passed);
            }
            return;
        }
        final Map<String, InternalSubscription> paused = new HashMap<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            running = false;
            // Snapshot the ids first, since ConcurrentHashMap#forEach is only weakly consistent, so iterating it while
            // removing could skip subscriptions
            for (String subscriptionId : new ArrayList<>(runningSubscriptions.keySet())) {
                InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
                if (internalSubscription != null) {
                    pausedSubscriptions.put(subscriptionId, internalSubscription);
                    retire(subscriptionId, internalSubscription);
                    paused.put(subscriptionId, internalSubscription);
                }
            }
        }
        paused.forEach(ReactorDurableSubscriptionModel::pausedBeforeStarting);
    }

    /**
     * Start this model, and with {@code resumeSubscriptionsAutomatically} start every subscription registered while it
     * was stopped.
     * <p>
     * A subscription whose position could not be read when it was registered is refused, and the rest are started all
     * the same, so one broken subscription does not withhold the others. Each refusal ends
     * {@link Subscription#waitUntilStarted()} of the subscription that {@code subscribe(..)} returned, so this call
     * does not report it. A {@link StartAt#dynamic(java.util.function.Supplier) dynamic} start position that throws
     * keeps its subscription paused, the rest are started all the same, and this call then throws the first such error
     * with the others suppressed on it.
     * <p>
     * The subscriptions are started one after the other on the calling thread, so a dynamic start position that takes
     * long to answer delays the ones after it in this call. It does not hold up a call for another subscription id.
     * This call reads no position of where the feed is and waits for none. A subscription from the subscription-model
     * default starts from the position asked for when it was registered, and {@link StartAt#now()} opens the feed at
     * this call, so a read that never answers holds up neither this call nor the other subscriptions.
     * <p>
     * A subscribe from the subscription-model default on a thread that may block waits for that position before it
     * returns. On a thread where Reactor does not allow blocking it does not wait, and when the wrapped model answers
     * only after the subscribe returned, the subscription starts after the events written in between. A dynamic start
     * position runs at this call, so its subscribe waits for no position either, with the same result when it answers
     * the subscription-model default.
     *
     * @see SubscriptionModelLifeCycle#start(boolean)
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        if (delegate != null) {
            List<KeptLifecycle> passing = keepOrCountForEveryId(resumeSubscriptionsAutomatically ? false : null);
            try {
                delegate.start(resumeSubscriptionsAutomatically);
            } finally {
                passing.forEach(this::passed);
            }
            return;
        }
        final List<Reservation> reservations = new ArrayList<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // Snapshot the ids first, for the same reason as stop()
                for (String subscriptionId : new ArrayList<>(pausedSubscriptions.keySet())) {
                    InternalSubscription paused = pausedSubscriptions.remove(subscriptionId);
                    if (paused != null) {
                        reservations.add(reserveToResume(subscriptionId, paused));
                    }
                }
            }
        }
        Throwable first = null;
        for (Reservation reservation : reservations) {
            try {
                startReserved(reservation, true, null);
            } catch (RuntimeException | Error e) {
                releaseReservation(reservation);
                if (first == null) {
                    first = e;
                } else if (first != e) {
                    first.addSuppressed(e);
                }
            }
        }
        if (first instanceof RuntimeException runtimeException) {
            throw runtimeException;
        } else if (first instanceof Error error) {
            throw error;
        }
    }

    @Override
    public boolean isRunning() {
        if (delegate != null) {
            return delegate.isRunning();
        }
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        if (delegate != null) {
            @Nullable Boolean paused = keptPaused(subscriptionId);
            return paused != null ? !paused : delegate.isRunning(subscriptionId);
        }
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        if (delegate != null) {
            @Nullable Boolean paused = keptPaused(subscriptionId);
            return paused != null ? paused : delegate.isPaused(subscriptionId);
        }
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    // Whether the state kept for the id while startAgainInWrappedModel starts it again is paused, or null when no state
    // is kept for it, and the wrapped model answers
    private @Nullable Boolean keptPaused(String subscriptionId) {
        synchronized (positionLock) {
            @Nullable KeptLifecycle kept = keeping(subscriptionId);
            return kept == null ? null : kept.paused;
        }
    }

    // Called under positionLock
    private boolean isStartedAgain(String subscriptionId) {
        return keeping(subscriptionId) != null;
    }

    // Called under positionLock. The state kept for the subscription registered under the id while
    // startAgainInWrappedModel starts it again, or null when none is kept.
    private @Nullable KeptLifecycle keeping(String subscriptionId) {
        @Nullable PositionWriter writer = positionWriters.get(subscriptionId);
        @Nullable KeptLifecycle kept = writer == null ? null : writer.kept;
        return kept != null && kept.keeping ? kept : null;
    }

    // For a stop or a start of the wrapped model. Keeps the state a stop, or a start that resumes every subscription,
    // asks for when it is asked for null, for each id that startAgainInWrappedModel starts again, and counts the call
    // for each subscription that can still start again, see countPassing, which the caller answers through passed once
    // the wrapped model has returned.
    private List<KeptLifecycle> keepOrCountForEveryId(@Nullable Boolean pause) {
        final List<KeptLifecycle> passing = new ArrayList<>();
        final List<Sinks.Empty<Void>> unpaused = new ArrayList<>();
        synchronized (positionLock) {
            Set<PositionWriter> writers = new HashSet<>(positionWriters.values());
            positionWritersStarting.values().forEach(writers::addAll);
            for (PositionWriter writer : writers) {
                @Nullable KeptLifecycle kept = writer.kept;
                if (kept == null) {
                    continue;
                }
                if (!kept.keeping) {
                    kept.passing++;
                    passing.add(kept);
                } else if (pause != null) {
                    Sinks.@Nullable Empty<Void> resumed = kept.ask(pause);
                    if (resumed != null) {
                        unpaused.add(resumed);
                    }
                }
            }
        }
        unpaused.forEach(Sinks.Empty::tryEmitEmpty);
        return passing;
    }

    // Ends the count of a call that passed to the wrapped model, see passOrKeep, and lets a start again that waits for
    // the calls counted go on once the last has returned
    private void passed(KeptLifecycle kept) {
        Sinks.@Nullable Empty<Void> allPassed = null;
        synchronized (positionLock) {
            kept.passing--;
            if (kept.passing == 0) {
                allPassed = kept.passed;
                kept.passed = null;
            }
        }
        if (allPassed != null) {
            allPassed.tryEmitEmpty();
        }
    }

    private boolean isKnown(String subscriptionId) {
        return runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId);
    }

    // Separates "no such subscription here" from "wrong state for this call", which a caller holding several models
    // needs in order to tell "keep looking" from "this is the owner and the answer is no".
    private void requireKnown(String subscriptionId) {
        if (!isKnown(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    /**
     * Synchronized when this model drives its subscriptions itself, because a subscription moves between the two maps
     * in two steps, so an unsynchronized reader can read between them and miss an id that exists. It also keeps a
     * caller from seeing the ids of a model that {@link #shutdown()} has already flagged as shut down but not yet
     * cleared.
     */
    @Override
    public Set<String> subscriptionIds() {
        if (delegate != null) {
            // The wrapped model owns the subscriptions now, so ask it when it can be asked. The ids handed to it
            // through this model are the fallback for one that cannot.
            if (delegate instanceof IntrospectableSubscriptions introspectable) {
                Set<String> ids = introspectable.subscriptionIds();
                // The wrapped model does not have an id that startAgainInWrappedModel has cancelled there and not
                // subscribed again yet
                final Set<String> startedAgain;
                synchronized (positionLock) {
                    startedAgain = positionWriters.keySet().stream()
                            .filter(id -> !ids.contains(id) && isStartedAgain(id))
                            .collect(Collectors.toUnmodifiableSet());
                }
                return startedAgain.isEmpty() ? ids : Stream.concat(ids.stream(), startedAgain.stream()).collect(Collectors.toUnmodifiableSet());
            }
            synchronized (positionLock) {
                return Set.copyOf(delegatedSubscriptionIds);
            }
        }
        synchronized (this) {
            return Stream.concat(runningSubscriptions.keySet().stream(), pausedSubscriptions.keySet().stream())
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    // What one generation of a subscription writes its positions through, and what tells whether that generation is
    // still the live one for its id. The two flags and handingOver are read and changed under positionLock only.
    private static final class PositionWriter {
        // Its takeover of the deletes of the id that a cancel started, see takeOverPositionDelete. Set before the
        // generation reads or writes anything.
        private volatile TakeOver takeOver = TakeOver.NONE;
        // Set once the generation is retired, by a cancel, a pause, a stop or a shutdown, or by a writer registered
        // in its place, after which none of its writes and none of its steps toward starting begin, and the wrapped
        // model no longer runs the action of one handed to it
        private boolean retired;
        // Set by a cancel of the id while the subscribe was still reading where to start or handing the subscription
        // over, after which none of its writes start, its action does not run and the subscribe ends
        private boolean overtakenByCancel;
        // Set once a position write of the writer succeeded, after which a delete it took over no longer removes that
        // position when the subscription ends before it started, see giveBackUnlessWritten. Read and changed under
        // positionLock only.
        private boolean wrotePosition;
        // Set while the wrapped model takes the subscribe, and completed once that hand-over has ended, including the
        // cancel in the wrapped model that a cancel or a shutdown which came meanwhile makes it send
        private Sinks.@Nullable Empty<Void> handingOver;
        // Fails with OvertakenByCancel once overtakenByCancel is set, which ends a wait of the subscribe on the
        // caller's thread and the read it waits for
        private final Sinks.Empty<Void> overtaken = Sinks.empty();
        // Set by a subscribe handed to the wrapped model that took over a delete of the id, and ends once the start
        // position it was handed from is recorded, with the position to start it again from, if any, see
        // startAtTheCall. Null otherwise.
        private volatile @Nullable Mono<Optional<Checkpoint>> settled;
        // Set when settled is, in the step that marks the hand-over, and taken away once settled has ended without a
        // start again, or once the start again has put the state kept in place in the wrapped model. Read and changed
        // under positionLock only.
        private @Nullable KeptLifecycle kept;
    }

    // The state a pause, a resume, a stop or a start asks for a subscription handed to the wrapped model while
    // startAgainInWrappedModel starts it there again, see passOrKeep. The fields that are not final are read and changed
    // under positionLock only.
    private static final class KeptLifecycle {
        // Set from the start again on, after which a call no longer passes to the wrapped model and its state is kept
        // here, until the wrapped model has that state
        private boolean keeping;
        // The calls that passed to the wrapped model and have not returned yet, and what the start again waits on for
        // them, if it does
        private int passing;
        private Sinks.@Nullable Empty<Void> passed;
        // Whether the state kept is paused, null until the start again or a call has read the one the wrapped model has
        private @Nullable Boolean paused;
        // How many times a state was asked for, so applyKeptLifecycle tells a call made after the wrapped model refused
        // one from the call it refused
        private long asks;
        // Emitted once the state kept is no longer paused or the keeping ends, and replaced by a pause
        private Sinks.Empty<Void> unpaused = Sinks.empty();
        // Ends once the wrapped model has the state kept, or fails with what ended the start again
        private final Sinks.Empty<Void> applied = Sinks.empty();

        // Answers what to emit once positionLock is released, if anything
        private Sinks.@Nullable Empty<Void> ask(boolean pause) {
            boolean wasPaused = Boolean.TRUE.equals(paused);
            paused = pause;
            asks++;
            if (pause) {
                if (!wasPaused) {
                    unpaused = Sinks.empty();
                }
                return null;
            }
            return unpaused;
        }
    }

    // What a generation that took over the deletes of its id waits for before it writes a position or starts from a
    // checkpoint, the condition its position writes go on, null for none, the checkpoint written back at once, null for
    // none, and the checkpoint that storage holds once the position writes in flight and the deletes have ended, null
    // for none. See takeOverPositionDelete.
    private static final class TakeOver {
        private static final TakeOver NONE = new TakeOver(Mono.empty(), null, null, null, List.of());
        private final Mono<Void> beforeStorage;
        private final @Nullable CheckpointWriteCondition writeCondition;
        private final @Nullable Checkpoint writtenBack;
        private final @Nullable Checkpoint restored;
        // The deletes this takeover counts itself against, see giveBackPositionDelete
        private final List<PositionDelete> counted;
        // Read and changed under positionLock only
        private boolean givenBack;
        // Set once beforeStorage has ended, whether it failed or not
        private volatile boolean waitsForNothing;

        private TakeOver(Mono<Void> beforeStorage, @Nullable CheckpointWriteCondition writeCondition, @Nullable Checkpoint writtenBack,
                         @Nullable Checkpoint restored, List<PositionDelete> counted) {
            this.beforeStorage = beforeStorage;
            this.writeCondition = writeCondition;
            this.writtenBack = writtenBack;
            this.restored = restored;
            this.counted = counted;
            beforeStorage.subscribe(unused -> {
            }, __ -> waitsForNothing = true, () -> waitsForNothing = true);
        }
    }

    // What storage held when a subscribe asked it before reading where the feed is, and the takeover that read came
    // after
    private record StoredAtTheCall(Mono<Checkpoint> read, TakeOver takeOver) {
    }

    // A delete of a stored position that cancelSubscription started. The fields that are not final are read and
    // changed under positionLock only.
    private static final class PositionDelete {
        // The position writes of the id in flight when the cancel came, which the delete runs after
        private final Mono<Void> writesInFlight;
        // Whether each try deletes on the condition that the stored version is not above the one it read, decided at
        // the cancel. Only then can a takeover write the stored checkpoint back instead of waiting for the try.
        private final boolean conditional;
        // Emitted once the delete has ended and been taken out of positionDeletes
        private final Sinks.Empty<Void> ended = Sinks.empty();
        // Emitted once the tries have ended, and the write back of a takeover with them
        private final Sinks.Empty<Void> writtenBack = Sinks.empty();
        // What a delete that goes ahead in place of the ones a failed call took over waits for instead of the end of
        // the delete it runs after, see giveBackPositionDelete. Null for a delete a cancel started.
        private final @Nullable Mono<Void> runsAfter;
        // Emitted once a subscription of the id has taken the delete over
        private final Sinks.Empty<Void> takenOverSignal = Sinks.empty();
        private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();
        // The last error of the earlier delete of the id, which this delete ends with when a shutdown comes before its
        // own first try
        private final @Nullable AtomicReference<Throwable> earlierFailure;
        // The earlier delete of the id that this delete runs after, until this delete has ended
        private @Nullable PositionDelete earlier;
        private boolean isTakenOver;
        // Set while a try calls the storage, with the checkpoint it read, and the version it read when the delete is
        // conditional
        private Sinks.@Nullable Empty<Void> tryUnderWay;
        private @Nullable Checkpoint storedWhenTried;
        private long versionWhenTried;
        // What a subscription that took the delete over waits for, set by the first one that came while a try was
        // under way
        private @Nullable Mono<Void> takeOver;
        // The version the position writes of a subscription that took the delete over go at, set where the write back
        // goes at once, and -1 otherwise
        private long positionWritesVersion = -1;
        // How many calls that took the delete over have not given it back, see giveBackPositionDelete
        private int takers;
        // The delete that goes ahead in place of this delete once every call that took it over has given it back
        // before it ended, which the end of this delete waits for
        private @Nullable Mono<Void> goneAhead;
        // Set once the delete waits for nothing but goneAhead, after which goneAhead is no longer set
        private boolean isEnding;

        private PositionDelete(Mono<Void> writesInFlight, boolean conditional, @Nullable PositionDelete earlier, @Nullable Mono<Void> runsAfter) {
            this.writesInFlight = writesInFlight;
            this.conditional = conditional;
            this.earlier = earlier;
            this.earlierFailure = earlier == null ? null : earlier.lastFailure;
            this.runsAfter = runsAfter;
        }
    }

    // What capturePositionUnlessStored answers when storage holds a checkpoint. Reaches a caller only when that
    // checkpoint is gone by the time a later generation of the subscription reads storage, where nothing tells where
    // the feed was when it was subscribed. Starting from where the feed is then could skip events, and a cancel deletes
    // whatever is stored by then, so the only remedy named is a start position of the caller's own.
    private static final class CheckpointStored extends IllegalStateException {
        CheckpointStored(String subscriptionId) {
            super("A checkpoint was stored for subscription " + subscriptionId + " when it was subscribed, so where the feed was " +
                  "then was not read, and storage holds no checkpoint for it now. It is refused rather than started from " +
                  "where the feed is now, which could skip events it has not handled. Subscribe it again with a StartAt " +
                  "of your own, from before the first event it has not handled.");
        }
    }

    // What ends a subscribe that a cancel of its id overtook while it waited on the caller's thread
    private static final class OvertakenByCancel extends CancellationException {
        OvertakenByCancel(String subscriptionId) {
            super("Subscription " + subscriptionId + " was cancelled before it started");
        }
    }

    private enum Registration {
        REGISTERED, OVERTAKEN, SHUT_DOWN
    }

    // What a subscribe, a resume or a start of this model decided under the monitor, for startReserved to start once
    // it is released. running is false for a subscription registered while this model is stopped.
    private record Reservation(String subscriptionId, InternalSubscription subscription, @Nullable InternalSubscription replaced,
                               boolean running) {
    }

    // One generation of a subscription this model drives. A resume makes a new one from the paused one, so each run
    // of the subscription has a writer and a signal of its own.
    private static final class InternalSubscription {
        final Disposable.Swap disposable;
        final AtomicReference<StartAt> currentStartAt;
        final @Nullable SubscriptionFilter filter;
        final Function<CloudEvent, Mono<Void>> action;
        // Retired with the generation. Registered under the id only for a generation that runs.
        final PositionWriter writer;
        // Completed once the feed is subscribed to, failed by a refusal or an error, or by the cancel or pause that
        // retires the generation before that
        final Sinks.Empty<Void> signal;
        final Mono<Void> started;
        // The signal that waitUntilStarted() of the subscribe or the resume that began this run of generations reads.
        // A start of this model returns no handle, so a generation it begins tells a refusal or a cancel here too,
        // where the caller still waits. The signal of the generation itself when nothing came before it.
        final Sinks.Empty<Void> heldSignal;
        // Where the feed was when this subscription was registered, read before any other call could find it, so
        // that a first run records it when nothing is stored, whichever generation starts. Null for a registration
        // with a start position of its own.
        final @Nullable Mono<Checkpoint> positionNow;
        // The same read for a subscription registered on a stopped model, which resolveStartAt reconciles with what is
        // stored instead of only seeding storage with it. Carries the reason instead when that read could not answer,
        // which is what refuses the subscription when it is started. Null for one registered while running.
        final @Nullable Mono<Checkpoint> positionAtRegistration;
        // Failed by a cancel of the id, which cancels the reads of where the feed is that this subscription and every
        // generation of it started, see capturePositionNow. A pause does not, since the generation a resume begins
        // still starts from them.
        final Sinks.Empty<Void> readsAbandoned;
        // The takeovers of deletes of the id that the subscribe took at the call, and that the earlier generations of
        // this run took and never gave back since none of them started or wrote a position, see giveBackUnlessWritten
        final List<TakeOver> takenOverBefore;

        private InternalSubscription(Disposable.Swap disposable, AtomicReference<StartAt> currentStartAt, @Nullable SubscriptionFilter filter,
                                     Function<CloudEvent, Mono<Void>> action, Sinks.Empty<Void> signal, Mono<Void> started,
                                     Sinks.Empty<Void> heldSignal, @Nullable Mono<Checkpoint> positionNow,
                                     @Nullable Mono<Checkpoint> positionAtRegistration, Sinks.Empty<Void> readsAbandoned,
                                     List<TakeOver> takenOverBefore) {
            this.disposable = disposable;
            this.currentStartAt = currentStartAt;
            this.filter = filter;
            this.action = action;
            this.writer = new PositionWriter();
            this.signal = signal;
            this.started = started;
            this.heldSignal = heldSignal;
            this.positionNow = positionNow;
            this.positionAtRegistration = positionAtRegistration;
            this.readsAbandoned = readsAbandoned;
            this.takenOverBefore = takenOverBefore;
        }
    }

    private static final class ReactorDurableSubscription implements Subscription {
        private final String subscriptionId;
        private final Mono<Void> started;

        private ReactorDurableSubscription(String subscriptionId, Mono<Void> started) {
            this.subscriptionId = subscriptionId;
            this.started = started;
        }

        @Override
        public String id() {
            return subscriptionId;
        }

        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
