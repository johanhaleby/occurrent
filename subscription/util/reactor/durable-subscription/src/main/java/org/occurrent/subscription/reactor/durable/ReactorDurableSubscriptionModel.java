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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * with an {@code ERROR} logged, when this model drives the cold primitive itself. A storage that answers {@code false}
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
 * not answer.
 * <p>
 * A {@link StartAt#dynamic(java.util.function.Supplier) dynamic} start position may answer the model default too,
 * and which of the two it answers decides whether the registration is refused. When the wrapped model manages named
 * subscriptions of its own that is resolved where {@link #subscribe(String, SubscriptionFilter, StartAt, Function)}
 * is called, so a refusal is thrown from that call like any other, with no handle involved. When this model drives
 * the cold primitive itself the function is resolved only once the subscription actually starts. A registration made
 * while running starts immediately, so the refusal comes out on the handle
 * {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} itself returns. One made while stopped leaves
 * that handle waiting instead, and the refusal comes out later, on the handle {@link #resumeSubscription(String)}
 * returns.
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
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();
    // Held while reading or changing the four maps below, and never while calling the storage. A cancel retires the
    // writer of the subscription it removes, collects the writes in flight for the id and installs its delete in one
    // step under it. A write checks its writer and looks for that delete in one step under it too, so each write is
    // one the delete waits for, one that waits for the delete, or one that never starts.
    private final Object positionLock = new Object();
    // The writer of the subscription registered under each id
    private final Map<String, PositionWriter> positionWriters = new HashMap<>();
    // The writers of subscribes still reading where to start, by subscription id. They are not registered yet, so a
    // cancel leaves them running and only tells them to read again.
    private final Map<String, Set<PositionWriter>> positionWritersStarting = new HashMap<>();
    // The position writes that have started and not ended yet, by subscription id
    private final Map<String, Set<Mono<Void>>> positionWritesInFlight = new HashMap<>();
    // The stored-position deletes that cancelSubscription started and that have not ended yet, by subscription id
    private final Map<String, Mono<Void>> positionDeletes = new HashMap<>();

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
            // Deliberately outside this model's monitor. Reading the start position waits on the checkpoint store, and
            // holding the monitor across that would let one slow read block every life cycle call, shutdown included.
            // subscribeByDelegating takes the monitor only once the position is read, to hand the subscription over.
            return subscribeByDelegating(delegate, subscriptionId, filter, startAt, action);
        }

        synchronized (this) {
            if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (shutdown) {
                throw new SubscriptionModelShutdownException();
            }
            return startInternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action, null);
        }
    }

    /**
     * Hands the subscription to the wrapped model and only adds the durable position handling on top, mirroring the
     * blocking {@code DurableSubscriptionModel}. The wrapped model keeps everything it already does for a named
     * subscription, which is what makes an unsupported filter refused here in {@code subscribe(..)} and a failing
     * action retried rather than fatal.
     */
    private Subscription subscribeByDelegating(SubscriptionModel delegate, String subscriptionId, @Nullable SubscriptionFilter filter,
                                               StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        PositionWriter writer = startingPositionWriter(subscriptionId);
        try {
            while (true) {
                StartAt startAtToUse = durableStartAt(subscriptionId, startAt, delegate, writer);
                // Under the monitor, so a cancel or a shutdown runs either before all of what follows or after it
                synchronized (this) {
                    if (shutdown) {
                        throw new SubscriptionModelShutdownException();
                    }
                    // A cancel of this id ran while the start position was read, and its delete can have removed what
                    // that read found or the position it recorded. Reading again waits for that delete.
                    if (overtakenByCancel(writer)) {
                        continue;
                    }
                    // A null startAtToUse means a dynamic StartAt opted out of starting, so the wrapped model gets the
                    // original position and the untouched action, and this model stays out of the way, exactly as the
                    // blocking twin does.
                    Subscription delegated = startAtToUse == null
                            ? delegate.subscribe(subscriptionId, filter, startAt, action)
                            : delegate.subscribe(subscriptionId, filter, startAtToUse, persistingAction(subscriptionId, writer, action));
                    positionWriterRegistered(subscriptionId, writer);
                    delegatedSubscriptionIds.add(subscriptionId);
                    return delegated;
                }
            }
        } finally {
            positionWriterNoLongerStarting(subscriptionId, writer);
        }
    }

    // The caller's action with the checkpoint save behind it, which is the whole of what this model adds to a delivery.
    private Function<CloudEvent, Mono<Void>> persistingAction(String subscriptionId, PositionWriter writer, Function<CloudEvent, Mono<Void>> action) {
        // One per subscription, so an EveryN configured for the whole model counts this subscription's events only
        Predicate<CloudEvent> persistCheckpoint = EveryN.forOneSubscription(config.persistCloudEventPositionPredicate);
        return cloudEvent -> action.apply(cloudEvent)
                .then(Mono.defer(() -> persistCheckpoint.test(cloudEvent)
                        ? writePosition(subscriptionId, writer, () -> storage.save(subscriptionId, getCheckpointOrThrowIAE(cloudEvent)), Mono.empty()).then()
                        : Mono.empty()));
    }

    /**
     * The reactor counterpart of the blocking {@code DurableSubscriptionModel#generateStartAtPositionFrom}. The
     * subscription-model default becomes a dynamic {@link StartAt} so that the wrapped model asks for the position when
     * it actually subscribes. That keeps this {@code subscribe(..)} synchronous, which is what lets the wrapped model
     * refuse an unsupported filter to the caller instead of failing later where nobody is listening.
     * <p>
     * Returns {@code null} when a dynamic {@code StartAt} opted out of starting.
     */
    private @Nullable StartAt durableStartAt(String subscriptionId, StartAt startAt, SubscriptionModel delegate, PositionWriter writer) {
        if (startAt.isDefault()) {
            // Awaited here, so that what the wrapped model receives is a position and not something it has to resolve
            // later. It re-resolves the position whenever it restarts a change stream, and that runs on a scheduler
            // thread where awaiting a reactive read is refused outright, which would leave a subscription that hit one
            // transient storage error unable to ever start. Awaiting on this thread also reads the position before the
            // subscription is registered, so one registered while the wrapped model is stopped begins from here rather
            // than from wherever the feed has reached when it is finally started.
            return resolveStartAt(subscriptionId, startAt, null, writer).block();
        } else if (startAt.isDynamic()) {
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            return nextStartAt == null ? null : durableStartAt(subscriptionId, nextStartAt, delegate, writer);
        }
        return startAt;
    }

    // Where the feed is, read once and remembered, so a subscription that is not started yet can begin from here. A
    // read that fails, and one that answers nothing, both refuse the subscription instead. Reading again when the
    // subscription starts would answer with wherever the feed has reached by then, and starting from that skips
    // everything written while the subscription waited, which is the whole of what reading at registration is for.
    // An empty answer is the unresolvable problem the wrapped model documents rather than a position, so it refuses
    // for the same reason. Cached, so the read runs once and every subscriber sees the same outcome.
    private Mono<Checkpoint> capturePositionNow(String subscriptionId) {
        Mono<Checkpoint> positionNow = config.startWhenNoStartPositionCanBeRecorded
                ? subscription.globalCheckpoint()
                : subscription.globalCheckpoint().switchIfEmpty(Mono.error(() -> positionSourceAnsweredNothing(subscriptionId)));
        return positionNow
                .doOnError(throwable -> log.warn("Could not read the current position while registering subscription {}. It is refused when it starts, unless by then a checkpoint is stored for it or its start position resolves to one of its own, either of which it starts from instead, so this failure alone does not refuse it", subscriptionId, throwable))
                .cache();
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

    private Subscription startInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt,
                                                   Function<CloudEvent, Mono<Void>> action, @Nullable Mono<Checkpoint> positionAtRegistration) {
        if (!running) {
            // The model is stopped: don't subscribe at all, so waitUntilStarted() doesn't complete for a subscription
            // that won't deliver anything until start(true)/resumeSubscription actually starts it.
            //
            // Read where the feed is now and hold it, because starting this subscription later would otherwise begin
            // wherever the feed had reached by then, skipping everything written while it waited. Nothing is stored
            // until the subscription starts, so one that never starts leaves nothing behind. A read that could not
            // answer refuses the subscription when it starts, which is where the model drops it, so getting it back
            // means registering it again rather than resuming.
            StartAt startAtNow = currentStartAt.get();
            // Only a registration that can still ask this model where to begin has anything to read for. A concrete
            // position is where the subscription begins whatever the feed does while it waits. A dynamic one is not
            // resolved until the subscription starts, so it is read for in case it answers the model default then.
            @Nullable Mono<Checkpoint> positionNow = startAtNow.isDefault() || startAtNow.isDynamic()
                    ? capturePositionNow(subscriptionId)
                    : null;
            // Kept as this subscription's disposable, though disposing it does not stop a read still in flight.
            // capturePositionNow's Mono ends in cache(), and disposing a subscriber of a cached Mono leaves the
            // upstream running to completion regardless. The error consumer is what keeps a failed read off
            // Operators.onErrorDropped, which throws on whichever thread the read finished on. Reporting it is
            // capturePositionNow's job, and it does it once.
            Disposable reading = positionNow == null ? Disposables.disposed() : positionNow.subscribe(unused -> {
            }, throwable -> {
            });
            // A read that answered still leaves waitUntilStarted() waiting, since the subscription has not started and
            // will not until it is asked to. Only the model default is certain to begin from what was read, so only
            // that one can end the wait here with the reason it could not be read.
            Mono<Void> started = startAtNow.isDefault() && positionNow != null
                    ? refusalOnceNothingIsStored(subscriptionId, positionNow)
                    : Mono.never();
            InternalSubscription internalSubscription = new InternalSubscription(reading, currentStartAt, filter, action, started, positionNow);
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            return new ReactorDurableSubscription(subscriptionId, internalSubscription.started);
        }
        // Assembled before anything is put into runningSubscriptions, since a dynamic StartAt is evaluated right here,
        // on the calling thread, and can throw. Thrown after the put, that left the id behind with nothing running
        // under it, and every later subscribe under the same id was refused as a duplicate. Nothing subscribes to it
        // until below.
        PositionWriter writer = registeredPositionWriter(subscriptionId);
        Mono<StartAt> resolvedStartAt = resolveStartAt(subscriptionId, currentStartAt.get(), positionAtRegistration, writer);
        Sinks.Empty<Void> startedSink = Sinks.empty();
        // One stable identity for this call's whole lifetime: put into runningSubscriptions before subscribing, and
        // never replaced afterwards, so the error handler below always removes the same object it (or nothing) put
        // there. A placeholder swapped for a real entry after subscribing would leave a window between installing
        // the entry and recording its identity where a concurrent error neither matches the map nor gets undone by
        // it; one entry, put once, has no such window. The disposable is filled in below once subscribing actually
        // returns one; Disposables.swap() disposes it immediately if this entry is disposed first.
        Disposable.Swap subscriptionDisposable = Disposables.swap();
        InternalSubscription internalSubscription = new InternalSubscription(subscriptionDisposable, currentStartAt, filter, action, startedSink.asMono(), positionAtRegistration);
        runningSubscriptions.put(subscriptionId, internalSubscription);
        Disposable disposable = resolvedStartAt
                .flatMapMany(startAt -> {
                    currentStartAt.set(startAt);
                    return source(subscriptionId, filter, startAt, action, currentStartAt, true, writer, startedSink);
                })
                // An empty resolveStartAt means a dynamic StartAt opted out of starting (its function returned null),
                // so read from the original StartAt without durable position handling, mirroring the blocking model's
                // "delegate to the wrapped model" branch.
                .switchIfEmpty(Flux.defer(() -> source(subscriptionId, filter, currentStartAt.get(), action, currentStartAt, false, writer, startedSink)))
                .subscribe(unused -> {
                        }, throwable -> {
                            log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable);
                            startedSink.tryEmitError(throwable);
                            // internalSubscription is the only entry this call ever puts under subscriptionId, so
                            // this removal is unambiguous regardless of when the error races the line below.
                            runningSubscriptions.remove(subscriptionId, internalSubscription);
                        });
        subscriptionDisposable.update(disposable);
        return new ReactorDurableSubscription(subscriptionId, internalSubscription.started);
    }

    // Reads events from the wrapped model's cold primitive, applies the action, then persists the position after each
    // event (per the config predicate) when persist is true. currentStartAt is advanced only after the action
    // completes so that pause/resume continues from the last delivered event rather than replaying or skipping.
    private Flux<Void> source(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action, AtomicReference<StartAt> currentStartAt, boolean persist,
                              PositionWriter writer, Sinks.Empty<Void> startedSink) {
        Function<CloudEvent, Mono<Void>> delivery = persist ? persistingAction(subscriptionId, writer, action) : action;
        return subscription.subscribe(filter, startAt)
                .doOnSubscribe(__ -> startedSink.tryEmitEmpty())
                .concatMap(cloudEvent -> delivery.apply(cloudEvent)
                        .doOnSuccess(unused -> currentStartAt.set(StartAt.checkpoint(getCheckpointOrThrowIAE(cloudEvent)))));
    }

    // Resolve the effective StartAt, mirroring the blocking DurableSubscriptionModel#generateStartAtPositionFrom:
    // the subscription-model default reads the last stored position (initializing it from the global position when
    // absent); a dynamic StartAt is resolved against this model's context and recursed, an empty result meaning "opt
    // out"; any concrete StartAt passes through unchanged.
    private Mono<StartAt> resolveStartAt(String subscriptionId, StartAt startAt, @Nullable Mono<Checkpoint> positionAtRegistration, PositionWriter writer) {
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
            // A registration with no position of its own reads storage directly, and pinStartPosition only ever runs
            // when that read found nothing, so there is no gap here between a read and a capture for anything to slip
            // into. A registration carrying positionAtRegistration is different: the capture already happened, at
            // registration, possibly long before this call, so whatever storage now holds may have been written
            // since, including by a checkpoint deleted and rewritten while this subscription waited to be started.
            // resolveFirstCheckpointRace reconciles the two by position instead of trusting storage.read() blindly,
            // when the storage can. Reading storage comes first and on its own, so a stored checkpoint still governs
            // exactly as it always has even when positionAtRegistration cannot be read or the storage cannot compare,
            // which the onErrorResume and defaultIfEmpty below both fall back to. See ADR 130 and #771.
            final Mono<StartAt> resolved;
            if (positionAtRegistration != null) {
                resolved = readStoredPosition(subscriptionId)
                        .flatMap(stored -> positionAtRegistration
                                .onErrorResume(__ -> Mono.empty())
                                .flatMap(checkpoint -> writePosition(subscriptionId, writer, () -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint), Mono.just(checkpoint)))
                                .defaultIfEmpty(stored))
                        .switchIfEmpty(Mono.defer(() -> positionAtRegistration.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint, writer))))
                        .map(StartAt::checkpoint);
            } else {
                Mono<Checkpoint> seed = config.startWhenNoStartPositionCanBeRecorded
                        ? subscription.globalCheckpoint()
                        : subscription.globalCheckpoint().switchIfEmpty(Mono.error(() -> positionSourceAnsweredNothing(subscriptionId)));
                resolved = readStoredPosition(subscriptionId)
                        .switchIfEmpty(Mono.defer(() -> seed.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint, writer))))
                        .map(StartAt::checkpoint);
            }
            // Empty here means nothing is stored and the position source answered nothing, which only the config
            // override lets through (capturePositionNow and the seed above refuse it otherwise). The original
            // default is what starts the subscription then, from wherever the feed is when it opens, with nothing
            // recorded, the loss window the override accepts.
            return config.startWhenNoStartPositionCanBeRecorded ? resolved.defaultIfEmpty(startAt) : resolved;
        } else if (startAt.isDynamic()) {
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            if (nextStartAt == null) {
                return Mono.empty();
            }
            return resolveStartAt(subscriptionId, nextStartAt, positionAtRegistration, writer);
        }
        return Mono.just(startAt);
    }

    // Read only once a position delete that a cancel of this id started has ended. Read before it, the position of
    // the cancelled subscription would be where the new subscription starts.
    private Mono<Checkpoint> readStoredPosition(String subscriptionId) {
        return afterPositionDelete(subscriptionId).then(Mono.defer(() -> storage.read(subscriptionId)));
    }

    // Completes once the position delete that a cancel of this id started has ended, failed ones included, so a
    // subscribe right after a cancel does not read the old position. A failed delete keeps the old position stored,
    // and it is then read as it would have been without the cancel.
    private Mono<Void> afterPositionDelete(String subscriptionId) {
        return Mono.defer(() -> {
            final Mono<Void> delete;
            synchronized (positionLock) {
                delete = positionDeletes.get(subscriptionId);
            }
            return delete == null ? Mono.empty() : delete.onErrorResume(__ -> Mono.empty());
        });
    }

    // For a subscribe that hands the subscription to the wrapped model, which reads where to start before it registers
    private PositionWriter startingPositionWriter(String subscriptionId) {
        PositionWriter writer = new PositionWriter();
        synchronized (positionLock) {
            positionWritersStarting.computeIfAbsent(subscriptionId, __ -> new HashSet<>()).add(writer);
        }
        return writer;
    }

    // Whether a cancel of the id ran since the writer started or since this was last asked
    private boolean overtakenByCancel(PositionWriter writer) {
        synchronized (positionLock) {
            boolean overtaken = writer.overtakenByCancel;
            writer.overtakenByCancel = false;
            return overtaken;
        }
    }

    private void positionWriterRegistered(String subscriptionId, PositionWriter writer) {
        synchronized (positionLock) {
            positionWriters.put(subscriptionId, writer);
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

    // For a subscription this model drives itself, which registers before it reads where to start
    private PositionWriter registeredPositionWriter(String subscriptionId) {
        PositionWriter writer = new PositionWriter();
        positionWriterRegistered(subscriptionId, writer);
        return writer;
    }

    // Answers whenRetired without writing once a cancel has retired the writer, and waits for a delete that a cancel
    // of the id started before checking again. Otherwise the write starts here and is tracked until it ends. A
    // storage can apply a write after its caller stopped waiting, so the write is subscribed here rather than by the
    // caller, and disposing the cancelled subscription does not end the tracking early. It is subscribed after the
    // lock is released, since a storage that blocks while subscribing would otherwise hold up every subscription.
    private <T> Mono<T> writePosition(String subscriptionId, PositionWriter writer, Supplier<Mono<T>> write, Mono<T> whenRetired) {
        return Mono.defer(() -> {
            Sinks.Empty<Void> ended = Sinks.empty();
            Mono<Void> writeEnded = ended.asMono();
            final Mono<Void> delete;
            synchronized (positionLock) {
                if (writer.retired) {
                    return whenRetired;
                }
                delete = positionDeletes.get(subscriptionId);
                if (delete == null) {
                    positionWritesInFlight.computeIfAbsent(subscriptionId, __ -> new HashSet<>()).add(writeEnded);
                }
            }
            if (delete != null) {
                return delete.onErrorResume(__ -> Mono.empty()).then(writePosition(subscriptionId, writer, write, whenRetired));
            }
            Mono<T> started = Mono.defer(write)
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
        });
    }

    private void positionWriteEnded(String subscriptionId, Mono<Void> writeEnded) {
        synchronized (positionLock) {
            Set<Mono<Void>> inFlight = positionWritesInFlight.get(subscriptionId);
            if (inFlight != null && inFlight.remove(writeEnded) && inFlight.isEmpty()) {
                positionWritesInFlight.remove(subscriptionId);
            }
        }
    }

    // The read above found nothing, so this is the first position recorded for this subscription id, and the write
    // is conditional on that still being true when it reaches storage. A refused write therefore means a checkpoint
    // arrived between the read and the write, written where this model cannot order it against the position it read,
    // unless storage.resolveFirstCheckpointRace can order it after all. That position is read after the storage read
    // on every path but one. A subscription registered while the model was stopped read it at registration instead;
    // resolveStartAt reconciles that one separately, against whatever storage.read() finds, since this method is
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
            return writePosition(subscriptionId, writer, () -> storage.save(subscriptionId, positionRead), Mono.just(positionRead));
        }
        return writePosition(subscriptionId, writer, () -> storage.save(subscriptionId, positionRead, CheckpointWriteCondition.ifAbsent()), Mono.just(positionRead))
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        // Asked first, because a storage able to compare the two settles this by position instead of
                        // by write order, with no exception either way. Falls through to the older, narrower rule
                        // only when the storage answers empty, meaning it cannot make that comparison.
                        __ -> writePosition(subscriptionId, writer, () -> storage.resolveFirstCheckpointRace(subscriptionId, positionRead), Mono.just(positionRead))
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
        return storage.read(subscriptionId)
                .onErrorMap(throwable -> StartPositionAlreadyPinnedException
                        .readingTheStoredPositionBackFailed(subscriptionId, positionRead, throwable))
                .flatMap(stored -> positionRead.asString().equals(stored.asString())
                        ? Mono.just(stored)
                        : Mono.error(() -> new StartPositionAlreadyPinnedException(subscriptionId, positionRead, stored)))
                .switchIfEmpty(Mono.error(() -> StartPositionAlreadyPinnedException
                        .readingTheStoredPositionBackFoundNothing(subscriptionId, positionRead)));
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (delegate != null) {
            delegate.pauseSubscription(subscriptionId);
            return;
        }
        if (shutdown) {
            throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isPaused(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        } else if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
    }

    /**
     * Start a subscription that was registered while this model was stopped, or resume one that was paused.
     * <p>
     * A subscription whose position could not be read when it was registered is refused here rather than started, and
     * the refusal is signalled on the returned {@link Subscription#waitUntilStarted()}. Such a subscription is dropped
     * from this model, so asking again answers with {@link UnknownSubscriptionException} and getting it back means
     * registering it again.
     *
     * @throws UnknownSubscriptionException        If this model has no such subscription.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running.
     */
    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (delegate != null) {
            return delegate.resumeSubscription(subscriptionId);
        }
        if (shutdown) {
            throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
        }
        requireKnown(subscriptionId);
        if (isRunning(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }

        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }

        running = true;
        // Reuse the same currentStartAt reference so resume continues from the position of the last event delivered
        // before the subscription was paused, rather than replaying (or skipping) from the original StartAt.
        try {
            return startInternalSubscription(subscriptionId, internalSubscription.filter, internalSubscription.currentStartAt, internalSubscription.action,
                    internalSubscription.positionAtRegistration);
        } catch (RuntimeException | Error e) {
            // A dynamic StartAt that throws puts nothing into runningSubscriptions, so it stays paused rather than
            // being dropped from both maps
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            throw e;
        }
    }

    /**
     * Cancel a subscription. It'll no longer receive events, and its persisted checkpoint is deleted.
     * <p>
     * The cancel takes effect when this method is called, whether or not anything subscribes to the returned
     * {@link Mono}. The {@code Mono} completes once the checkpoint is deleted and, when this model wraps a model that
     * manages named subscriptions, once that model's own cancel has completed. It fails when either fails. Once it
     * completes, the store holds no checkpoint that the cancelled subscription wrote, so a later subscribe of the same
     * id does not resume from where the cancelled one got to, in this process or after a restart. The delete runs
     * after every checkpoint write the cancelled subscription had already started, and a write it had not started by
     * then never runs. A subscribe in this process that does not wait for the {@code Mono} still reads and writes the
     * checkpoint only after the delete has ended. A subscribe of the same id that is still reading where to start when
     * this is called reads again once the delete has ended, and the checkpoint it writes after that is kept, since
     * this cancel stops only the subscription it removes.
     * <p>
     * The returned {@code Mono} is cached. A failed delete is also logged as a warning, whether or not anything
     * subscribes to the {@code Mono}.
     * <p>
     * When the delete fails, the subscription stays cancelled and the checkpoint stays stored, so call this again. Do
     * the same after a restart when the process ended before the {@code Mono} completed. It deletes the checkpoint for
     * an id this model has never subscribed too.
     *
     * @param subscriptionId The subscription id to cancel
     * @return A {@code Mono} that completes once the checkpoint is deleted
     */
    @Override
    public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
        final Mono<Void> wrappedModelCancelled;
        if (delegate != null) {
            wrappedModelCancelled = delegate.cancelSubscription(subscriptionId);
            delegatedSubscriptionIds.remove(subscriptionId);
        } else {
            InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
            if (internalSubscription != null) {
                internalSubscription.disposable.dispose();
            }
            // A paused subscription can hold a position read that is still in flight, which shutdown already disposes.
            InternalSubscription pausedSubscription = pausedSubscriptions.remove(subscriptionId);
            if (pausedSubscription != null) {
                pausedSubscription.disposable.dispose();
            }
            wrappedModelCancelled = Mono.empty();
        }
        return Mono.when(wrappedModelCancelled, deleteStoredCheckpoint(subscriptionId)).cache();
    }

    // Started here rather than by whoever subscribes to the result, so the delete runs even when nobody waits for it.
    // Cached, so a caller that waits does not delete a second time. Runs after the position writes already in flight
    // and after an earlier delete of the id, since either ending after this delete would leave a position stored.
    // Installed under the lock in the same step that retires the writer, so no read or write of the id falls between.
    private Mono<Void> deleteStoredCheckpoint(String subscriptionId) {
        final Mono<Void> delete;
        synchronized (positionLock) {
            PositionWriter cancelled = positionWriters.remove(subscriptionId);
            if (cancelled != null) {
                cancelled.retired = true;
            }
            positionWritersStarting.getOrDefault(subscriptionId, Set.of()).forEach(writer -> writer.overtakenByCancel = true);
            List<Mono<Void>> endedFirst = new ArrayList<>(positionWritesInFlight.getOrDefault(subscriptionId, Set.of()));
            Mono<Void> earlierDelete = positionDeletes.get(subscriptionId);
            if (earlierDelete != null) {
                endedFirst.add(earlierDelete.onErrorResume(__ -> Mono.empty()));
            }
            delete = Mono.when(endedFirst).then(Mono.defer(() -> storage.delete(subscriptionId))).cache();
            positionDeletes.put(subscriptionId, delete);
        }
        delete.doFinally(__ -> positionDeleteEnded(subscriptionId, delete))
                .subscribe(unused -> {
                }, throwable -> log.warn("Failed to delete stored checkpoint for cancelled subscription {}. Cancel it again, or a later subscribe with the subscription-model default start position resumes from that checkpoint.", subscriptionId, throwable));
        return delete;
    }

    private void positionDeleteEnded(String subscriptionId, Mono<Void> delete) {
        synchronized (positionLock) {
            positionDeletes.remove(subscriptionId, delete);
        }
    }

    @Override
    public synchronized void shutdown() {
        // Before the delegated ids are cleared, which the second check in subscribeByDelegating depends on.
        shutdown = true;
        if (delegate != null) {
            delegate.shutdown();
            delegatedSubscriptionIds.clear();
            return;
        }
        running = false;
        runningSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        runningSubscriptions.clear();
        pausedSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        pausedSubscriptions.clear();
    }

    @Override
    public synchronized void stop() {
        if (delegate != null) {
            delegate.stop();
            return;
        }
        if (!shutdown) {
            running = false;
            // Snapshot the ids first: pauseSubscription mutates runningSubscriptions, and ConcurrentHashMap#forEach is
            // only weakly consistent, so iterating it while removing could skip subscriptions.
            new ArrayList<>(runningSubscriptions.keySet()).forEach(this::pauseSubscription);
        }
    }

    /**
     * Start this model, and with {@code resumeSubscriptionsAutomatically} start every subscription registered while it
     * was stopped.
     * <p>
     * A subscription whose position could not be read when it was registered is refused, and the rest are started all
     * the same, so one broken subscription does not withhold the others. Each refusal is signalled on that
     * subscription's own {@link Subscription#waitUntilStarted()}, so this call does not report it.
     *
     * @see SubscriptionModelLifeCycle#start(boolean)
     */
    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        if (delegate != null) {
            delegate.start(resumeSubscriptionsAutomatically);
            return;
        }
        if (!shutdown) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                // Snapshot the ids first, for the same reason as stop(): resumeSubscription mutates pausedSubscriptions.
                new ArrayList<>(pausedSubscriptions.keySet()).forEach(this::resumeSubscription);
            }
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
            return delegate.isRunning(subscriptionId);
        }
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        if (delegate != null) {
            return delegate.isPaused(subscriptionId);
        }
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
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
     * Synchronized because a subscription moves between the two maps in two steps, so an unsynchronized reader can
     * land between them and miss an id that exists. It also keeps a caller from seeing the ids of a model that
     * {@link #shutdown()} has already flagged as shut down but not yet cleared.
     */
    @Override
    public synchronized Set<String> subscriptionIds() {
        if (delegate != null) {
            // The wrapped model owns the subscriptions now, so ask it when it can be asked. The ids handed to it
            // through this model are the fallback for one that cannot.
            return delegate instanceof IntrospectableSubscriptions introspectable
                    ? introspectable.subscriptionIds()
                    : Set.copyOf(delegatedSubscriptionIds);
        }
        return Stream.concat(runningSubscriptions.keySet().stream(), pausedSubscriptions.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    // What one subscription writes its positions through. Both fields are read and changed under positionLock only.
    private static final class PositionWriter {
        // Set by the cancel that removes the subscription, after which none of its writes start
        private boolean retired;
        // Set by a cancel of the id while the subscribe was still reading where to start
        private boolean overtakenByCancel;
    }

    private static final class InternalSubscription {
        final Disposable disposable;
        final AtomicReference<StartAt> currentStartAt;
        final @Nullable SubscriptionFilter filter;
        final Function<CloudEvent, Mono<Void>> action;
        final Mono<Void> started;
        // Where the feed was when this subscription was registered on a stopped model, so starting it later resumes
        // from then rather than from wherever the feed has reached by that point. Carries the reason instead when
        // that read could not answer, which is what refuses the subscription when it is started. Null for one
        // registered while running, which has no gap to cover.
        final @Nullable Mono<Checkpoint> positionAtRegistration;

        private InternalSubscription(Disposable disposable, AtomicReference<StartAt> currentStartAt, @Nullable SubscriptionFilter filter,
                                     Function<CloudEvent, Mono<Void>> action, Mono<Void> started, @Nullable Mono<Checkpoint> positionAtRegistration) {
            this.disposable = disposable;
            this.currentStartAt = currentStartAt;
            this.filter = filter;
            this.action = action;
            this.started = started;
            this.positionAtRegistration = positionAtRegistration;
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
