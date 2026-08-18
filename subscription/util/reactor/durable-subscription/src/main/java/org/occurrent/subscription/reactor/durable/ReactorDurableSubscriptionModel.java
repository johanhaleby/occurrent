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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
            // Deliberately outside this model's monitor. Reading the start position waits on the checkpoint store, and
            // holding the monitor across that would let one slow read block every life cycle call, shutdown included.
            // The wrapped model does its own locking, and this model keeps no state of its own on this path.
            return subscribeByDelegating(delegate, subscriptionId, filter, startAt, action);
        }

        synchronized (this) {
            if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (shutdown) {
                throw new IllegalStateException("Cannot start subscription because the subscription model is shutdown.");
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
        StartAt startAtToUse = durableStartAt(subscriptionId, startAt, delegate);
        // A null startAtToUse means a dynamic StartAt opted out of starting, so the wrapped model gets the original
        // position and the untouched action, and this model stays out of the way, exactly as the blocking twin does.
        Subscription delegated = startAtToUse == null
                ? delegate.subscribe(subscriptionId, filter, startAt, action)
                : delegate.subscribe(subscriptionId, filter, startAtToUse, persistingAction(subscriptionId, action));
        delegatedSubscriptionIds.add(subscriptionId);
        return delegated;
    }

    // The caller's action with the checkpoint save behind it, which is the whole of what this model adds to a delivery.
    private Function<CloudEvent, Mono<Void>> persistingAction(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return cloudEvent -> action.apply(cloudEvent)
                .then(Mono.defer(() -> config.persistCloudEventPositionPredicate.test(cloudEvent)
                        ? storage.save(subscriptionId, getCheckpointOrThrowIAE(cloudEvent)).then()
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
    private @Nullable StartAt durableStartAt(String subscriptionId, StartAt startAt, SubscriptionModel delegate) {
        if (startAt.isDefault()) {
            // Awaited here, so that what the wrapped model receives is a position and not something it has to resolve
            // later. It re-resolves the position whenever it restarts a change stream, and that runs on a scheduler
            // thread where awaiting a reactive read is refused outright, which would leave a subscription that hit one
            // transient storage error unable to ever start. Awaiting on this thread also reads the position before the
            // subscription is registered, so one registered while the wrapped model is stopped begins from here rather
            // than from wherever the feed has reached when it is finally started.
            return resolveStartAt(subscriptionId, startAt, null).block();
        } else if (startAt.isDynamic()) {
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            return nextStartAt == null ? null : durableStartAt(subscriptionId, nextStartAt, delegate);
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
        return storage.read(subscriptionId)
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
        Disposable disposable = resolveStartAt(subscriptionId, currentStartAt.get(), positionAtRegistration)
                .flatMapMany(resolvedStartAt -> {
                    currentStartAt.set(resolvedStartAt);
                    return source(subscriptionId, filter, resolvedStartAt, action, currentStartAt, true, startedSink);
                })
                // An empty resolveStartAt means a dynamic StartAt opted out of starting (its function returned null),
                // so read from the original StartAt without durable position handling, mirroring the blocking model's
                // "delegate to the wrapped model" branch.
                .switchIfEmpty(Flux.defer(() -> source(subscriptionId, filter, currentStartAt.get(), action, currentStartAt, false, startedSink)))
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
    private Flux<Void> source(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action, AtomicReference<StartAt> currentStartAt, boolean persist, Sinks.Empty<Void> startedSink) {
        Function<CloudEvent, Mono<Void>> delivery = persist ? persistingAction(subscriptionId, action) : action;
        return subscription.subscribe(filter, startAt)
                .doOnSubscribe(__ -> startedSink.tryEmitEmpty())
                .concatMap(cloudEvent -> delivery.apply(cloudEvent)
                        .doOnSuccess(unused -> currentStartAt.set(StartAt.checkpoint(getCheckpointOrThrowIAE(cloudEvent)))));
    }

    // Resolve the effective StartAt, mirroring the blocking DurableSubscriptionModel#generateStartAtPositionFrom:
    // the subscription-model default reads the last stored position (initializing it from the global position when
    // absent); a dynamic StartAt is resolved against this model's context and recursed, an empty result meaning "opt
    // out"; any concrete StartAt passes through unchanged.
    private Mono<StartAt> resolveStartAt(String subscriptionId, StartAt startAt, @Nullable Mono<Checkpoint> positionAtRegistration) {
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
                resolved = storage.read(subscriptionId)
                        .flatMap(stored -> positionAtRegistration
                                .onErrorResume(__ -> Mono.empty())
                                .flatMap(checkpoint -> storage.resolveFirstCheckpointRace(subscriptionId, checkpoint))
                                .defaultIfEmpty(stored))
                        .switchIfEmpty(Mono.defer(() -> positionAtRegistration.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint))))
                        .map(StartAt::checkpoint);
            } else {
                Mono<Checkpoint> seed = config.startWhenNoStartPositionCanBeRecorded
                        ? subscription.globalCheckpoint()
                        : subscription.globalCheckpoint().switchIfEmpty(Mono.error(() -> positionSourceAnsweredNothing(subscriptionId)));
                resolved = storage.read(subscriptionId)
                        .switchIfEmpty(Mono.defer(() -> seed.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint))))
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
            return resolveStartAt(subscriptionId, nextStartAt, positionAtRegistration);
        }
        return Mono.just(startAt);
    }

    // The read above found nothing, so this is the first position recorded for this subscription id, and the write
    // is conditional on that still being true when it reaches storage. A refused write therefore means a checkpoint
    // arrived between the read and the write, written where this model cannot order it against the position it read,
    // unless storage.resolveFirstCheckpointRace can order it after all. That position is read after the storage read
    // on every path but one. A subscription registered while the model was stopped read it at registration instead;
    // resolveStartAt reconciles that one separately, against whatever storage.read() finds, since this method is
    // never reached for it when something is already stored. See ADR 130 and #771.
    private Mono<Checkpoint> pinStartPosition(String subscriptionId, Checkpoint positionRead) {
        return recordFirstPosition(subscriptionId, positionRead)
                .switchIfEmpty(Mono.error(() -> storageAnsweredNothingAboutItsOwnWrite(subscriptionId, positionRead)));
    }

    private Mono<Checkpoint> recordFirstPosition(String subscriptionId, Checkpoint positionRead) {
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
            return storage.save(subscriptionId, positionRead);
        }
        return storage.save(subscriptionId, positionRead, CheckpointWriteCondition.ifAbsent())
                .onErrorResume(CheckpointWriteConditionNotFulfilledException.class,
                        // Asked first, because a storage able to compare the two settles this by position instead of
                        // by write order, with no exception either way. Falls through to the older, narrower rule
                        // only when the storage answers empty, meaning it cannot make that comparison.
                        __ -> storage.resolveFirstCheckpointRace(subscriptionId, positionRead)
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
        return startInternalSubscription(subscriptionId, internalSubscription.filter, internalSubscription.currentStartAt, internalSubscription.action,
                internalSubscription.positionAtRegistration);
    }

    /**
     * Cancel a subscription. It'll no longer receive events, and its persisted checkpoint is removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        if (delegate != null) {
            delegate.cancelSubscription(subscriptionId);
            delegatedSubscriptionIds.remove(subscriptionId);
            deleteStoredCheckpoint(subscriptionId);
            return;
        }
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
        }
        // A paused subscription can hold a position read that is still in flight, which shutdown already disposes.
        InternalSubscription pausedSubscription = pausedSubscriptions.remove(subscriptionId);
        if (pausedSubscription != null) {
            pausedSubscription.disposable.dispose();
        }
        deleteStoredCheckpoint(subscriptionId);
    }

    // Best-effort asynchronous cleanup of the stored position. cancelSubscription is void (fire-and-forget), so the
    // delete runs on its own without blocking the caller.
    private void deleteStoredCheckpoint(String subscriptionId) {
        storage.delete(subscriptionId).subscribe(unused -> {
        }, throwable -> log.warn("Failed to delete stored checkpoint for cancelled subscription {}", subscriptionId, throwable));
    }

    @Override
    public synchronized void shutdown() {
        if (delegate != null) {
            delegate.shutdown();
            delegatedSubscriptionIds.clear();
            return;
        }
        shutdown = true;
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
