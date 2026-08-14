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
 * {@link CheckpointAwareSubscriptionModel#subscribe(SubscriptionFilter, StartAt)} primitive, which is what the reactor
 * catch-up models do, this model drives that primitive itself and manages the life cycle. A failing action is not
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
 * reports a start it could not make: thrown from {@link #subscribe(String, SubscriptionFilter, StartAt, Function)}
 * when the wrapped model manages named subscriptions, and signalled on {@link Subscription#waitUntilStarted()},
 * with an {@code ERROR} logged, when this model drives the cold primitive itself. A storage that answers {@code false}
 * from {@link CheckpointStorage#evaluatesWriteConditionsFor(String)} cannot be written to conditionally, so that
 * write stays unconditional and is logged at {@code WARN} instead. See ADR 89.
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
    // failed read is swallowed because the position is read again when the subscription starts.
    private Mono<Checkpoint> capturePositionNow(String subscriptionId) {
        return subscription.globalCheckpoint()
                .onErrorResume(throwable -> {
                    log.warn("Could not read the current position while registering subscription {}, it will be read again when the subscription starts", subscriptionId, throwable);
                    return Mono.empty();
                })
                .cache();
    }

    private Subscription startInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt,
                                                   Function<CloudEvent, Mono<Void>> action, @Nullable Mono<Checkpoint> positionAtRegistration) {
        if (!running) {
            // The model is stopped: don't subscribe at all, so waitUntilStarted() doesn't complete for a subscription
            // that won't deliver anything until start(true)/resumeSubscription actually starts it.
            //
            // Read where the feed is now and hold it, because starting this subscription later would otherwise begin
            // wherever the feed had reached by then, skipping everything written while it waited. Nothing is stored
            // until the subscription starts, so one that never starts leaves nothing behind.
            Mono<Checkpoint> positionNow = capturePositionNow(subscriptionId);
            // Kept as this subscription's disposable so shutdown and cancellation stop a read that is still in flight.
            Disposable reading = positionNow.subscribe();
            InternalSubscription internalSubscription = new InternalSubscription(reading, currentStartAt, filter, action, Mono.never(), positionNow);
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            return new ReactorDurableSubscription(subscriptionId, internalSubscription.started);
        }
        Sinks.Empty<Void> startedSink = Sinks.empty();
        runningSubscriptions.put(subscriptionId, new InternalSubscription(Disposables.disposed(), currentStartAt, filter, action, startedSink.asMono(), positionAtRegistration));
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
                            runningSubscriptions.remove(subscriptionId);
                        });
        InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, startedSink.asMono(), positionAtRegistration);
        if (runningSubscriptions.replace(subscriptionId, internalSubscription) == null) {
            // The placeholder was already removed by a synchronous error, so this subscription is already dead.
            disposable.dispose();
        }
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
            // A stored position always wins, so this only records one the first time a subscription runs. It prefers
            // the position read when the subscription was registered, which is earlier than now for one registered on
            // a stopped model, and reads the position again when there is none. Recording it can be refused, see
            // pinStartPosition below.
            Mono<Checkpoint> seed = positionAtRegistration == null
                    ? subscription.globalCheckpoint()
                    : positionAtRegistration.switchIfEmpty(subscription.globalCheckpoint());
            return storage.read(subscriptionId)
                    .switchIfEmpty(Mono.defer(() -> seed.flatMap(checkpoint -> pinStartPosition(subscriptionId, checkpoint))))
                    .map(StartAt::checkpoint)
                    .switchIfEmpty(Mono.fromSupplier(StartAt::now));
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
    // says so: it is conditional on nothing being stored when it lands. The read is what makes that condition the
    // right one, and it runs before the position is, since seed is only subscribed once the read comes back empty.
    // A refused write therefore means a checkpoint arrived between the two, written where this model cannot order
    // it against the position it read. See ADR 89.
    private Mono<Checkpoint> pinStartPosition(String subscriptionId, Checkpoint positionRead) {
        if (!storage.evaluatesWriteConditionsFor(subscriptionId)) {
            // Nothing here can make a storage that writes unconditionally do otherwise, so the write is the one
            // 0.32.0 made and two nodes recording a first position at the same moment keep the race. Logged rather
            // than refused, because refusing would take out a storage that has worked until now over a capability
            // it never claimed. It reads as once per subscription: a pin that succeeds is not attempted again.
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
                        __ -> refuseUnlessTheStoredPositionIsTheOneRead(subscriptionId, positionRead));
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
        // from then rather than from wherever the feed has reached by that point. Null for one registered while
        // running, which has no gap to cover.
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
