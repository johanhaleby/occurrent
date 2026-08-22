/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.projection.reactor.Projections;
import org.occurrent.dsl.projection.reactor.ReactiveDcbProjectionRunner;
import org.occurrent.dsl.projection.reactor.ReactiveProjectionRunner;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.AppliedAppendRecordingRegistry;
import org.occurrent.springboot.common.PolledCatchupSignals;
import org.occurrent.springboot.common.AsynchronousSubscribables;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.OccurrentProperties.ProjectionProperties.AppliedAppendProperties.ReplayPollProperties;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.occurrent.springboot.common.SubscriptionAnnotations.shouldWaitUntilStarted;
import static org.occurrent.springboot.common.SubscriptionAnnotations.subscriptionsStartOnTheirOwn;

/**
 * Scans a bean for {@link org.occurrent.annotation.Projection} factory methods in
 * {@code afterSingletonsInstantiated} and registers each one, including PUSH and domain-push routing and
 * read-model-store resolution. Domain-push feeds are collected and caught up once, after every projection has
 * registered, via {@link #catchUpCollectedFeeds()}.
 */
class ProjectionAnnotationRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ProjectionAnnotationRegistrar.class);

    // How long close() waits for the background catch-ups it started. Matches the blocking twin.
    private static final Duration SHUTDOWN_CATCHUP_TIMEOUT = Duration.ofSeconds(5);

    private final ApplicationContext applicationContext;
    private final Set<String> registeredIds;
    private final StartPositionSupport startPositionSupport;

    // Domain-push feeds collected during projection registration, caught up once after every projection is
    // registered. A list rather than a set now that a feed carries one projection: each entry is one projection's
    // catch-up and carries its own startupMode, so there is nothing left to de-duplicate.
    private final List<DomainFeedCatchUp> domainFeedsToCatchUp = new ArrayList<>();
    // Push catch-up models created here, kept so the context can stop their replays on the way down.
    private final List<CatchupThenPushSubscriptionModel> pushModels = new ArrayList<>();
    // Domain feeds whose catch-up was started in the background, kept so the context can stop those too, each with
    // the signal close() waits on afterwards.
    private final List<DomainEventFeed<?>> backgroundFeeds = new ArrayList<>();
    private final List<Mono<Void>> backgroundCatchUps = new ArrayList<>();

    // The applied-append recording poll's pacing (ADR 132 decision 7), and the scheduler that runs it. Both created
    // lazily on the first recordAppliedAppends = true projection, so an application that never uses the feature pays
    // for neither. One Scheduler shared by every recording projection in this context, disposed in close(), with a
    // thread per busy projection so one projection's stuck clear() cannot starve another's poll (see where it is
    // built). recordingLock guards both fields' initialization and recordingPollScheduler's disposal, since a
    // manually started push projection can register concurrently with another one, or with close() itself.
    private final Object recordingLock = new Object();
    private @Nullable AppliedAppendRecordingRegistry recordingRegistry;
    private @Nullable Scheduler recordingPollScheduler;
    // Set by close(), before close() reaches recordingLock below, so any check of this flag made while holding that
    // lock sees it in the same order close() itself observes recordingPollScheduler in.
    private volatile boolean closing = false;

    private record DomainFeedCatchUp(String id, DomainEventFeed<?> feed, boolean waitUntilStarted) {
    }

    ProjectionAnnotationRegistrar(ApplicationContext applicationContext, Set<String> registeredIds, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.registeredIds = registeredIds;
        this.startPositionSupport = startPositionSupport;
    }

    // Stop every catch-up this registrar started and wait for it to unwind, so no replay is still folding into a
    // store the closing context is about to dispose. Telling a replay to stop is not enough on its own: it notices at
    // its next event, and the store can be gone by then.
    void close() {
        closing = true;
        pushModels.forEach(CatchupThenPushSubscriptionModel::shutdown);
        pushModels.clear();
        backgroundFeeds.forEach(DomainEventFeed::stopCatchUp);
        backgroundFeeds.clear();
        long deadline = System.nanoTime() + SHUTDOWN_CATCHUP_TIMEOUT.toNanos();
        for (Mono<Void> catchUp : backgroundCatchUps) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            try {
                catchUp.block(Duration.ofNanos(remaining));
            } catch (RuntimeException e) {
                // Already logged and recorded where it happened, and a shutdown has nowhere useful to put a timeout.
            }
        }
        backgroundCatchUps.clear();
        // dispose() alone only stops the scheduler from accepting new work. A tick already blocked in
        // AppliedAppendStore.clear() can otherwise still be running once close() returns, against a store the
        // context is tearing down. disposeGracefully() is awaited outside recordingLock instead, capped at the
        // same deadline the catch-ups above use, so a stuck clear cannot hold this method open forever.
        Scheduler schedulerToClose;
        synchronized (recordingLock) {
            schedulerToClose = recordingPollScheduler;
            if (schedulerToClose != null) {
                schedulerToClose.dispose();
            }
        }
        if (schedulerToClose != null) {
            try {
                schedulerToClose.disposeGracefully().block(SHUTDOWN_CATCHUP_TIMEOUT);
            } catch (RuntimeException e) {
                // Nowhere useful to put a shutdown timeout, same as the catch-ups above.
            }
        }
    }

    @SuppressWarnings("unchecked")
    <E, S, ID> void processProjectionAnnotation(Object bean, Method method, org.occurrent.annotation.Projection annotation) {
        String id = annotation.id();
        if (!registeredIds.add(id)) {
            throw new DuplicateSubscriptionIdException(id, "Duplicate subscription/projection id '%s' (used by @Projection on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(id, bean.getClass().getName(), method.getName()));
        }
        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException("@Projection factory method %s#%s must take no parameters and return a Projection or DcbProjection.".formatted(bean.getClass().getName(), method.getName()));
        }
        boolean synchronous = annotation.mode() == org.occurrent.annotation.Mode.SYNCHRONOUS;
        SubscriptionAnnotations.validateModeStartKnobs("@Projection", id, synchronous,
                annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT,
                annotation.startAtGlobalPosition() >= 0,
                annotation.resumeBehavior() != ResumeBehavior.DEFAULT,
                annotation.startupMode() != StartupMode.DEFAULT);
        if (annotation.recordAppliedAppends()) {
            if (synchronous) {
                throw AppliedAppendRecordingRegistry.recordAppliedAppendsWithSynchronousMode(id);
            }
            // Failing fast here, rather than lazily the first time a recording projection would need the store,
            // reports the misconfiguration at startup instead of leaving the projection running unrecorded.
            resolveAppliedAppendStore(id);
        }

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Object descriptor = SubscriptionAnnotations.invokeDescriptorFactory("@Projection", bean, method);

        if (annotation.source() == org.occurrent.annotation.Source.PUSH) {
            // The feed bean's type decides the flavor: a PushSubscriptionModel feeds CloudEvents, a DomainEventFeed
            // feeds domain events directly.
            Object feedBean = SubscriptionAnnotations.resolveFeedBean(applicationContext, "@Projection", annotation.subscriptionModel(), annotation.subscriptionModelName(), id, PushSubscriptionModel.class, DomainEventFeed.class);
            if (feedBean instanceof PushSubscriptionModel pushModel) {
                registerPushProjection(id, converter, descriptor, synchronous, annotation, pushModel);
            } else if (feedBean instanceof DomainEventFeed<?> domainFeed) {
                registerDomainPushProjection(id, converter, descriptor, synchronous, annotation, domainFeed);
            } else {
                throw new IllegalArgumentException("@Projection '%s' with source=PUSH resolved a %s, which is neither a PushSubscriptionModel nor a DomainEventFeed.".formatted(id, feedBean.getClass().getName()));
            }
            return;
        }
        if (annotation.catchup() != org.occurrent.annotation.Catchup.FROM_EVENT_STORE) {
            // Ignoring it would be the expensive kind of silence: someone reaching for catchup=NONE means "don't read
            // the history", and an event-store projection left on its default start position reads all of it.
            throw new IllegalArgumentException("@Projection '%s' sets catchup, which only applies to source=PUSH, where it decides whether the projection replays the event store before going live. An event-store projection chooses its history with startAt instead (startAt = NOW to skip it).".formatted(id));
        }

        if (descriptor instanceof DcbProjection<?, ?, ?> raw) {
            DcbProjection<S, E, ID> dcbProjection = (DcbProjection<S, E, ID>) raw;
            if (synchronous) {
                throw new IllegalArgumentException("@Projection '%s' returns a DcbProjection with mode = SYNCHRONOUS, which the reactive stack does not support in this version. Use mode = ASYNC for a DCB read model, or an agnostic Projection for synchronous read-your-writes.".formatted(id));
            }
            FluxSubscriptionModel fluxSubscriptionModel = applicationContext.getBean(FluxSubscriptionModel.class);
            ReactiveDcbProjectionRunner<E> runner = ReactiveDcbProjectionRunner.create(fluxSubscriptionModel, converter);
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            startPositionSupport.applyStartupWorkarounds();
            // Resolved from fluxSubscriptionModel itself, not from an independently looked-up Subscribable bean: a
            // DCB-only composition is not required to expose one at all, and a context with more than one would let
            // the two lookups disagree about which model the phase actually describes. Checked against
            // SubscriptionModelCapability rather than Subscribable specifically, since ReplayAwareSubscriptions
            // itself does not require the wider Subscribable contract, only the capability lookup.
            CatchupResolution recordingResolution = annotation.recordAppliedAppends()
                    ? resolveCatchupModel(id, fluxSubscriptionModel instanceof SubscriptionModelCapability capability ? capability : null)
                    : null;
            warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), verifiedNeverReplays(annotation, recordingResolution));
            var subscription = projectDcb(runner, id, annotation, dcbProjection, resolveStore(annotation, id), startAt, recordingResolution);
            if (subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                subscription.waitUntilStarted().block();
            }
        } else if (descriptor instanceof Projection<?, ?, ?> raw) {
            Projection<S, E, ID> projection = (Projection<S, E, ID>) raw;
            boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
            if (synchronous) {
                // The synchronous subscription model has no start position, so nothing to wait for. It
                // delivers the just-written events on the write path (read-your-writes); the fold ignores unhandled types.
                ReactiveProjectionRunner<E> runner = ReactiveProjectionRunner.agnostic(applicationContext.getBean(SynchronousSubscriptionModel.class), converter);
                projectAgnosticOrStream(runner, id, annotation, projection, resolveStore(annotation, id), null, null);
            } else {
                // Resolved through AsynchronousSubscribables rather than a bare getBean(Subscribable.class): that
                // also matches the register-only SynchronousSubscriptionModel, which is ambiguous the moment an
                // application supplies its own asynchronous model without marking it @Primary (see
                // AsynchronousSubscribables, and #541).
                Subscribable subscribable = AsynchronousSubscribables.resolve(applicationContext, Subscribable.class, RegisteringSubscribable.class);
                ReactiveProjectionRunner<E> runner = stream ? ReactiveProjectionRunner.stream(subscribable, converter) : ReactiveProjectionRunner.agnostic(subscribable, converter);
                boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
                if (replaysHistory && stream && !startPositionSupport.streamHistoryReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' (capability = STREAM) asks to replay history, but this store does not support reactive stream history replay. Use capability = AGNOSTIC, startAt = NOW/DEFAULT, or a DcbProjection.".formatted(id));
                }
                if (replaysHistory && !stream && !startPositionSupport.positionReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' asks to replay history, but this store does not write a global position, so the reactive position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.".formatted(id));
                }
                StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
                startPositionSupport.applyStartupWorkarounds();
                CatchupResolution recordingResolution = annotation.recordAppliedAppends() ? resolveCatchupModel(id, subscribable) : null;
                warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), verifiedNeverReplays(annotation, recordingResolution));
                var subscription = projectAgnosticOrStream(runner, id, annotation, projection, resolveStore(annotation, id), startAt, recordingResolution);
                if (subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                    subscription.waitUntilStarted().block();
                }
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor.getClass().getName()));
        }
    }

    // Catch up each domain-push feed once, after every projection is registered.
    void catchUpCollectedFeeds() {
        for (DomainFeedCatchUp pending : domainFeedsToCatchUp) {
            if (pending.waitUntilStarted()) {
                recordingProgress(pending.id(), pending.feed().catchUpAll()).block();
            } else {
                // startupMode = BACKGROUND. No thread of our own here, unlike the blocking twin: subscribing without
                // blocking is all it takes, since the handover runs the replay on boundedElastic.
                backgroundFeeds.add(pending.feed());
                // cache() so close() can wait on the same run rather than starting a second one.
                Mono<Void> catchUp = recordingProgress(pending.id(), pending.feed().catchUpAll()).cache();
                backgroundCatchUps.add(catchUp);
                catchUp.subscribe(ignored -> {
                }, error -> recordBackgroundFailure(pending.id(), error));
            }
        }
        domainFeedsToCatchUp.clear();
    }

    // Put a background catch-up failure where the application can read it. Nobody waited for the replay, which is the
    // whole point of BACKGROUND, so the failure has to be recorded rather than thrown.
    private void recordBackgroundFailure(String id, Throwable error) {
        log.error("The background catch-up of projection {} failed. It has folded no history and will receive no live "
                + "events until the application is restarted.", id, error);
        withPushCatchupStatus(status -> status.recordFailure(id, error));
    }

    // getIfAvailable rather than getBean: the starter contributes this bean, but a context that wires the post
    // processor directly has no reason to, and losing the record is better than losing the log too.
    private void withPushCatchupStatus(Consumer<PushCatchupStatusImpl> action) {
        PushCatchupStatusImpl status = applicationContext.getBeanProvider(PushCatchupStatusImpl.class).getIfAvailable();
        if (status != null) {
            action.accept(status);
        }
    }

    // Wrap a domain-feed replay so an application can see where it is. A DomainEventFeed is not a subscription model,
    // so unlike the push-model path there is nothing to ask afterwards and the state has to be recorded as it happens.
    // A replay that errors records neither, leaving the subscriber's error handler to record the failure instead.
    private Mono<Void> recordingProgress(String id, Mono<Void> replay) {
        return Mono.<Void>fromRunnable(() -> withPushCatchupStatus(status -> status.recordCatchingUp(id)))
                .then(replay)
                .doOnSuccess(ignored -> withPushCatchupStatus(status -> status.recordLive(id)));
    }

    // Resolves the AppliedAppendStore a recordAppliedAppends = true projection records into, failing fast at
    // registration when none exists rather than leaving the projection running unrecorded.
    private AppliedAppendStore resolveAppliedAppendStore(String id) {
        AppliedAppendStore store = applicationContext.getBeanProvider(AppliedAppendStore.class).getIfAvailable();
        if (store == null) {
            throw AppliedAppendRecordingRegistry.noAppliedAppendStoreConfigured(id);
        }
        return store;
    }

    // Who tells a recording projection about its catch-ups, for an event-store-fed projection (DCB or plain),
    // asynchronous. Tried in order: the ComposedCatchupModel holder OccurrentReactiveMongoAutoConfiguration fills
    // for the default Mongo composition, then a direct capability check on capability itself for a composition that
    // exposes ReplayAwareSubscriptions directly (this stack's capability lookup is a plain instanceof, not a
    // wrapper-chain walk, so nothing else can see further in). Coming up empty on both cannot tell "this
    // composition genuinely never catches up" from "it does but nothing here can see it", so it warns rather than
    // silently choosing the optimistic reading (ADR 132 decision 2). Typed as the minimal
    // SubscriptionModelCapability rather than Subscribable, since a DCB-only FluxSubscriptionModel bean can
    // implement ReplayAwareSubscriptions directly without being a full Subscribable. capability is null for a
    // composition whose model exposes neither, treated the same as one that does but answers empty.
    private CatchupResolution resolveCatchupModel(String id, @Nullable SubscriptionModelCapability capability) {
        ComposedCatchupModel holder = applicationContext.getBeanProvider(ComposedCatchupModel.class).getIfAvailable();
        if (holder != null && holder.isSupplied()) {
            // The holder answering with no model is a known fact (the default composition legitimately has no
            // catch-up layer, decision 9), not an unresolved question, so it is a different answer from the warning
            // below even though both leave the projection with nothing to listen to.
            return new CatchupResolution(holder.catchupModel().orElse(null), holder.catchupModel().isEmpty(), false);
        }
        Optional<ReplayAwareSubscriptions> direct = capability == null ? Optional.empty() : capability.capability(ReplayAwareSubscriptions.class);
        if (direct.isPresent()) {
            return new CatchupResolution(direct.get(), false, false);
        }
        log.warn("@Projection '{}' sets recordAppliedAppends = true, but its subscription model ({}) does not expose " +
                        "whether it is catching up. The default reactive Mongo composition does not have this problem; a " +
                        "custom or third-party subscription model can. Recording proceeds as though this projection " +
                        "never catches up: a genuine catch-up would record straight through it, with no automatic clear afterwards.",
                id, capability == null ? "none" : capability.getClass().getName());
        // catchupAwarenessUnknown is true here, since the warning just above already covers this case accurately (it
        // might genuinely catch up, this registrar just cannot tell). warnIfRecordingNeverResets must not also claim
        // it never does, and must not fire a second warning for the same unresolved composition.
        return new CatchupResolution(null, false, true);
    }

    // Who a recording projection listens to for its catch-up boundaries, null when nothing can tell it. The two
    // reasons for a null model are different facts and are kept apart: structurallyNeverCatchesUp is a composition
    // known to have no catch-up layer, catchupAwarenessUnknown is one this registrar cannot see into.
    private record CatchupResolution(@Nullable ReplayAwareSubscriptions model, boolean structurallyNeverCatchesUp,
                                     boolean catchupAwarenessUnknown, boolean pollsForClear) {
        CatchupResolution(@Nullable ReplayAwareSubscriptions model, boolean structurallyNeverCatchesUp, boolean catchupAwarenessUnknown) {
            this(model, structurallyNeverCatchesUp, catchupAwarenessUnknown, false);
        }
    }

    // ADR 132 decision 9's third case: a composition that can replay and can report its phase, but is wired so it
    // is never asked to (the resolved start position never replays, or the composition has no catch-up layer at
    // all). Recording still proceeds, since decision 9 allows it, but nothing ever clears it automatically.
    // verifiedNeverReplays must already be a fact this registrar can stand behind (see verifiedNeverReplays(...)
    // below), never an inference from an unobservable composition's assumed behavior, so a false positive here
    // never happens.
    private void warnIfRecordingNeverResets(String id, boolean recordAppliedAppends, boolean verifiedNeverReplays) {
        if (recordAppliedAppends && verifiedNeverReplays) {
            log.warn(AppliedAppendRecordingRegistry.recordAppliedAppendsNeverResetsAutomatically(id));
        }
    }

    // The three sound cases the recordAppliedAppends-never-resets warning may fire on for an event-store
    // (DCB/agnostic/stream) projection, per the epic's doctrine that a fact about a composition comes from the
    // owner that composed it, never from a probe or an annotation-only predicate:
    //   1. An explicit NOW, StartAt.now() is a documented, composition-independent contract every Subscribable
    //      must honor, true regardless of what the composition can do.
    //   2. The composition structurally has no catch-up layer at all (ADR 132 decision 9's third case), a known
    //      fact resolveCatchupModel's ComposedCatchupModel branch already verified, not an unobserved absence.
    //   3. DEFAULT, where the auto-configuration that composed this model registered, through ComposedCatchupModel,
    //      that its own DEFAULT bypasses catch-up. True for Occurrent's shipped Mongo composition (issue 865), never
    //      assumed for an application-supplied one, whose own DEFAULT semantics are its own to declare.
    // DEFAULT on a composition with no registered fact stays silent. This registrar cannot verify it either way.
    private boolean verifiedNeverReplays(org.occurrent.annotation.Projection annotation, @Nullable CatchupResolution recordingResolution) {
        if (annotation.startAt() == org.occurrent.annotation.StartPosition.NOW) {
            return true;
        }
        boolean structurallyNeverReplays = recordingResolution != null && recordingResolution.structurallyNeverCatchesUp();
        if (structurallyNeverReplays) {
            return true;
        }
        if (annotation.startAt() == org.occurrent.annotation.StartPosition.DEFAULT && annotation.startAtGlobalPosition() < 0) {
            // startAtGlobalPosition >= 0 overrides DEFAULT and replays from that position regardless
            // (generateAgnosticStartAt/generateDcbStartAt check it first), so it must be excluded here too, or a
            // projection that genuinely replays from an explicit position would get the never-replays warning.
            ComposedCatchupModel holder = applicationContext.getBeanProvider(ComposedCatchupModel.class).getIfAvailable();
            return holder != null && holder.isDefaultKnownLiveOnly();
        }
        return false;
    }

    // Wraps update so it records every applied append. Caller only calls this once it has already decided recording
    // is on (resolution is only ever built when annotation.recordAppliedAppends() is true), so there is no flag to
    // re-check here.
    private <E> BiFunction<EventMetadata, E, Mono<Void>> applyRecording(String id, BiFunction<EventMetadata, E, Mono<Void>> update, CatchupResolution resolution) {
        BiFunction<EventMetadata, E, Mono<Void>> recording = Projections.recordingAppliedAppends(update, id, resolveAppliedAppendStore(id));
        ReplayAwareSubscriptions model = resolution.model();
        if (model != null && recording instanceof AppliedAppendRecorder recorder) {
            // Registered before the subscription that produces the catch-ups is started by the caller, so a
            // catch-up that begins the moment it starts is heard rather than recorded as though it were live.
            if (model.listenForCatchup(id, recorder)) {
                registerForPoll(id, recorder);
            } else {
                registerForPoll(id, new PolledCatchupSignals(recorder, () -> model.isCatchingUp(id)));
            }
        } else if (model == null && resolution.pollsForClear() && recording instanceof AppliedAppendRecorder recorder) {
            // A pull feed drives its own replay and needs no watching, but the clear that replay owes can still fail,
            // and a feed that then goes quiet has nothing left to retry it. The poll does.
            registerForPoll(id, recorder);
        }
        return recording;
    }

    // Registers projectionId with the applied-append recording poll (ADR 132 decision 7), lazily creating the shared
    // registry and its Scheduler on the first call. The poll is self-rescheduling per projection: each tick asks the
    // registry how long until the next one is due and reschedules only itself, rather than waking every registered
    // projection on one fixed global tick. boundedElastic-family only, per this epic's ruling, since AppliedAppendStore
    // is a blocking-shaped interface and the tick calls it directly.
    private void registerForPoll(String id, AppliedAppendRecorder recorder) {
        appliedAppendRecordingRegistry().register(id, recorder);
        scheduleNextTick(id);
    }

    private void registerForPoll(String id, BooleanSupplier tick) {
        appliedAppendRecordingRegistry().register(id, tick);
        scheduleNextTick(id);
    }

    // Self-rescheduling: each tick asks the registry how long until the projection is next due (an interval the
    // tick itself may have just changed) and schedules exactly one more tick at that delay. A tick that throws is
    // caught and logged rather than left to cancel the reschedule below it, the same protection the blocking
    // registrar's poll already has, so a transient failure (a flaky phase or store) costs a skipped tick rather
    // than replay detection for the rest of the context's life.
    //
    // The closing check and the scheduler's lazy creation share recordingLock with close()'s own dispose check, so a
    // registration racing close() either creates the scheduler before close() sees it (and close() disposes what it
    // just created) or sees closing already set (and creates nothing close() would have to know about). Checking
    // closing outside that lock, or creating the scheduler outside it, leaves a window where a scheduler created
    // after close() ran is never disposed.
    private void scheduleNextTick(String id) {
        AppliedAppendRecordingRegistry registry = appliedAppendRecordingRegistry();
        synchronized (recordingLock) {
            if (closing) {
                return;
            }
            if (recordingPollScheduler == null) {
                // Unbounded thread and queue caps deliberately: a single worker would let one projection's stuck
                // clear() (AppliedAppendStore's default retry is unbounded, decision 7) occupy the only thread and
                // starve every other registered projection's poll. One long-delayed, self-rescheduling task lives
                // per registered recording projection at a time, so both caps' natural size is the registration
                // count, a legitimate size rather than something a fixed cap should reject once exceeded.
                recordingPollScheduler = Schedulers.newBoundedElastic(Integer.MAX_VALUE, Integer.MAX_VALUE, "occurrent-applied-append-poll", 60, true);
            }
            recordingPollScheduler.schedule(() -> {
                try {
                    registry.tick(id);
                } catch (RuntimeException e) {
                    log.error("The applied-append recording poll for projection '{}' failed. It will be retried at the next tick.", id, e);
                }
                scheduleNextTick(id);
            }, registry.dueInNanos(id), java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private AppliedAppendRecordingRegistry appliedAppendRecordingRegistry() {
        synchronized (recordingLock) {
            if (recordingRegistry == null) {
                ReplayPollProperties pollProperties = applicationContext.getBean(OccurrentProperties.class).getProjection().getAppliedAppend().getReplayPoll();
                recordingRegistry = new AppliedAppendRecordingRegistry(pollProperties.getInitial(), pollProperties.getMax(), pollProperties.getMultiplier());
            }
            return recordingRegistry;
        }
    }

    // Register a source=PUSH projection whose feed bean is a PushSubscriptionModel (CloudEvents). Wrapped in a
    // replay-then-push catch-up so a new or rebuilt projection is backfilled from the event store, unless
    // catchup = NONE, where the bare model is used directly and no event store is touched at all.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        Subscribable subscribable;
        // Who a recording push projection listens to, per ADR 132 decision 8. A catch-up-then-push composition
        // listens to the model this registrar just built, since it already has the reference. One with
        // catchup = NONE has no catch-ups to hear about (decision 9), so it listens to nothing.
        @Nullable CatchupResolution recordingResolution = null;
        if (catchesUp) {
            PositionOrderedReader reader = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", PositionOrderedReader.class, id);
            CheckpointStorage catchupMarker = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", CheckpointStorage.class, id);
            CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker, catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)));
            // Retained so close() can stop it. Its replay runs on boundedElastic, so a context that closes without
            // stopping it leaves that replay folding into a store that is closing with it.
            pushModels.add(model);
            // Asked rather than recorded, so a model that is stopped and started again, replaying a second time,
            // reports catching up again instead of staying at whatever it reached the first time.
            withPushCatchupStatus(status -> status.register(id, () -> model.isCatchingUp(id), () -> model.isRunning(id)));
            subscribable = model;
            if (annotation.recordAppliedAppends()) {
                recordingResolution = new CatchupResolution(model, false, false);
            }
        } else {
            // catchup = NONE never replays, so it is live as soon as it is running. Asked rather than recorded because
            // occurrent.subscription.mode = manual defers the subscription, and a recorded Live would tell a readiness
            // probe that a projection nobody has started yet is ready to serve.
            withPushCatchupStatus(status -> status.register(id, () -> false, () -> pushModel.isRunning(id)));
            subscribable = pushModel;
            if (annotation.recordAppliedAppends()) {
                recordingResolution = new CatchupResolution(null, true, false);
            }
        }
        warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), recordingResolution == null || recordingResolution.model() == null);
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        ReactiveProjectionRunner<E> runner = stream ? ReactiveProjectionRunner.stream(subscribable, converter) : ReactiveProjectionRunner.agnostic(subscribable, converter);
        Object store = resolveStore(annotation, id);
        @Nullable CatchupResolution resolution = recordingResolution;
        if (subscriptionsStartOnTheirOwn(applicationContext)) {
            // Registration happens here, on the refresh thread, which is what captures an event committing mid-replay.
            // Only the replay is elsewhere.
            var subscription = projectAgnosticOrStream(runner, id, annotation, projection, store, null, resolution);
            if (SubscriptionAnnotations.pushCatchUpShouldWaitUntilStarted(annotation.startupMode())) {
                subscription.waitUntilStarted().block();
            } else {
                subscription.waitUntilStarted().subscribe(ignored -> {
                }, error -> recordBackgroundFailure(id, error));
            }
        } else {
            // This feed bypasses the subscription model bean entirely, so manual mode's own withholding never reaches
            // it. Defer the same call instead, to run once the application starts this projection itself. startupMode
            // is not read here: start(id) hands the caller the Mono, so waiting or not is already their choice.
            applicationContext.getBean(ManualStartPushSources.class).register(id,
                    () -> projectAgnosticOrStream(runner, id, annotation, projection, store, null, resolution).waitUntilStarted());
        }
    }

    // Register a source=PUSH projection whose feed bean is a DomainEventFeed. The reactor feed folds via a
    // ViewStateRepository (through reactiveUpdate on boundedElastic), so the store must resolve to a ViewStateRepository.
    // Catches up from the event store unless catchup = NONE, where it goes live immediately instead and touches no
    // event store at all.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerDomainPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, DomainEventFeed<?> feedBean) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        Object store = resolveStore(annotation, id);
        DomainEventFeed<E> feed = (DomainEventFeed<E>) feedBean;
        // Computed early: catchesUp is what actually decides whether this projection replays (it drives the view-DSL
        // replay lifecycle below, not a subscription model), and warnIfRecordingNeverResets needs it before the
        // fold is built.
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), !catchesUp);
        Runnable registerOnFeed;
        if (!annotation.recordAppliedAppends() && store instanceof ViewStateRepository) {
            registerOnFeed = () -> feed.register(id, projection, (ViewStateRepository<S, ID>) store);
        } else {
            // Either recording is on, so every shape routes through the metadata-carrying BiFunction registration so
            // the recording wrapper is reachable, or store is a MaterializedView, which has no Projection-taking
            // DomainEventFeed.register overload to begin with. Built the same way the ViewStateRepository/
            // MaterializedView-taking register overloads build it internally (reactiveUpdateWithMetadata), so an
            // unwrapped registration folds exactly the same way it always did.
            //
            // This is a correctness fix independent of recording too: the previous MaterializedView branch used
            // reactiveUpdate's plain Function<E, Mono<Void>>, which folds every live event with
            // EventMetadata.empty() and so silently dropped appendid, and anything else metadata-keyed, even with
            // recording off.
            @SuppressWarnings("unchecked")
            BiFunction<EventMetadata, E, Mono<Void>> fold = store instanceof ViewStateRepository
                    ? Projections.reactiveUpdateWithMetadata(projection, (ViewStateRepository<S, ID>) store, id)
                    : Projections.reactiveUpdateWithMetadata((MaterializedView<E>) store);
            // A domain feed listens to no subscription model. The CatchupProjectionFeed handover behind it drives
            // the view-DSL ReplayAware lifecycle instead, which the recording wrapper forwards to and drives its own
            // bookkeeping from (ADR 132 decision 6).
            BiFunction<EventMetadata, E, Mono<Void>> effectiveFold = annotation.recordAppliedAppends()
                    ? applyRecording(id, fold, new CatchupResolution(null, true, false, catchesUp))
                    : fold;
            Filter replayFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
            registerOnFeed = () -> feed.register(id, effectiveFold, replayFilter);
        }
        if (subscriptionsStartOnTheirOwn(applicationContext)) {
            registerOnFeed.run();
            if (catchesUp) {
                domainFeedsToCatchUp.add(new DomainFeedCatchUp(id, feed,
                        SubscriptionAnnotations.pushCatchUpShouldWaitUntilStarted(annotation.startupMode())));
            } else {
                // Nothing to replay, so there is nothing to defer to catchUpCollectedFeeds(): go live right away.
                feed.goLive(id).block();
                withPushCatchupStatus(status -> status.recordLive(id));
            }
        } else {
            // register(...) alone puts the feed into buffering mode immediately, so deferring only the catch-up would
            // let accept(...) buffer into a bounded buffer rather than fold, and eventually overflow it. Defer both
            // together, so nothing about this projection reaches the feed until the application starts it, and
            // running the deferred work leaves the feed in the same state registering it under auto mode would.
            applicationContext.getBean(ManualStartPushSources.class).register(id, () -> {
                registerOnFeed.run();
                return catchesUp
                        ? recordingProgress(id, feed.catchUp(id))
                        : feed.goLive(id).doOnSuccess(ignored -> withPushCatchupStatus(status -> status.recordLive(id)));
            });
        }

    }

    // Common validation for a source=PUSH projection: no synchronous mode, no catch-up start knobs, must be a Projection.
    @SuppressWarnings("unchecked")
    private <S, E, ID> Projection<S, E, ID> validatePushDescriptor(org.occurrent.annotation.Projection annotation, String id, Object descriptor, boolean synchronous) {
        if (synchronous) {
            throw new IllegalArgumentException("@Projection '%s' cannot combine source=PUSH with mode=SYNCHRONOUS: a push feed is asynchronous.".formatted(id));
        }
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT || annotation.startAtGlobalPosition() >= 0
                || annotation.resumeBehavior() != ResumeBehavior.DEFAULT) {
            // The startupMode hint only makes sense under the default catchup. With catchup=NONE there is no replay to
            // move off the startup path, and startupMode is rejected there anyway (checked below).
            String reason = catchesUp
                    ? "It catches up before going live, but always from the beginning, so there is no start position to choose. Use startupMode = BACKGROUND to keep that replay off the startup path"
                    : "With catchup=NONE it takes live events only, so there is no history to position into";
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH cannot set startAt, startAtGlobalPosition or resumeBehavior. %s, and live-resume is the broker's responsibility.".formatted(id, reason));
        }
        if (!catchesUp && annotation.startupMode() != StartupMode.DEFAULT) {
            throw new IllegalArgumentException("@Projection '%s' combines source=PUSH with catchup=NONE, so it replays nothing and there is no startup work for startupMode to decide about. Remove startupMode, or drop catchup=NONE if you meant the projection to catch up first.".formatted(id));
        }
        if (!(descriptor instanceof Projection<?, ?, ?> raw)) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH must return a Projection. A DcbProjection push source is not supported, since a DCB boundary cannot be catch-up-replayed in position order.".formatted(id));
        }
        return (Projection<S, E, ID>) raw;
    }

    // Resolve the read-model store. On the reactive stack there is no zero-config store default (the view DSL's
    // materialization is blocking and a reactive store default is a planned follow-up), so a store bean is required: a
    // MaterializedView or a ViewStateRepository (any backend, driven reactively by the runner). Named by store() when
    // set, otherwise the unique bean of either type. Deliberately no DefaultProjectionStoreProvider seam: unlike the
    // blocking stack there is nothing for a store starter to contribute yet.
    private Object resolveStore(org.occurrent.annotation.Projection annotation, String id) {
        Object referencedStore = resolveStoreBeanByReference(annotation, id);
        if (referencedStore != null) {
            return requireReactiveStoreShape(referencedStore, id);
        }
        Object materializedView = uniqueStoreBeanOrThrow(MaterializedView.class, id);
        if (materializedView != null) {
            return materializedView;
        }
        Object repository = uniqueStoreBeanOrThrow(ViewStateRepository.class, id);
        if (repository != null) {
            return repository;
        }
        throw new IllegalArgumentException(("@Projection '%s' has no read-model store. On the reactive stack, declare a MaterializedView or ViewStateRepository bean and point at it with store = SomeStore.class or storeName = \"beanName\" (or make it the only bean of its type). A zero-config reactive store default is a planned follow-up, the blocking stack already has one.").formatted(id));
    }

    // Validate a referenced store bean is a shape the reactive stack supports. Unlike the blocking stack there is no
    // CrudRepository adapter or zero-config default here, so only a MaterializedView or ViewStateRepository is accepted.
    private Object requireReactiveStoreShape(Object bean, String id) {
        if (bean instanceof MaterializedView || bean instanceof ViewStateRepository) {
            return bean;
        }
        throw new IllegalArgumentException("@Projection '%s' store bean must be a MaterializedView or a ViewStateRepository, but was %s.".formatted(id, bean.getClass().getName()));
    }

    // Resolve the store bean referenced by store() (bean type) or storeName() (bean name), or null when neither is set
    // so the caller applies convention-based resolution. store() and storeName() together pick one bean of the type
    // when several exist.
    private Object resolveStoreBeanByReference(org.occurrent.annotation.Projection annotation, String id) {
        Class<?> storeType = annotation.store();
        String storeName = annotation.storeName();
        boolean byType = storeType != Void.class;
        boolean byName = !storeName.isBlank();
        if (byType) {
            if (byName) {
                try {
                    return applicationContext.getBean(storeName, storeType);
                } catch (BeansException e) {
                    throw new IllegalArgumentException("@Projection '%s' could not resolve a store bean named '%s' of type %s: %s".formatted(id, storeName, storeType.getName(), e.getMessage()), e);
                }
            }
            String[] names = applicationContext.getBeanNamesForType(storeType);
            if (names.length == 0) {
                throw new IllegalStateException("@Projection '%s' found no bean of type %s. Declare one, or leave store unset to resolve by convention.".formatted(id, storeType.getName()));
            }
            if (names.length > 1) {
                throw new IllegalStateException("@Projection '%s' found %d beans of type %s (%s) and cannot pick one. Disambiguate with storeName = \"beanName\".".formatted(id, names.length, storeType.getName(), String.join(", ", names)));
            }
            return applicationContext.getBean(names[0]);
        }
        if (byName) {
            try {
                return applicationContext.getBean(storeName);
            } catch (BeansException e) {
                throw new IllegalArgumentException("@Projection '%s' could not resolve a store bean named '%s': %s".formatted(id, storeName, e.getMessage()), e);
            }
        }
        return null;
    }

    // Returns the single bean of the given store type, or null when there is none so the caller tries the next type.
    // Throws when several beans of the type exist, since the application provided store beans but none is uniquely
    // selectable, so it names the ambiguity instead of failing later with a misleading "no store" message.
    private Object uniqueStoreBeanOrThrow(Class<?> storeType, String id) {
        String[] names = applicationContext.getBeanNamesForType(storeType);
        if (names.length == 0) {
            return null;
        }
        if (names.length > 1) {
            throw new IllegalStateException(("@Projection '%s' found %d %s beans (%s) and cannot pick one. Name the store bean with storeName = \"beanName\".").formatted(id, names.length, storeType.getSimpleName(), String.join(", ", names)));
        }
        return applicationContext.getBean(names[0]);
    }

    @SuppressWarnings("unchecked")
    private <E, S, ID> org.occurrent.subscription.api.reactor.SubscriptionHandle projectAgnosticOrStream(ReactiveProjectionRunner<E> runner, String id, org.occurrent.annotation.Projection annotation, Projection<S, E, ID> projection, Object store, @Nullable StartAt startAt, @Nullable CatchupResolution recordingResolution) {
        if (recordingResolution == null) {
            if (store instanceof MaterializedView) {
                return runner.project(id, projection, (MaterializedView<E>) store, startAt);
            }
            return runner.project(id, projection, (ViewStateRepository<S, ID>) store, startAt);
        }
        // Built the same way ReactiveProjectionRunner's own MaterializedView/ViewStateRepository overloads build it
        // internally, then wrapped, then handed to the BiFunction overload directly, so the recording wrapper sees
        // the same metadata the unwrapped path would have folded with.
        BiFunction<EventMetadata, E, Mono<Void>> update = store instanceof MaterializedView
                ? Projections.reactiveUpdateWithMetadata((MaterializedView<E>) store)
                : Projections.reactiveUpdateWithMetadata(projection, (ViewStateRepository<S, ID>) store, id);
        return runner.project(id, projection, applyRecording(id, update, recordingResolution), startAt);
    }

    @SuppressWarnings("unchecked")
    private <E, S, ID> org.occurrent.subscription.api.reactor.SubscriptionHandle projectDcb(ReactiveDcbProjectionRunner<E> runner, String id, org.occurrent.annotation.Projection annotation, DcbProjection<S, E, ID> dcbProjection, Object store, @Nullable DcbStartAt startAt, @Nullable CatchupResolution recordingResolution) {
        if (recordingResolution == null) {
            if (store instanceof MaterializedView) {
                return runner.project(id, dcbProjection, (MaterializedView<E>) store, startAt);
            }
            return runner.project(id, dcbProjection, (ViewStateRepository<S, ID>) store, startAt);
        }
        Projection<S, E, ID> projection = dcbProjection.projection();
        BiFunction<EventMetadata, E, Mono<Void>> update = store instanceof MaterializedView
                ? Projections.reactiveUpdateWithMetadata((MaterializedView<E>) store)
                : Projections.reactiveUpdateWithMetadata(projection, (ViewStateRepository<S, ID>) store, id);
        return runner.project(id, dcbProjection, applyRecording(id, update, recordingResolution), startAt);
    }


    // Unset knobs keep their own default, so setting one does not reset the other.
    // Package-private for a direct unit test: resolution is easy to get subtly wrong and needs no Spring context.
    static CatchupThenLiveOptions catchupThenLiveOptions(OccurrentProperties properties) {
        CatchupThenLiveProperties configured = properties.getSubscription().getCatchupThenLive();
        Integer dedupCacheSize = configured.getDedupCacheSize();
        Integer maxBufferedEvents = configured.getMaxBufferedEvents();
        if (dedupCacheSize == null && maxBufferedEvents == null) {
            return CatchupThenLiveOptions.defaults();
        }
        return new CatchupThenLiveOptions(
                dedupCacheSize == null ? CatchupThenLiveOptions.DEFAULT_DEDUP_CACHE_SIZE : dedupCacheSize,
                maxBufferedEvents == null ? CatchupThenLiveOptions.DEFAULT_MAX_BUFFERED_EVENTS : maxBufferedEvents);
    }
}
