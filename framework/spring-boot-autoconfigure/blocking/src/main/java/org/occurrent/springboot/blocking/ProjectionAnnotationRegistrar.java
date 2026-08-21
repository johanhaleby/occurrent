/*
 *
 *  Copyright 2024 Johan Haleby
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

package org.occurrent.springboot.blocking;

import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.projection.blocking.RecordingMaterializedView;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.AppliedAppendRecordingRegistry;
import org.occurrent.springboot.common.PolledCatchupSignals;
import org.occurrent.springboot.common.PushCatchupStatusImpl;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelCapability;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.repository.CrudRepository;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Registers {@code @Projection} factory methods: resolves the read-model store, routes push/domain-push feeds, and
 * subscribes the materialized view. Invoked from the coordinator's {@code afterSingletonsInstantiated}, after all
 * subscription ids are collected, sharing the one duplicate-id registry. Domain-push feeds registered here are caught
 * up once through {@link #catchUpCollectedFeeds()} after all projections are registered.
 */
class ProjectionAnnotationRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ProjectionAnnotationRegistrar.class);

    // How long close() waits for the catch-ups it started itself, after the push models have already stopped theirs.
    // Long enough for a replay to notice the stop at its next event, short enough that a parked fold cannot hold a
    // closing context open.
    private static final Duration SHUTDOWN_CATCHUP_TIMEOUT = Duration.ofSeconds(5);

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;
    private final Set<String> registeredIds;
    // Resolves the competing-consumer strategy lazily, on the first checkpoint write a catch-up-then-push projection
    // makes, so this registrar does not force the strategy bean into existence while singletons are still being
    // instantiated (ADR 116).
    private final CompetingConsumerCheckpointWriteVersionSource writeVersionSource;
    // Domain-push feeds collected during projection registration, caught up once after every projection is registered.
    // A list rather than a set now that a feed carries one projection: each entry is one projection's catch-up, and
    // each carries its own startupMode, so there is nothing left to de-duplicate.
    private final List<DomainFeedCatchUp> domainFeedsToCatchUp = new ArrayList<>();
    // Push catch-up models created here, kept so the context can stop their replay threads on the way down.
    private final List<CatchupThenPushSubscriptionModel> pushModels = new ArrayList<>();
    // Catch-ups this registrar started on a thread of its own, plus how to stop each one. Concurrent because under
    // occurrent.subscription.mode = manual these are added from whichever thread calls ManualStartPushSources.start,
    // which can run long after refresh and alongside close().
    private final List<BackgroundCatchUp> backgroundCatchUps = new CopyOnWriteArrayList<>();
    // Set by close(). A background catch-up checks it before starting, because stopping a feed only takes effect once
    // the replay is running: a stop that lands before the thread gets scheduled would otherwise be cleared by the
    // catch-up itself and the whole history would replay into a closing store.
    private volatile boolean closing = false;

    // The applied-append recording poll's pacing (ADR 132 decision 7), the timer that fires each projection's due
    // tick, and the executor that actually runs one. All three created lazily on the first recordAppliedAppends =
    // true projection, so an application that never uses the feature pays for none of them. Split in two because
    // AppliedAppendStore's default retry is unbounded (a stuck clear() can run for a long time): a single small
    // timer keeps every projection's schedule on time regardless of what is currently blocked, and each fired tick
    // runs on its own virtual thread, so a stuck clear costs one cheap unmounted thread rather than a pooled
    // platform thread this registrar would otherwise have to size for the whole registration count.
    // recordingLock guards all three fields' initialization together with close()'s shutdown of the two executors,
    // since a manually started push projection can register concurrently with another one, or with close() itself.
    private final Object recordingLock = new Object();
    private @Nullable AppliedAppendRecordingRegistry recordingRegistry;
    private @Nullable ScheduledExecutorService recordingPollScheduler;
    private @Nullable ExecutorService recordingTickExecutor;

    private record DomainFeedCatchUp(String id, DomainEventFeed<?> feed, boolean waitUntilStarted) {
    }

    private record BackgroundCatchUp(Future<?> task, Runnable stop) {
    }

    ProjectionAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport, Set<String> registeredIds) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
        this.registeredIds = registeredIds;
        this.writeVersionSource = new CompetingConsumerCheckpointWriteVersionSource(applicationContext.getBeanProvider(CompetingConsumerStrategy.class),
                () -> CheckpointFencingConfigurationCheck.fenceCheckpoints(applicationContext.getBeanProvider(OccurrentProperties.class)));
    }

    // Stop every catch-up this registrar started or created a model for, waiting for any replay still in flight to
    // unwind, so no replay thread survives the context that owns the store it is folding into.
    void close() {
        // Stopped before the push models: a poll tick still in flight when shutdownNow() is called is interrupted,
        // and there is no in-flight replay of its own to wait for the way the push models below have. Setting
        // closing and shutting down share recordingLock with scheduleRecordingPoll's own use of it below, so a
        // registration racing this either creates the executors before this sees them (and this shuts down what it
        // just created) or sees closing already set (and creates nothing this would have to know about).
        //
        // shutdownNow() alone only requests interruption. A tick already blocked in AppliedAppendStore.clear() can
        // still be running once close() returns, against a store the context is tearing down. Both executors are
        // awaited outside recordingLock afterward, capped at the same deadline the catch-ups below use, so a stuck
        // clear cannot hold this method open forever.
        ScheduledExecutorService pollSchedulerToClose;
        ExecutorService tickExecutorToClose;
        synchronized (recordingLock) {
            closing = true;
            pollSchedulerToClose = recordingPollScheduler;
            if (pollSchedulerToClose != null) {
                pollSchedulerToClose.shutdownNow();
            }
            tickExecutorToClose = recordingTickExecutor;
            if (tickExecutorToClose != null) {
                tickExecutorToClose.shutdownNow();
            }
        }
        awaitTermination(pollSchedulerToClose);
        awaitTermination(tickExecutorToClose);
        // The models first, because their shutdown is what stops a push replay and so releases the watcher joined
        // below. Each model waits for its own replays, so this can take that long again before the join starts.
        pushModels.forEach(CatchupThenPushSubscriptionModel::shutdown);
        pushModels.clear();
        backgroundCatchUps.forEach(background -> background.stop().run());
        long deadline = System.nanoTime() + SHUTDOWN_CATCHUP_TIMEOUT.toNanos();
        for (BackgroundCatchUp background : backgroundCatchUps) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            try {
                background.task().get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (TimeoutException | ExecutionException e) {
                // A failure was already recorded and logged where it happened, and a shutdown has nowhere useful to
                // put either that or a timeout. Keep unwinding the rest.
            }
        }
        backgroundCatchUps.clear();
    }

    // Catch up each domain-push feed once, after every projection is registered.
    void catchUpCollectedFeeds() {
        for (DomainFeedCatchUp pending : domainFeedsToCatchUp) {
            if (pending.waitUntilStarted()) {
                recordingProgress(pending.id(), () -> pending.feed().catchUpAll()).run();
            } else {
                // startupMode = BACKGROUND. The feed itself deliberately has no background overload, since a caller
                // that wants the replay off its own thread can run catchUpAll() on a thread it owns. This is that
                // caller: the registrar is what knows the startupMode, so it is what decides the threading.
                runInBackground("occurrent-domain-feed-catchup", pending.id(),
                        recordingProgress(pending.id(), () -> pending.feed().catchUpAll()), pending.feed()::stopCatchUp);
            }
        }
        domainFeedsToCatchUp.clear();
    }

    // Run catch-up work on a virtual thread this registrar owns, recording a failure where the application can see it.
    // Nobody joins the task except close(), which is the whole point of BACKGROUND, so the failure has to be put
    // somewhere rather than thrown.
    private void runInBackground(String threadName, String id, Runnable work, Runnable stop) {
        FutureTask<Void> task = new FutureTask<>(() -> {
            try {
                if (closing) {
                    return null;
                }
                work.run();
            } catch (RuntimeException | Error e) {
                log.error("The background catch-up of projection {} failed. It has folded no history and will receive "
                        + "no live events until the application is restarted.", id, e);
                // getIfAvailable rather than getBean: the starter contributes this bean, but a context that wires the
                // post processor directly has no reason to, and losing the record is better than losing the log too.
                withPushCatchupStatus(status -> status.recordFailure(id, e));
            }
            return null;
        });
        backgroundCatchUps.add(new BackgroundCatchUp(task, stop));
        Thread.ofVirtual().name(threadName + "-" + id).start(task);
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
    // A replay that throws records neither, leaving runInBackground to record the failure instead.
    private Runnable recordingProgress(String id, Runnable replay) {
        return () -> {
            withPushCatchupStatus(status -> status.recordCatchingUp(id));
            replay.run();
            withPushCatchupStatus(status -> status.recordLive(id));
        };
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

    // The two sound cases the recordAppliedAppends-never-resets warning may fire on for a DCB/agnostic/stream
    // projection, per the epic's doctrine that a fact about a composition comes from the owner that composed it,
    // never from a probe or an annotation-only predicate:
    //   1. An explicit NOW, StartAt.now() is a documented, composition-independent contract every Subscribable
    //      must honor, true regardless of what the composition can do.
    //   2. DEFAULT, where the auto-configuration that composed this stack's default model registered, through
    //      ComposedDefaultStartPosition, that its own DEFAULT bypasses catch-up. True for Occurrent's shipped Mongo
    //      composition (issue 865), never assumed for an application-supplied one, whose own DEFAULT semantics are
    //      its own to declare.
    // Unlike the reactor stack, ReplayAwareSubscriptions.findIn already unwraps a wrapper chain here (ADR 132
    // decision 8), so there is no separate "structurally no catch-up layer" fact distinct from "unknown composition"
    // to draw on for this path, both collapse to the same empty capability lookup. DEFAULT on a composition with no
    // registered fact, and BEGINNING/a global position on a composition whose capability cannot be read, both stay
    // silent. This registrar cannot verify either one.
    private boolean verifiedNeverReplays(org.occurrent.annotation.Projection annotation) {
        if (annotation.startAt() == org.occurrent.annotation.StartPosition.NOW) {
            return true;
        }
        if (annotation.startAt() == org.occurrent.annotation.StartPosition.DEFAULT && annotation.startAtGlobalPosition() < 0) {
            // startAtGlobalPosition >= 0 overrides DEFAULT and replays from that position regardless
            // (generateAgnosticStartAt/generateDcbStartAt check it first), so it must be excluded here too, or a
            // projection that genuinely replays from an explicit position would get the never-replays warning.
            ComposedDefaultStartPosition holder = applicationContext.getBeanProvider(ComposedDefaultStartPosition.class).getIfAvailable();
            return holder != null && holder.isDefaultKnownLiveOnly();
        }
        return false;
    }

    // Wraps materializedView in the applied-append recorder when the annotation asks for it, told about its
    // catch-ups by the same model instance the caller's own subscription actually runs on rather than an
    // independently resolved bean of the same type, since a context can have more than one and the two could
    // disagree about which is catching up. ReplayAwareSubscriptions.findIn(...) unwraps whatever wrapper chain sits
    // in front of the catch-up model (ADR 132 decision 8), the same lookup SagaAnnotationRegistrar already relies on
    // for its timer gate. Empty means the composition cannot say, and the projection is wrapped with nothing
    // watching it. Returns materializedView unchanged when recording is off.
    private <E> MaterializedView<E> wrapForRecordingIfNeeded(org.occurrent.annotation.Projection annotation, String id, MaterializedView<E> materializedView, SubscriptionModelCapability capability) {
        if (!annotation.recordAppliedAppends()) {
            return materializedView;
        }
        warnIfRecordingNeverResets(id, true, verifiedNeverReplays(annotation));
        return wrapForRecording(annotation, id, materializedView, ReplayAwareSubscriptions.findIn(capability).orElse(null), true);
    }

    // Wraps materializedView in the applied-append recorder when the annotation asks for it, listening to
    // catchupModel for the catch-up boundaries. A null catchupModel is a composition whose catch-ups nothing can
    // learn about, which is also every composition that never has any. Returns materializedView unchanged when
    // recording is off.
    private <E> MaterializedView<E> wrapForRecording(org.occurrent.annotation.Projection annotation, String id, MaterializedView<E> materializedView, @Nullable ReplayAwareSubscriptions catchupModel, boolean hasCatchups) {
        if (!annotation.recordAppliedAppends()) {
            return materializedView;
        }
        AppliedAppendStore store = resolveAppliedAppendStore(id);
        RecordingMaterializedView<E> recordingView = Projections.recordingAppliedAppends(materializedView, id, store);
        if (catchupModel != null) {
            // Registered before the subscription that produces the catch-ups is started below, so a catch-up that
            // begins the moment it starts is heard rather than recorded as though it were live.
            if (catchupModel.listenForCatchup(id, recordingView)) {
                recordingRegistry().register(id, recordingView);
            } else {
                recordingRegistry().register(id, new PolledCatchupSignals(recordingView, () -> catchupModel.isCatchingUp(id)));
            }
            scheduleRecordingPoll(id);
        } else if (hasCatchups) {
            // A pull feed drives its own replay and needs no watching, but the clear that replay owes can still fail,
            // and a feed that then goes quiet has nothing left to retry it. The poll does.
            recordingRegistry().register(id, recordingView);
            scheduleRecordingPoll(id);
        }
        return recordingView;
    }

    private AppliedAppendStore resolveAppliedAppendStore(String id) {
        AppliedAppendStore store = applicationContext.getBeanProvider(AppliedAppendStore.class).getIfAvailable();
        if (store == null) {
            throw AppliedAppendRecordingRegistry.noAppliedAppendStoreConfigured(id);
        }
        return store;
    }

    private AppliedAppendRecordingRegistry recordingRegistry() {
        synchronized (recordingLock) {
            if (recordingRegistry == null) {
                OccurrentProperties.ProjectionProperties.AppliedAppendProperties.ReplayPollProperties pollProperties =
                        applicationContext.getBean(OccurrentProperties.class).getProjection().getAppliedAppend().getReplayPoll();
                recordingRegistry = new AppliedAppendRecordingRegistry(pollProperties.getInitial(), pollProperties.getMax(), pollProperties.getMultiplier());
            }
            return recordingRegistry;
        }
    }

    // Self-rescheduling: each tick asks the registry how long until the projection is next due (an interval the
    // tick itself may have just changed) and schedules exactly one more tick at that delay, rather than running a
    // shared fixed-rate loop that wakes up for every registered projection whether or not it is due.
    //
    // The closing check, both executors' lazy creation, and the actual schedule() call all share recordingLock with
    // close()'s own shutdown, for the same reason the reactor registrar's equivalent method does: checking closing
    // or creating an executor outside that lock leaves a window where an executor created after close() ran is
    // never shut down.
    private void scheduleRecordingPoll(String id) {
        AppliedAppendRecordingRegistry registry = recordingRegistry();
        synchronized (recordingLock) {
            if (closing) {
                return;
            }
            // Timing only, kept deliberately small. This thread never runs a tick itself, only fires the moment one
            // is due and hands it to recordingTickExecutor, so a stuck clear() on one projection never delays
            // another's timer.
            if (recordingPollScheduler == null) {
                recordingPollScheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory("occurrent-applied-append-poll-timer"));
            }
            // Where a fired tick actually runs. A platform-thread pool sized for the registration count would either
            // sit near-idle most of the time (every healthy projection's clear returns fast) or, sized down, let one
            // stuck clear (AppliedAppendStore's default retry is unbounded, decision 7) queue up every other
            // projection's tick behind it. Virtual threads sidestep both, cheap enough to hand out one per fired
            // tick, so a stuck clear occupies only its own and every other projection's tick still runs on time.
            if (recordingTickExecutor == null) {
                recordingTickExecutor = Executors.newVirtualThreadPerTaskExecutor();
            }
            ExecutorService tickExecutor = recordingTickExecutor;
            recordingPollScheduler.schedule(() -> tickExecutor.execute(() -> {
                try {
                    registry.tick(id);
                } catch (RuntimeException e) {
                    log.error("The applied-append recording poll for projection '{}' failed. It will be retried at the next tick.", id, e);
                }
                scheduleRecordingPoll(id);
            }), registry.dueInNanos(id), TimeUnit.NANOSECONDS);
        }
    }

    // Waits for an already-shutdownNow() executor to actually finish whatever it was running, capped at
    // SHUTDOWN_CATCHUP_TIMEOUT so a tick stuck in a blocking clear() cannot hold close() open forever.
    private static void awaitTermination(@Nullable ExecutorService executor) {
        if (executor == null) {
            return;
        }
        try {
            executor.awaitTermination(SHUTDOWN_CATCHUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory daemonThreadFactory(String namePrefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, namePrefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
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
                registerPushProjection(method, annotation, id, converter, descriptor, synchronous, pushModel);
            } else if (feedBean instanceof DomainEventFeed<?> domainFeed) {
                registerDomainPushProjection(method, annotation, id, converter, descriptor, synchronous, domainFeed);
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
            // Resolved once, in the same place it always was, before any subscription bean: a raw return type with
            // no store bean has to fail here regardless of which subscription bean this projection would go on to need.
            MaterializedView<E> resolvedView = resolveStoreView(annotation, method, dcbProjection.projection(), id);
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            if (synchronous) {
                // The synchronous subscription model is capability-neutral and applies no DCB criteria, so a DCB
                // projection receives every synchronously dispatched event and the fold no-ops on unhandled types.
                // recordAppliedAppends is refused earlier whenever mode = SYNCHRONOUS, so wrapForRecordingIfNeeded
                // is always a no-op here, but resolving the bean first still keeps this path shaped like the one
                // below rather than a special case that happens to be safe only by that refusal holding.
                Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
                MaterializedView<E> materializedView = wrapForRecordingIfNeeded(annotation, id, resolvedView, synchronousSubscriptions.getSubscriptionModel());
                synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.subscriptionModelDefault(), false, (metadata, event) -> {
                    materializedView.update(metadata, event);
                    return Unit.INSTANCE;
                });
                return;
            }
            // Resolved before wrapping the view, so the recording phase (when recordAppliedAppends is set) comes
            // from the exact model this subscription runs on, not an independently resolved bean of the same type.
            DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);
            MaterializedView<E> materializedView = wrapForRecordingIfNeeded(annotation, id, resolvedView, dcbSubscriptions.subscriptionModel());
            DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            startPositionSupport.applyStartupWorkarounds();
            var subscription = dcbSubscriptions.subscribeWithMetadata(id, dcbProjection.criteria(), startAt, (dcbMetadata, event) -> materializedView.update(dcbMetadata.eventMetadata(), event));
            if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext) && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                subscription.waitUntilStarted();
            }
        } else if (descriptor instanceof Projection<?, ?, ?> raw) {
            Projection<S, E, ID> projection = (Projection<S, E, ID>) raw;
            // Resolved once, in the same place it always was, before any subscription bean: a raw return type with
            // no store bean has to fail here regardless of which subscription bean this projection would go on to need.
            MaterializedView<E> resolvedView = resolveStoreView(annotation, method, projection, id);
            Filter eventFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
            boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            if (synchronous) {
                // recordAppliedAppends is refused earlier whenever mode = SYNCHRONOUS, so wrapForRecordingIfNeeded
                // is always a no-op here, for the same reason the DCB branch above resolves the bean first anyway.
                Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
                MaterializedView<E> materializedView = wrapForRecordingIfNeeded(annotation, id, resolvedView, synchronousSubscriptions.getSubscriptionModel());
                synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), StartAt.subscriptionModelDefault(), false, (metadata, event) -> {
                    materializedView.update(metadata, event);
                    return Unit.INSTANCE;
                });
                return;
            }
            StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            boolean waitUntilStarted = SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext) && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
            startPositionSupport.applyStartupWorkarounds();
            // Resolved before wrapping the view, so the recording phase (when recordAppliedAppends is set) comes
            // from the exact model this subscription runs on, not an independently resolved bean of the same type.
            if (stream) {
                StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);
                MaterializedView<E> materializedView = wrapForRecordingIfNeeded(annotation, id, resolvedView, streamSubscriptions.getSubscriptionModel());
                streamSubscriptions.subscribe(id, filter(eventFilter), startAt, waitUntilStarted, (metadata, event) -> {
                    materializedView.update(metadata, event);
                    return Unit.INSTANCE;
                });
            } else {
                Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);
                MaterializedView<E> materializedView = wrapForRecordingIfNeeded(annotation, id, resolvedView, subscriptions.getSubscriptionModel());
                subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), startAt, waitUntilStarted, (metadata, event) -> {
                    materializedView.update(metadata, event);
                    return Unit.INSTANCE;
                });
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor.getClass().getName()));
        }
    }

    // Register a @Projection(source = PUSH) fed by a bare PushSubscriptionModel. Wrapped in a replay-then-push
    // catch-up so a new or rebuilt projection is backfilled from the event store first, unless catchup = NONE, where
    // the bare model is used directly and no event store is touched at all.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(Method method, org.occurrent.annotation.Projection annotation, String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        MaterializedView<E> resolvedView = resolveStoreView(annotation, method, projection, id);
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        Subscribable subscribable;
        ReplayAwareSubscriptions catchupModel;
        if (catchesUp) {
            PositionOrderedReader reader = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", PositionOrderedReader.class, id);
            CheckpointStorage catchupMarker = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", CheckpointStorage.class, id);
            CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker,
                    catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)), writeVersionSource);
            // Retained so close() can stop it. Its replay runs on its own thread, so a context that closes without
            // stopping it leaves that replay folding into a store that is closing with it.
            pushModels.add(model);
            // Asked rather than recorded, so a model that is stopped and started again, replaying a second time,
            // reports catching up again instead of staying at whatever it reached the first time.
            withPushCatchupStatus(status -> status.register(id, () -> model.isCatchingUp(id), () -> model.isRunning(id)));
            // Published so a CloudEvent-level broker bridge, wired in a separate starter module that never depends
            // on this one, can look this exact object up and gate its own consumption on
            // model::isReadyForLiveDelivery. See CatchupThenPushSubscriptionModelPublisher.
            CatchupThenPushSubscriptionModelPublisher.publish(applicationContext, id, model, log);
            subscribable = model;
            catchupModel = model;
        } else {
            // catchup = NONE never replays, so it is live as soon as it is running. Asked rather than recorded because
            // occurrent.subscription.mode = manual defers the subscription, and a recorded Live would tell a readiness
            // probe that a projection nobody has started yet is ready to serve.
            withPushCatchupStatus(status -> status.register(id, () -> false, () -> pushModel.isRunning(id)));
            subscribable = pushModel;
            catchupModel = null;
        }
        // Listens for catch-ups only when catchesUp, since catchup = NONE has none to hear about.
        warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), !catchesUp);
        MaterializedView<E> materializedView = wrapForRecording(annotation, id, resolvedView, catchupModel, catchesUp);
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        ProjectionRunner<E> runner = stream ? ProjectionRunner.stream(subscribable, converter) : ProjectionRunner.agnostic(subscribable, converter);
        // No catchesUp guard needed: with catchup = NONE, validatePushDescriptor already rejected any startupMode but
        // the default, so this is true there too, and a bare push subscription with nothing to replay resolves it
        // immediately either way.
        boolean waitUntilStarted = SubscriptionAnnotations.pushCatchUpShouldWaitUntilStarted(annotation.startupMode());
        if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext)) {
            // With waitUntilStarted the catch-up replay finishes here before handing over to the live push feed;
            // without it the replay runs on its own thread and this returns straight away.
            Subscription subscription = runner.project(id, projection, materializedView, null, waitUntilStarted);
            if (!waitUntilStarted) {
                // Nobody is left to see this replay fail, so join it on a thread of this registrar's own purely to
                // record the failure. Stopping it is close()'s job through the model, so this needs no stop of its own.
                runInBackground("occurrent-push-catchup-watch", id, subscription::waitUntilStarted, () -> {
                });
            }
            return;
        }
        // This feed bypasses the SubscriptionModel bean entirely, so manual mode's own withholding never reaches it.
        // Defer the same work instead, to run once the application starts this projection itself. It has to be the
        // same work: ManualStartPushSources.start returns void, so the application never sees the handle and could
        // not watch a background replay for itself.
        applicationContext.getBean(ManualStartPushSources.class).register(id, () -> {
            Subscription deferred = runner.project(id, projection, materializedView, null, waitUntilStarted);
            if (!waitUntilStarted) {
                runInBackground("occurrent-push-catchup-watch", id, deferred::waitUntilStarted, () -> {
                });
            }
        });
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

    // Register a source=PUSH projection whose feed bean is a DomainEventFeed: the projection folds domain events directly
    // (no CloudEvent conversion on the live path). Catches up from the event store unless catchup = NONE, where it goes
    // live immediately instead and touches no event store at all.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerDomainPushProjection(Method method, org.occurrent.annotation.Projection annotation, String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, DomainEventFeed<?> feedBean) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        // Computed early: catchesUp is what actually decides whether this projection replays (it drives the view-DSL
        // replay lifecycle below, not a subscription model), so warnIfRecordingNeverResets
        // needs it before the view is wrapped.
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        warnIfRecordingNeverResets(id, annotation.recordAppliedAppends(), !catchesUp);
        // Listens to no model: a domain feed's own ReplayAware lifecycle (forwarded to the recording wrapper
        // through CatchupProjectionFeed's instanceof probe) is this composition's only replay signal, and there is
        // no subscription model behind it to hear the same catch-up from twice (ADR 132 decisions 8 and 12).
        MaterializedView<E> materializedView = wrapForRecording(annotation, id, resolveStoreView(annotation, method, projection, id), null, catchesUp);
        DomainEventFeed<E> feed = (DomainEventFeed<E>) feedBean;
        Filter eventFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
        // Only read below where catchesUp is true, where a goLive() branch runs instead.
        boolean waitUntilStarted = SubscriptionAnnotations.pushCatchUpShouldWaitUntilStarted(annotation.startupMode());
        if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext)) {
            feed.register(id, materializedView, eventFilter);
            if (catchesUp) {
                domainFeedsToCatchUp.add(new DomainFeedCatchUp(id, feed, waitUntilStarted));
            } else {
                // Nothing to replay, so there is nothing to defer to catchUpCollectedFeeds(): go live right away.
                feed.goLive(id);
                withPushCatchupStatus(status -> status.recordLive(id));
            }
        } else {
            // register(...) alone puts the feed into buffering mode immediately, so deferring only the catch-up would
            // let accept(...) buffer into a bounded buffer rather than fold, and eventually overflow it. Defer both
            // together, so nothing about this projection reaches the feed until the application starts it, and
            // running the deferred work leaves the feed in the same state registering it under auto mode would.
            applicationContext.getBean(ManualStartPushSources.class).register(id, () -> {
                feed.register(id, materializedView, eventFilter);
                if (!catchesUp) {
                    feed.goLive(id);
                    withPushCatchupStatus(status -> status.recordLive(id));
                } else if (waitUntilStarted) {
                    recordingProgress(id, () -> feed.catchUp(id)).run();
                } else {
                    // Same treatment as auto mode, or startAll() would block for a full replay on a projection that
                    // asked for BACKGROUND.
                    runInBackground("occurrent-domain-feed-catchup", id, recordingProgress(id, () -> feed.catchUp(id)), feed::stopCatchUp);
                }
            });
        }
    }

    // Resolve the read-model store into a MaterializedView. Selected by store() type or storeName() when set, otherwise
    // the unique bean of type MaterializedView, then ViewStateRepository, then Spring Data CrudRepository (any backend),
    // and finally the zero-config default the store starter contributes. All non-default options are first-class.
    @SuppressWarnings("unchecked")
    private <E, S, ID> MaterializedView<E> resolveStoreView(org.occurrent.annotation.Projection annotation, Method factoryMethod, Projection<S, E, ID> projection, String id) {
        Object referencedStore = resolveStoreBeanByReference(annotation, id);
        if (referencedStore != null) {
            return toMaterializedView(referencedStore, projection, id);
        }
        Object materializedView = uniqueStoreBeanOrThrow(MaterializedView.class, id);
        if (materializedView != null) {
            return (MaterializedView<E>) materializedView;
        }
        Object repository = uniqueStoreBeanOrThrow(ViewStateRepository.class, id);
        if (repository != null) {
            return Projections.materializedView(projection, (ViewStateRepository<S, ID>) repository, id);
        }
        Object crudRepository = uniqueStoreBeanOrThrow(CrudRepository.class, id);
        if (crudRepository != null) {
            return Projections.materializedView(projection, crudBackedRepository((CrudRepository<S, ID>) crudRepository), id);
        }
        // No candidate store bean of any type exists, so fall back to the store starter's zero-config default. The
        // state type is reflected first, so a factory that declares none reports that (the actionable fix) rather than
        // a missing provider.
        Class<S> stateType = (Class<S>) reflectStateType(factoryMethod, id);
        return Projections.materializedView(projection, defaultProjectionStore(stateType, id), id);
    }

    private <S, ID> ViewStateRepository<S, ID> defaultProjectionStore(Class<S> stateType, String id) {
        // getIfAvailable() applies @Primary and @Fallback resolution and only throws when the container genuinely
        // cannot pick, so an ambiguous seam is reported with the annotation id rather than as a bare Spring failure.
        final DefaultProjectionStoreProvider provider;
        try {
            provider = applicationContext.getBeanProvider(DefaultProjectionStoreProvider.class).getIfAvailable();
        } catch (NoUniqueBeanDefinitionException e) {
            String[] providerNames = applicationContext.getBeanNamesForType(DefaultProjectionStoreProvider.class);
            throw new IllegalStateException(("@Projection '%s' found %d DefaultProjectionStoreProvider beans (%s) and cannot pick one to create the zero-config default read-model store. " +
                    "Declare a MaterializedView, ViewStateRepository or CrudRepository bean, select one with store/storeName, or mark one provider @Primary.").formatted(id, providerNames.length, String.join(", ", providerNames)), e);
        }
        if (provider == null) {
            throw new IllegalStateException(("@Projection '%s' found no read-model store bean and this starter contributes no zero-config default. " +
                    "Declare a MaterializedView, ViewStateRepository or CrudRepository bean, or select one with store/storeName.").formatted(id));
        }
        return provider.createDefaultProjectionStore(id, stateType);
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

    // Returns the single bean of the given store type, or null when there is none so the caller tries the next type
    // (and finally the zero-config default). Throws when several beans of the type exist, since the application
    // provided store beans but none is uniquely selectable, and silently materializing elsewhere would hide that.
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
    private <E, S, ID> MaterializedView<E> toMaterializedView(Object storeBean, Projection<S, E, ID> projection, String id) {
        if (storeBean instanceof MaterializedView<?> materializedView) {
            return (MaterializedView<E>) materializedView;
        }
        if (storeBean instanceof ViewStateRepository<?, ?> repository) {
            return Projections.materializedView(projection, (ViewStateRepository<S, ID>) repository, id);
        }
        if (storeBean instanceof CrudRepository<?, ?> crudRepository) {
            return Projections.materializedView(projection, crudBackedRepository((CrudRepository<S, ID>) crudRepository), id);
        }
        throw new IllegalArgumentException("@Projection '%s' store bean must be a MaterializedView, a ViewStateRepository, or a Spring Data CrudRepository, but was %s.".formatted(id, storeBean.getClass().getName()));
    }

    private <S, ID> ViewStateRepository<S, ID> crudBackedRepository(CrudRepository<S, ID> crudRepository) {
        return ViewStateRepository.create(
                instanceId -> crudRepository.findById(instanceId).orElse(null),
                (instanceId, state) -> crudRepository.save(state));
    }

    private static Class<?> reflectStateType(Method factoryMethod, String id) {
        Type returnType = factoryMethod.getGenericReturnType();
        if (returnType instanceof ParameterizedType parameterizedType) {
            Type[] arguments = parameterizedType.getActualTypeArguments();
            if (arguments.length >= 1) {
                Type stateArgument = arguments[0];
                if (stateArgument instanceof Class<?> stateClass) {
                    return stateClass;
                }
                if (stateArgument instanceof ParameterizedType stateParameterized && stateParameterized.getRawType() instanceof Class<?> rawState) {
                    return rawState;
                }
            }
        }
        throw new IllegalArgumentException(("@Projection '%s' needs a read-model store: either name one with store=\"beanName\" (a MaterializedView, ViewStateRepository, or CrudRepository), " +
                "or declare the factory return type with a concrete state type (for example Projection<MyView, MyEvent, String>) so the read model can use the store's zero-config default.").formatted(id));
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
