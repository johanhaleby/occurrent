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
import org.occurrent.springboot.common.AsynchronousSubscribables;
import org.occurrent.springboot.common.PushCatchupStatus;
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
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Object descriptor = invokeFactory(method, bean);

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
            ReactiveDcbProjectionRunner<E> runner = ReactiveDcbProjectionRunner.create(applicationContext.getBean(FluxSubscriptionModel.class), converter);
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            startPositionSupport.applyStartupWorkarounds();
            var subscription = projectDcb(runner, id, dcbProjection, resolveStore(annotation, id), startAt);
            if (subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                subscription.waitUntilStarted().block();
            }
        } else if (descriptor instanceof Projection<?, ?, ?> raw) {
            Projection<S, E, ID> projection = (Projection<S, E, ID>) raw;
            boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
            if (synchronous) {
                // The synchronous subscription model has no lifecycle or start position, so nothing to wait for. It
                // delivers the just-written events on the write path (read-your-writes); the fold ignores unhandled types.
                ReactiveProjectionRunner<E> runner = ReactiveProjectionRunner.agnostic(applicationContext.getBean(SynchronousSubscriptionModel.class), converter);
                projectAgnosticOrStream(runner, id, projection, resolveStore(annotation, id), null);
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
                var subscription = projectAgnosticOrStream(runner, id, projection, resolveStore(annotation, id), startAt);
                if (subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                    subscription.waitUntilStarted().block();
                }
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
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
    private void withPushCatchupStatus(Consumer<PushCatchupStatus> action) {
        PushCatchupStatus status = applicationContext.getBeanProvider(PushCatchupStatus.class).getIfAvailable();
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

    // Register a source=PUSH projection whose feed bean is a PushSubscriptionModel (CloudEvents). Wrapped in a
    // replay-then-push catch-up so a new or rebuilt projection is backfilled from the event store, unless
    // catchup = NONE, where the bare model is used directly and no event store is touched at all.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
        Subscribable subscribable;
        if (catchesUp) {
            PositionOrderedReader reader = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", PositionOrderedReader.class, id);
            CheckpointStorage catchupMarker = SubscriptionAnnotations.resolveCatchupBean(applicationContext, "@Projection", CheckpointStorage.class, id);
            CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker, catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)));
            // Retained so close() can stop it. Its replay runs on boundedElastic, so a context that closes without
            // stopping it leaves that replay folding into a store that is closing with it.
            pushModels.add(model);
            // Asked rather than recorded, so a model that is stopped and started again, replaying a second time,
            // reports catching up again instead of staying at whatever it reached the first time.
            withPushCatchupStatus(status -> status.register(id, () -> model.isCatchingUp(id)));
            subscribable = model;
        } else {
            // catchup = NONE has no history to replay, so it is live from the start. Leaving it unknown would make a
            // readiness probe useless for exactly the projection that is always ready.
            withPushCatchupStatus(status -> status.recordLive(id));
            subscribable = pushModel;
        }
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        ReactiveProjectionRunner<E> runner = stream ? ReactiveProjectionRunner.stream(subscribable, converter) : ReactiveProjectionRunner.agnostic(subscribable, converter);
        Object store = resolveStore(annotation, id);
        if (subscriptionsStartOnTheirOwn(applicationContext)) {
            // Registration happens here, on the refresh thread, which is what captures an event committing mid-replay.
            // Only the replay is elsewhere.
            var subscription = projectAgnosticOrStream(runner, id, projection, store, null);
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
                    () -> projectAgnosticOrStream(runner, id, projection, store, null).waitUntilStarted());
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
        Runnable registerOnFeed;
        if (store instanceof ViewStateRepository) {
            registerOnFeed = () -> feed.register(id, projection, (ViewStateRepository<S, ID>) store);
        } else {
            // resolveStore guarantees a ViewStateRepository or MaterializedView, so this is a MaterializedView. Drive it
            // with a reactive fold (folded on boundedElastic, as the normal reactor projection path does).
            Function<E, Mono<Void>> fold = Projections.reactiveUpdate((MaterializedView<E>) store);
            Filter replayFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
            registerOnFeed = () -> feed.register(id, fold, replayFilter);
        }
        boolean catchesUp = annotation.catchup() == org.occurrent.annotation.Catchup.FROM_EVENT_STORE;
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
    private <E, S, ID> org.occurrent.subscription.api.reactor.Subscription projectAgnosticOrStream(ReactiveProjectionRunner<E> runner, String id, Projection<S, E, ID> projection, Object store, @Nullable StartAt startAt) {
        if (store instanceof MaterializedView) {
            return runner.project(id, projection, (MaterializedView<E>) store, startAt);
        }
        return runner.project(id, projection, (ViewStateRepository<S, ID>) store, startAt);
    }

    @SuppressWarnings("unchecked")
    private <E, S, ID> org.occurrent.subscription.api.reactor.Subscription projectDcb(ReactiveDcbProjectionRunner<E> runner, String id, DcbProjection<S, E, ID> dcbProjection, Object store, @Nullable DcbStartAt startAt) {
        if (store instanceof MaterializedView) {
            return runner.project(id, dcbProjection, (MaterializedView<E>) store, startAt);
        }
        return runner.project(id, dcbProjection, (ViewStateRepository<S, ID>) store, startAt);
    }

    private static Object invokeFactory(Method method, Object bean) {
        try {
            method.setAccessible(true);
            return method.invoke(bean);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to invoke @Projection factory %s#%s".formatted(bean.getClass().getName(), method.getName()), e);
        }
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
