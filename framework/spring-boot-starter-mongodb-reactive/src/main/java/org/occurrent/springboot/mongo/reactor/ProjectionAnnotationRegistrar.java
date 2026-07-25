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

package org.occurrent.springboot.mongo.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.projection.reactor.Projections;
import org.occurrent.dsl.projection.reactor.ReactiveDcbProjectionRunner;
import org.occurrent.dsl.projection.reactor.ReactiveProjectionRunner;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.mongo.common.OccurrentProperties;
import org.occurrent.springboot.mongo.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.occurrent.springboot.mongo.common.SubscriptionAnnotations.shouldWaitUntilStarted;

/**
 * Scans a bean for {@link org.occurrent.annotation.Projection} factory methods in
 * {@code afterSingletonsInstantiated} and registers each one, including PUSH and domain-push routing and
 * read-model-store resolution. Domain-push feeds are collected and caught up once, after every projection has
 * registered, via {@link #catchUpCollectedFeeds()}.
 */
class ProjectionAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final Set<String> registeredIds;
    private final StartPositionSupport startPositionSupport;

    // Domain-push feeds collected during projection registration, caught up once after all are registered.
    private final Set<DomainEventFeed<?>> domainFeedsToCatchUp = Collections.newSetFromMap(new IdentityHashMap<>());

    ProjectionAnnotationRegistrar(ApplicationContext applicationContext, Set<String> registeredIds, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.registeredIds = registeredIds;
        this.startPositionSupport = startPositionSupport;
    }

    @SuppressWarnings("unchecked")
    <E, S, ID> void processProjectionAnnotation(Object bean, Method method, org.occurrent.annotation.Projection annotation) {
        String id = annotation.id();
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("Duplicate subscription/projection id '%s' (used by @Projection on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(id, bean.getClass().getName(), method.getName()));
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
            Object feedBean = SubscriptionAnnotations.resolveFeedBean(applicationContext, annotation.subscriptionModel(), annotation.subscriptionModelName(), id, PushSubscriptionModel.class, DomainEventFeed.class);
            if (feedBean instanceof PushSubscriptionModel pushModel) {
                registerPushProjection(id, converter, descriptor, synchronous, annotation, pushModel);
            } else if (feedBean instanceof DomainEventFeed<?> domainFeed) {
                registerDomainPushProjection(id, converter, descriptor, synchronous, annotation, domainFeed);
            } else {
                throw new IllegalArgumentException("@Projection '%s' with source=PUSH resolved a %s, which is neither a PushSubscriptionModel nor a DomainEventFeed.".formatted(id, feedBean.getClass().getName()));
            }
            return;
        }

        if (descriptor instanceof DcbProjection<?, ?, ?> raw) {
            DcbProjection<S, E, ID> dcbProjection = (DcbProjection<S, E, ID>) raw;
            if (synchronous) {
                throw new IllegalArgumentException("@Projection '%s' returns a DcbProjection with mode = SYNCHRONOUS, which the reactive stack does not support in this version. Use mode = ASYNC for a DCB read model, or an agnostic Projection for synchronous read-your-writes.".formatted(id));
            }
            ReactiveDcbProjectionRunner<E> runner = ReactiveDcbProjectionRunner.create(applicationContext.getBean(SubscriptionModel.class), converter);
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            startPositionSupport.applyStartupWorkarounds();
            var subscription = projectDcb(runner, id, dcbProjection, resolveStore(annotation, id), startAt);
            if (shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
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
                Subscribable subscribable = applicationContext.getBean(Subscribable.class);
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
                if (shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                    subscription.waitUntilStarted().block();
                }
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
        }
    }

    // Catch up each domain-push feed once, after all its projections are registered.
    void catchUpCollectedFeeds() {
        for (DomainEventFeed<?> feed : domainFeedsToCatchUp) {
            feed.catchUpAll().block();
        }
    }

    // Register a source=PUSH projection whose feed bean is a PushSubscriptionModel (CloudEvents), wrapped in a
    // replay-then-push catch-up so a new or rebuilt projection is backfilled from the event store.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        PositionOrderedReader reader = applicationContext.getBean(PositionOrderedReader.class);
        CheckpointStorage catchupMarker = applicationContext.getBean(CheckpointStorage.class);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker, catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)));
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        ReactiveProjectionRunner<E> runner = stream ? ReactiveProjectionRunner.stream(model, converter) : ReactiveProjectionRunner.agnostic(model, converter);
        // The catch-up replay runs when the pipeline is subscribed; block until it has handed over to the live feed.
        var subscription = projectAgnosticOrStream(runner, id, projection, resolveStore(annotation, id), null);
        subscription.waitUntilStarted().block();
    }

    // Register a source=PUSH projection whose feed bean is a DomainEventFeed. The reactor feed folds via a
    // ViewStateRepository (through reactiveUpdate on boundedElastic), so the store must resolve to a ViewStateRepository.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerDomainPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, DomainEventFeed<?> feedBean) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        Object store = resolveStore(annotation, id);
        DomainEventFeed<E> feed = (DomainEventFeed<E>) feedBean;
        if (store instanceof ViewStateRepository) {
            feed.register(id, projection, (ViewStateRepository<S, ID>) store);
        } else {
            // resolveStore guarantees a ViewStateRepository or MaterializedView, so this is a MaterializedView. Drive it
            // with a reactive fold (folded on boundedElastic, as the normal reactor projection path does).
            Function<E, Mono<Void>> fold = Projections.reactiveUpdate((MaterializedView<E>) store);
            Filter replayFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
            feed.register(id, fold, replayFilter);
        }
        domainFeedsToCatchUp.add(feed);
    }

    // Common validation for a source=PUSH projection: no synchronous mode, no catch-up start knobs, must be a Projection.
    @SuppressWarnings("unchecked")
    private <S, E, ID> Projection<S, E, ID> validatePushDescriptor(org.occurrent.annotation.Projection annotation, String id, Object descriptor, boolean synchronous) {
        if (synchronous) {
            throw new IllegalArgumentException("@Projection '%s' cannot combine source=PUSH with mode=SYNCHRONOUS: a push feed is asynchronous.".formatted(id));
        }
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT || annotation.startAtGlobalPosition() >= 0
                || annotation.resumeBehavior() != ResumeBehavior.DEFAULT || annotation.startupMode() != StartupMode.DEFAULT) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH does not support the catch-up start knobs (startAt, startAtGlobalPosition, resumeBehavior, startupMode): the catch-up always replays from the beginning and live-resume is the broker's responsibility.".formatted(id));
        }
        if (!(descriptor instanceof Projection<?, ?, ?> raw)) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH must return a Projection. A DcbProjection push source is not supported, since a DCB boundary cannot be catch-up-replayed in position order.".formatted(id));
        }
        return (Projection<S, E, ID>) raw;
    }

    // Resolve the read-model store. On the reactive stack there is no zero-config Mongo default (the view DSL's
    // materialization is blocking and a reactive Mongo store is a planned follow-up), so a store bean is required: a
    // MaterializedView or a ViewStateRepository (any backend, driven reactively by the runner). Named by store() when
    // set, otherwise the unique bean of either type.
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
        throw new IllegalArgumentException(("@Projection '%s' has no read-model store. On the reactive stack, declare a MaterializedView or ViewStateRepository bean and point at it with store = SomeStore.class or storeName = \"beanName\" (or make it the only bean of its type). A zero-config reactive Mongo default is a planned follow-up, the blocking stack already has the Mongo default.").formatted(id));
    }

    // Validate a referenced store bean is a shape the reactive stack supports. Unlike the blocking stack there is no
    // CrudRepository adapter or Mongo default here, so only a MaterializedView or ViewStateRepository is accepted.
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
    private static CatchupThenLiveOptions catchupThenLiveOptions(OccurrentProperties properties) {
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
