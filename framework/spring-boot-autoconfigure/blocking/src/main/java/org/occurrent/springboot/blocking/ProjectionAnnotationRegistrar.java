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
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.projection.blocking.Projections;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.springboot.common.OccurrentProperties.SubscriptionProperties.CatchupThenLiveProperties;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.repository.CrudRepository;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Registers {@code @Projection} factory methods: resolves the read-model store, routes push/domain-push feeds, and
 * subscribes the materialized view. Invoked from the coordinator's {@code afterSingletonsInstantiated}, after all
 * subscription ids are collected, sharing the one duplicate-id registry. Domain-push feeds registered here are caught
 * up once through {@link #catchUpCollectedFeeds()} after all projections are registered.
 */
class ProjectionAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;
    private final Set<String> registeredIds;
    // Domain-push feeds collected during projection registration, caught up once after all are registered.
    private final Set<DomainEventFeed<?>> domainFeedsToCatchUp = Collections.newSetFromMap(new IdentityHashMap<>());
    // Push catch-up models created here, kept so the context can stop their replay threads on the way down.
    private final List<CatchupThenPushSubscriptionModel> pushModels = new ArrayList<>();

    ProjectionAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport, Set<String> registeredIds) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
        this.registeredIds = registeredIds;
    }

    // Stop every push catch-up model this registrar created, waiting for any replay still in flight to unwind, so no
    // replay thread survives the context that owns the store it is folding into.
    void close() {
        pushModels.forEach(CatchupThenPushSubscriptionModel::shutdown);
        pushModels.clear();
    }

    // Catch up each domain-push feed once, after all its projections are registered.
    void catchUpCollectedFeeds() {
        for (DomainEventFeed<?> feed : domainFeedsToCatchUp) {
            feed.catchUpAll();
        }
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
                registerPushProjection(method, annotation, id, converter, descriptor, synchronous, pushModel);
            } else if (feedBean instanceof DomainEventFeed<?> domainFeed) {
                registerDomainPushProjection(method, annotation, id, converter, descriptor, synchronous, domainFeed);
            } else {
                throw new IllegalArgumentException("@Projection '%s' with source=PUSH resolved a %s, which is neither a PushSubscriptionModel nor a DomainEventFeed.".formatted(id, feedBean.getClass().getName()));
            }
            return;
        }

        if (descriptor instanceof DcbProjection<?, ?, ?> raw) {
            DcbProjection<S, E, ID> dcbProjection = (DcbProjection<S, E, ID>) raw;
            MaterializedView<E> materializedView = resolveStore(annotation, method, dcbProjection.projection(), id);
            if (synchronous) {
                // The synchronous subscription model is capability-neutral and applies no DCB criteria, so a DCB
                // projection receives every synchronously dispatched event and the fold no-ops on unhandled types.
                Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
                synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(Filter.all()), StartAt.subscriptionModelDefault(), false, (metadata, event) -> {
                    materializedView.update(metadata, event);
                    return Unit.INSTANCE;
                });
                return;
            }
            DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);
            DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            startPositionSupport.applyStartupWorkarounds();
            var subscription = dcbSubscriptions.subscribeWithMetadata(id, dcbProjection.criteria(), startAt, (dcbMetadata, event) -> materializedView.update(dcbMetadata.eventMetadata(), event));
            if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext) && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                subscription.waitUntilStarted();
            }
        } else if (descriptor instanceof Projection<?, ?, ?> raw) {
            Projection<S, E, ID> projection = (Projection<S, E, ID>) raw;
            MaterializedView<E> materializedView = resolveStore(annotation, method, projection, id);
            Filter eventFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
            Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
                materializedView.update(metadata, event);
                return Unit.INSTANCE;
            };
            boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
            if (synchronous) {
                Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
                synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), StartAt.subscriptionModelDefault(), false, consumer);
                return;
            }
            StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
            boolean waitUntilStarted = SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext) && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
            startPositionSupport.applyStartupWorkarounds();
            if (stream) {
                StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);
                streamSubscriptions.subscribe(id, filter(eventFilter), startAt, waitUntilStarted, consumer);
            } else {
                Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);
                subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), startAt, waitUntilStarted, consumer);
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
        }
    }

    // Register a @Projection(source = PUSH): feed it from an external push subscription model, wrapped in a
    // replay-then-push catch-up so a new or rebuilt projection is backfilled from the event store first.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(Method method, org.occurrent.annotation.Projection annotation, String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        MaterializedView<E> materializedView = resolveStore(annotation, method, projection, id);
        PositionOrderedReader reader = applicationContext.getBean(PositionOrderedReader.class);
        CheckpointStorage catchupMarker = applicationContext.getBean(CheckpointStorage.class);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker, catchupThenLiveOptions(applicationContext.getBean(OccurrentProperties.class)));
        // Retained so close() can stop it. Its replay runs on its own thread, so a context that closes without
        // stopping it leaves that replay folding into a store that is closing with it.
        pushModels.add(model);
        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        ProjectionRunner<E> runner = stream ? ProjectionRunner.stream(model, converter) : ProjectionRunner.agnostic(model, converter);
        // Deliberately not SubscriptionAnnotations.shouldWaitUntilStarted, which maps DEFAULT to "background if it
        // replays history". A push catch-up always replays from the beginning, so that would silently move every
        // existing push projection off the startup path. Only an explicit BACKGROUND does that here.
        boolean waitUntilStarted = annotation.startupMode() != StartupMode.BACKGROUND;
        if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext)) {
            // With waitUntilStarted the catch-up replay finishes here before handing over to the live push feed;
            // without it the replay runs on its own thread and this returns straight away.
            runner.project(id, projection, materializedView, null, waitUntilStarted);
        } else {
            // This feed bypasses the SubscriptionModel bean entirely, so manual mode's own withholding never reaches
            // it. Defer the same call instead, to run once the application starts this projection itself.
            applicationContext.getBean(ManualStartProjections.class).register(id, () -> runner.project(id, projection, materializedView, null, waitUntilStarted));
        }
    }

    // Common validation for a source=PUSH projection: no synchronous mode, no catch-up start knobs, must be a Projection.
    @SuppressWarnings("unchecked")
    private <S, E, ID> Projection<S, E, ID> validatePushDescriptor(org.occurrent.annotation.Projection annotation, String id, Object descriptor, boolean synchronous) {
        if (synchronous) {
            throw new IllegalArgumentException("@Projection '%s' cannot combine source=PUSH with mode=SYNCHRONOUS: a push feed is asynchronous.".formatted(id));
        }
        if (annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT || annotation.startAtGlobalPosition() >= 0
                || annotation.resumeBehavior() != ResumeBehavior.DEFAULT) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH cannot set startAt, startAtGlobalPosition or resumeBehavior: the catch-up always replays from the beginning and live-resume is the broker's responsibility. startupMode is supported, so use startupMode = BACKGROUND to keep that replay off the startup path.".formatted(id));
        }
        if (!(descriptor instanceof Projection<?, ?, ?> raw)) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH must return a Projection. A DcbProjection push source is not supported, since a DCB boundary cannot be catch-up-replayed in position order.".formatted(id));
        }
        return (Projection<S, E, ID>) raw;
    }

    // Register a source=PUSH projection whose feed bean is a DomainEventFeed: the projection folds domain events directly
    // (no CloudEvent conversion on the live path), with a catch-up from the event store.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerDomainPushProjection(Method method, org.occurrent.annotation.Projection annotation, String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, DomainEventFeed<?> feedBean) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        MaterializedView<E> materializedView = resolveStore(annotation, method, projection, id);
        DomainEventFeed<E> feed = (DomainEventFeed<E>) feedBean;
        Filter eventFilter = ProjectionFilters.filterFor(converter, (Projection<?, E, ?>) projection);
        if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext)) {
            feed.register(id, materializedView, eventFilter);
            domainFeedsToCatchUp.add(feed);
        } else {
            // register(...) alone puts the feed into buffering mode immediately, so deferring only the catch-up would
            // let accept(...) buffer into a bounded buffer rather than fold, and eventually overflow it. Defer both
            // together, so nothing about this projection reaches the feed until the application starts it, and
            // running the deferred work leaves the feed in the same state registering it under auto mode would.
            applicationContext.getBean(ManualStartProjections.class).register(id, () -> {
                feed.register(id, materializedView, eventFilter);
                feed.catchUp(id);
            });
        }
    }

    // Resolve the read-model store into a MaterializedView. Selected by store() type or storeName() when set, otherwise
    // the unique bean of type MaterializedView, then ViewStateRepository, then Spring Data CrudRepository (any backend),
    // and finally the zero-config default the store starter contributes. All non-default options are first-class.
    @SuppressWarnings("unchecked")
    private <E, S, ID> MaterializedView<E> resolveStore(org.occurrent.annotation.Projection annotation, Method factoryMethod, Projection<S, E, ID> projection, String id) {
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

    private static Object invokeFactory(Method method, Object bean) {
        try {
            method.setAccessible(true);
            Object result = method.invoke(bean);
            if (result == null) {
                throw new IllegalStateException("@Projection factory %s#%s returned null.".formatted(bean.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke @Projection factory %s#%s.".formatted(bean.getClass().getName(), method.getName()), e);
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
