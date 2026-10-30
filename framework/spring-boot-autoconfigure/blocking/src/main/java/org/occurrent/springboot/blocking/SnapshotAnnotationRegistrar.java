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

import io.cloudevents.CloudEvent;
import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.internal.SkippedSaves;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.View;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.occurrent.filter.Filter;
import org.occurrent.filter.internal.EventTypeExpansion;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Registers {@code @Snapshot} factory methods, maintaining a per-stream (or per-DCB-boundary) resume-ready snapshot.
 * Invoked from the coordinator's {@code afterSingletonsInstantiated}, after projections.
 */
class SnapshotAnnotationRegistrar {

    private static final Logger log = LoggerFactory.getLogger(SnapshotAnnotationRegistrar.class);

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;

    SnapshotAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
    }

    // A @Snapshot maintains a per-stream, resume-ready snapshot: for each handled event it folds the event onto the
    // stored snapshot for that stream and saves the new state at the event's stream version. A schema-version change or a
    // gap makes the stored base reset to the beginning, in which case the range up to this event is folded from the store
    // so the snapshot rebuilds correctly.
    @SuppressWarnings("unchecked")
    <E, S> void processSnapshotAnnotation(Object bean, Method method, org.occurrent.annotation.Snapshot annotation) {
        String id = annotation.id();
        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException("@Snapshot factory method %s#%s must take no parameters and return a SnapshotView.".formatted(bean.getClass().getName(), method.getName()));
        }
        boolean synchronous = annotation.mode() == org.occurrent.annotation.Mode.SYNCHRONOUS;
        SubscriptionAnnotations.validateModeStartKnobs("@Snapshot", id, synchronous,
                annotation.startAt() != org.occurrent.annotation.StartPosition.BEGINNING,
                annotation.startAtGlobalPosition() >= 0,
                annotation.resumeBehavior() != org.occurrent.annotation.ResumeBehavior.DEFAULT,
                annotation.startupMode() != org.occurrent.annotation.StartupMode.DEFAULT);

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Object descriptor = SubscriptionAnnotations.invokeDescriptorFactory("@Snapshot", bean, method);
        int everyNEvents = annotation.everyNEvents();
        if (everyNEvents < 1) {
            throw new IllegalArgumentException("@Snapshot '%s' everyNEvents must be at least 1, but was %d.".formatted(id, everyNEvents));
        }
        SnapshotStore<S> store = resolveSnapshotStore(annotation, method, id);

        if (descriptor instanceof DcbSnapshotView<?, ?> rawDcb) {
            processDcbSnapshot(id, annotation, synchronous, converter, (DcbSnapshotView<S, E>) rawDcb, store, everyNEvents);
            return;
        }
        if (!(descriptor instanceof SnapshotView<?, ?>)) {
            throw new IllegalArgumentException("@Snapshot '%s' method %s#%s must return a SnapshotView or DcbSnapshotView, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor.getClass().getName()));
        }
        SnapshotView<S, E> snapshotView = (SnapshotView<S, E>) descriptor;
        int schemaVersion = snapshotView.schemaVersion();
        Filter eventFilter = snapshotFilterFor(converter, snapshotView);
        EventStore eventStore = applicationContext.getBean(EventStore.class);

        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            String key = metadata.getStreamId();
            long eventVersion = metadata.getStreamVersion();
            Optional<org.occurrent.dsl.snapshot.Snapshot<S>> loaded = store.findLatest(key);
            // A snapshot version at or beyond this delivery is normally a redelivery, but if the stream was reset below
            // the snapshot the snapshot is stale and resuming from it would freeze the maintainer forever. Only in that
            // ambiguous case do we probe the true head (a suffix read returns the real stream version regardless of
            // skip/limit); the happy path (eventVersion beyond the snapshot) pays no extra read. A head below the
            // snapshot version means a reset, so resolveBase demotes to initial and the range-fold below rebuilds and
            // self-heals (the save overwrites the stale snapshot at the reset version). Caching this probe was tried and
            // reverted: a cached confirmation cannot detect a reset that happens after it was cached, which reintroduces
            // the exact freeze this guard exists to prevent, so every ambiguous delivery is probed fresh.
            long observedHead = Long.MAX_VALUE;
            if (loaded.isPresent() && loaded.get().schemaVersion() == schemaVersion && eventVersion <= loaded.get().version()) {
                int snapshotVersion = SnapshotSupport.requireInt(loaded.get().version(), "the snapshot version used as the head-probe read offset");
                observedHead = eventStore.read(key, snapshotVersion, 1).version();
            }
            if (SnapshotSupport.isRedelivery(loaded, schemaVersion, eventVersion, observedHead)) {
                return Unit.INSTANCE; // already folded (a redelivery within the head), skip so folding stays idempotent
            }
            SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(loaded, schemaVersion, observedHead, snapshotView.view()::initialState);
            if (eventVersion - base.version() < everyNEvents) {
                return Unit.INSTANCE; // throttle: too few new events since the last saved snapshot, fold them in on a later save
            }
            S newState;
            if (eventVersion == base.version() + 1) {
                newState = snapshotView.view().evolve(base.state(), metadata, event);
            } else {
                newState = foldWithMetadata(snapshotView.view(), base.state(),
                        eventStore.read(key, Math.toIntExact(base.version()), Math.toIntExact(eventVersion - base.version())).eventList(), converter);
            }
            store.save(key, new org.occurrent.dsl.snapshot.Snapshot<>(newState, eventVersion, schemaVersion));
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
    }

    // A DCB @Snapshot keeps one snapshot per boundary, keyed by the canonical criteria key and saved at the marker
    // versions of the read that produced it. On each matching event it reads the events after the saved versions, folds
    // them onto the saved state and saves at the read's versions. A redelivered event is at or below the saved versions,
    // so the read doesn't return it. everyNEvents counts the events read since the last save.
    @SuppressWarnings("unchecked")
    private <E, S> void processDcbSnapshot(String id, org.occurrent.annotation.Snapshot annotation, boolean synchronous,
                                           CloudEventConverter<E> converter, DcbSnapshotView<S, E> dcbSnapshotView,
                                           SnapshotStore<S> store, int everyNEvents) {
        if (synchronous) {
            throw new IllegalArgumentException("@Snapshot '%s' returns a DcbSnapshotView with mode = SYNCHRONOUS, which is not supported. Use the default asynchronous mode for a DCB snapshot, or maintain a synchronous DCB snapshot through the DSL.".formatted(id));
        }
        DcbCriteria criteria = dcbSnapshotView.criteria();
        if (criteria instanceof DcbCriteria.MatchAll) {
            log.warn("@Snapshot '{}' reads every event of the store (DcbCriteria.all()), which has no marker versions to save a snapshot at, so no snapshot is kept for it.", id);
            return;
        }
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        SkippedSaves skippedSaves = new SkippedSaves(log);
        View<S, E> view = dcbSnapshotView.snapshotView().view();
        int schemaVersion = dcbSnapshotView.schemaVersion();
        DcbEventStore dcbEventStore = applicationContext.getBean(DcbEventStore.class);

        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        startPositionSupport.applyStartupWorkarounds();
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);
        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, (dcbMetadata, event) -> {
            SnapshotSupport.DcbBase<S> base = SnapshotSupport.resolveDcbBase(store.findLatest(key), schemaVersion, view::initialState);
            DcbEventStream read;
            try {
                read = dcbEventStore.read(criteria, base.markerVersions().map(DcbReadOptions::afterVersions).orElseGet(() -> DcbReadOptions.fromBeginning().withVersions()));
                SnapshotSupport.refuseEventBelowFoldedPosition(criteria, base, read);
            } catch (DcbSnapshotInvalidException e) {
                if (base.markerVersions().isEmpty()) {
                    throw e;
                }
                log.debug("@Snapshot '{}' reads the whole boundary since it can't resume from the saved marker versions: {}", id, e.getMessage());
                base = SnapshotSupport.DcbBase.initial(view::initialState);
                read = dcbEventStore.read(criteria, DcbReadOptions.fromBeginning().withVersions());
            }
            List<CloudEvent> events = read.events();
            if (events.size() < everyNEvents) {
                return; // throttle: too few matching events since the last saved snapshot
            }
            if (read.markerVersions().isEmpty()) {
                skippedSaves.skip(key, SkippedSaves.Reason.NO_MARKER_VERSIONS);
                return;
            }
            S newState = foldWithMetadata(view, base.state(), events, converter);
            store.save(key, org.occurrent.dsl.snapshot.Snapshot.atMarkerVersions(newState, read.markerVersions().get(), Math.max(base.highestFoldedPosition(), read.highestEventPosition()), schemaVersion));
        });
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext) && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
            subscription.waitUntilStarted();
        }
    }

    @SuppressWarnings("unchecked")
    private <S> SnapshotStore<S> resolveSnapshotStore(org.occurrent.annotation.Snapshot annotation, Method factoryMethod, String id) {
        Class<?> storeType = annotation.store();
        String storeName = annotation.storeName();
        boolean typeSet = storeType != Void.class;
        boolean nameSet = !storeName.isBlank();
        if (typeSet || nameSet) {
            Object bean = resolveReferencedSnapshotStore(storeType, storeName, typeSet, nameSet, id);
            if (!(bean instanceof SnapshotStore<?>)) {
                throw new IllegalArgumentException("@Snapshot '%s' store bean must be a SnapshotStore, but was %s.".formatted(id, bean.getClass().getName()));
            }
            return (SnapshotStore<S>) bean;
        }
        String[] names = applicationContext.getBeanNamesForType(SnapshotStore.class);
        if (names.length == 1) {
            return (SnapshotStore<S>) applicationContext.getBean(names[0]);
        }
        if (names.length > 1) {
            throw new IllegalStateException("@Snapshot '%s' found %d SnapshotStore beans (%s) and cannot pick one. Name one with storeName = \"beanName\".".formatted(id, names.length, String.join(", ", names)));
        }
        // The state type is reflected first, so a factory that declares none reports that (the actionable fix) rather
        // than a missing provider.
        Class<S> stateType = (Class<S>) reflectSnapshotStateType(factoryMethod, id);
        // getIfAvailable() applies @Primary and @Fallback resolution and only throws when the container genuinely
        // cannot pick, so an ambiguous seam is reported with the annotation id rather than as a bare Spring failure.
        final DefaultSnapshotStoreProvider provider;
        try {
            provider = applicationContext.getBeanProvider(DefaultSnapshotStoreProvider.class).getIfAvailable();
        } catch (NoUniqueBeanDefinitionException e) {
            String[] providerNames = applicationContext.getBeanNamesForType(DefaultSnapshotStoreProvider.class);
            throw new IllegalStateException(("@Snapshot '%s' found %d DefaultSnapshotStoreProvider beans (%s) and cannot pick one to create the zero-config default snapshot store. " +
                    "Declare a SnapshotStore bean, select one with store/storeName, or mark one provider @Primary.").formatted(id, providerNames.length, String.join(", ", providerNames)), e);
        }
        if (provider == null) {
            throw new IllegalStateException(("@Snapshot '%s' found no SnapshotStore bean and this starter contributes no zero-config default. " +
                    "Declare a SnapshotStore bean, or select one with store/storeName.").formatted(id));
        }
        return provider.createDefaultSnapshotStore(id, stateType);
    }

    private Object resolveReferencedSnapshotStore(Class<?> storeType, String storeName, boolean typeSet, boolean nameSet, String id) {
        if (typeSet && nameSet) {
            try {
                return applicationContext.getBean(storeName, storeType);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("@Snapshot '%s' could not resolve a store bean named '%s' of type %s: %s".formatted(id, storeName, storeType.getName(), e.getMessage()), e);
            }
        }
        if (typeSet) {
            String[] names = applicationContext.getBeanNamesForType(storeType);
            if (names.length == 0) {
                throw new IllegalStateException("@Snapshot '%s' found no bean of type %s. Declare one, or leave store unset to resolve by convention.".formatted(id, storeType.getName()));
            }
            if (names.length > 1) {
                throw new IllegalStateException("@Snapshot '%s' found %d beans of type %s (%s) and cannot pick one. Disambiguate with storeName = \"beanName\".".formatted(id, names.length, storeType.getName(), String.join(", ", names)));
            }
            return applicationContext.getBean(names[0]);
        }
        try {
            return applicationContext.getBean(storeName);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("@Snapshot '%s' could not resolve a store bean named '%s': %s".formatted(id, storeName, e.getMessage()), e);
        }
    }

    private static Class<?> reflectSnapshotStateType(Method factoryMethod, String id) {
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
        throw new IllegalArgumentException(("@Snapshot '%s' needs a snapshot store: either name one with store or storeName (a SnapshotStore bean), " +
                "or declare the factory return type with a concrete state type (for example SnapshotView<MyState, MyEvent>) so the snapshot can use the store's zero-config default.").formatted(id));
    }

    // Folds a range one CloudEvent at a time so each event keeps its own metadata. View.evolve(state, List) folds through
    // the two-argument evolve, which substitutes EventMetadata.empty(), and a metadata-reading fold cannot tolerate that.
    private static <S, E> S foldWithMetadata(View<S, E> view, S state, List<CloudEvent> cloudEvents, CloudEventConverter<E> converter) {
        S result = state;
        for (CloudEvent cloudEvent : cloudEvents) {
            result = view.evolve(result, EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <E> Filter snapshotFilterFor(CloudEventConverter<E> converter, SnapshotView<?, E> snapshotView) {
        Filter explicit = snapshotView.filter();
        if (explicit != null) {
            return explicit;
        }
        return EventTypeExpansion.deriveFilter(snapshotView.eventTypes(),
                type -> converter.getCloudEventType((Class<? extends E>) type),
                SnapshotAnnotationRegistrar::cannotDeriveFilterFor);
    }

    private static IllegalArgumentException cannotDeriveFilterFor(Class<?> eventType) {
        if (eventType.isArray()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a registered event type, since this expansion does not support an array. Register the concrete event types instead.");
        }
        if (eventType.isPrimitive()) {
            return new IllegalArgumentException(eventType.getTypeName()
                    + " cannot be a registered event type, since no event is ever an instance of a primitive type. Register the concrete event types instead.");
        }
        return new IllegalArgumentException("the concrete event types dispatch would accept for " + eventType.getName()
                + " cannot all be enumerated, so a filter derived from it would miss some of them. Register the concrete "
                + "event types instead, make " + eventType.getSimpleName() + " and every level below it final or sealed, "
                + "or set an explicit filter(...) on the SnapshotView, which is used instead of deriving one and is the "
                + "way out when a CloudEventTypeMapper of your own maps the whole hierarchy onto a single CloudEvent type string.");
    }

}
