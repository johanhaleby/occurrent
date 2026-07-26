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

package org.occurrent.springboot.mongo.blocking;

import kotlin.Unit;
import kotlin.jvm.functions.Function2;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.dsl.view.View;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.List;
import java.util.Optional;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Registers {@code @Snapshot} factory methods, maintaining a per-stream (or per-DCB-boundary) resume-ready snapshot.
 * Invoked from the coordinator's {@code afterSingletonsInstantiated}, after projections, sharing the one duplicate-id
 * registry.
 */
class SnapshotAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;
    private final Set<String> registeredIds;

    SnapshotAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport, Set<String> registeredIds) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
        this.registeredIds = registeredIds;
    }

    // A @Snapshot maintains a per-stream, resume-ready snapshot: for each handled event it folds the event onto the
    // stored snapshot for that stream and saves the new state at the event's stream version. A schema-version change or a
    // gap makes the stored base reset to the beginning, in which case the range up to this event is folded from the store
    // so the snapshot rebuilds correctly.
    @SuppressWarnings("unchecked")
    <E, S> void processSnapshotAnnotation(Object bean, Method method, org.occurrent.annotation.Snapshot annotation) {
        String id = annotation.id();
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("Duplicate subscription/projection/snapshot id '%s' (used by @Snapshot on %s#%s), each id must be unique because it is the durable checkpoint key.".formatted(id, bean.getClass().getName(), method.getName()));
        }
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
        Object descriptor = invokeSnapshotFactory(method, bean);
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
                newState = snapshotView.view().evolve(base.state(), event);
            } else {
                List<E> range = converter.toDomainEvents(eventStore.read(key, Math.toIntExact(base.version()), Math.toIntExact(eventVersion - base.version())).events()).toList();
                newState = snapshotView.view().evolve(base.state(), range);
            }
            store.save(key, new org.occurrent.dsl.snapshot.Snapshot<>(newState, eventVersion, schemaVersion));
            return Unit.INSTANCE;
        };

        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        if (synchronous) {
            Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingAnnotationBeanPostProcessor.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
            synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), StartAt.subscriptionModelDefault(), false, consumer);
            return;
        }
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean waitUntilStarted = SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        startPositionSupport.applyStartupWorkarounds();
        if (stream) {
            StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);
            streamSubscriptions.subscribe(id, filter(eventFilter), startAt, waitUntilStarted, consumer);
        } else {
            Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);
            subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), startAt, waitUntilStarted, consumer);
        }
    }

    // A DCB @Snapshot maintains one snapshot per boundary, keyed by the canonical criteria key and versioned by the
    // global DCB position. On each matching event it folds the events after the stored snapshot's position onto the
    // stored state and saves at the current position, so a rebuild after a schema change or a gap re-reads the boundary
    // rather than losing history. everyNEvents throttles by the number of matching events folded since the last save.
    @SuppressWarnings("unchecked")
    private <E, S> void processDcbSnapshot(String id, org.occurrent.annotation.Snapshot annotation, boolean synchronous,
                                           CloudEventConverter<E> converter, DcbSnapshotView<S, E> dcbSnapshotView,
                                           SnapshotStore<S> store, int everyNEvents) {
        if (synchronous) {
            throw new IllegalArgumentException("@Snapshot '%s' returns a DcbSnapshotView with mode = SYNCHRONOUS, which is not supported. Use the default asynchronous mode for a DCB snapshot, or maintain a synchronous DCB snapshot through the DSL.".formatted(id));
        }
        DcbCriteria criteria = dcbSnapshotView.criteria();
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        View<S, E> view = dcbSnapshotView.snapshotView().view();
        int schemaVersion = dcbSnapshotView.schemaVersion();
        DcbEventStore dcbEventStore = applicationContext.getBean(DcbEventStore.class);

        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        startPositionSupport.applyStartupWorkarounds();
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);
        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, (dcbMetadata, event) -> {
            long position = dcbMetadata.eventMetadata().getPosition();
            Optional<org.occurrent.dsl.snapshot.Snapshot<S>> loaded = store.findLatest(key);
            // DCB positions are global and monotonic, they never reset, so a snapshot can never be ahead of the true
            // head: no head probe is needed and the 3-arg isRedelivery is correct (unlike the stream path above).
            if (SnapshotSupport.isRedelivery(loaded, schemaVersion, position)) {
                return; // already folded (a redelivery), keep folding idempotent
            }
            SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(loaded, schemaVersion, view::initialState);
            if (position - base.version() < everyNEvents) {
                return; // throttle before reading, matching events cannot exceed the position gap since the snapshot
            }
            List<E> range = converter.toDomainEvents(dcbEventStore.read(criteria, DcbReadOptions.between(base.version(), position)).stream()).toList();
            if (range.size() < everyNEvents) {
                return; // throttle: too few matching events since the last saved snapshot
            }
            S newState = view.evolve(base.state(), range);
            store.save(key, new org.occurrent.dsl.snapshot.Snapshot<>(newState, position, schemaVersion));
        });
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
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
        MongoOperations mongoOperations = applicationContext.getBean(MongoOperations.class);
        Class<S> stateType = (Class<S>) reflectSnapshotStateType(factoryMethod, id);
        return new SpringMongoSnapshotStore<>(mongoOperations, stateType, "occurrent-snapshot-" + id);
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
                "or declare the factory return type with a concrete state type (for example SnapshotView<MyState, MyEvent>) so the snapshot can default to MongoDB.").formatted(id));
    }

    private static <E> Filter snapshotFilterFor(CloudEventConverter<E> converter, SnapshotView<?, E> snapshotView) {
        Filter explicit = snapshotView.filter();
        if (explicit != null) {
            return explicit;
        }
        List<Condition<String>> typeConditions = snapshotView.eventTypes().stream()
                .map(type -> Condition.eq(converter.getCloudEventType(type)))
                .toList();
        return switch (typeConditions.size()) {
            case 0 -> Filter.all();
            case 1 -> Filter.type(typeConditions.getFirst());
            default -> Filter.type(Condition.or(typeConditions));
        };
    }

    private static Object invokeSnapshotFactory(Method method, Object bean) {
        try {
            method.setAccessible(true);
            Object result = method.invoke(bean);
            if (result == null) {
                throw new IllegalStateException("@Snapshot factory %s#%s returned null.".formatted(bean.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke @Snapshot factory %s#%s.".formatted(bean.getClass().getName(), method.getName()), e);
        }
    }
}
