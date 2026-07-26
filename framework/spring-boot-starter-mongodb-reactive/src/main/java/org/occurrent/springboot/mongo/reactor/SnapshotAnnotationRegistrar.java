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

import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.snapshot.mongodb.spring.reactor.ReactiveSpringMongoSnapshotStore;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.View;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;
import kotlin.jvm.functions.Function2;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.occurrent.springboot.common.SubscriptionAnnotations.shouldWaitUntilStarted;
import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Scans a bean for {@link org.occurrent.annotation.Snapshot} factory methods in {@code afterSingletonsInstantiated}
 * and maintains a per-stream (or, for DCB, per-boundary) snapshot for each one.
 */
class SnapshotAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final Set<String> registeredIds;
    private final StartPositionSupport startPositionSupport;

    SnapshotAnnotationRegistrar(ApplicationContext applicationContext, Set<String> registeredIds, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.registeredIds = registeredIds;
        this.startPositionSupport = startPositionSupport;
    }

    // A @Snapshot maintains a per-stream, resume-ready snapshot: for each handled event it folds the event onto the
    // stored snapshot for that stream and saves the new state at the event's stream version, all composed reactively. A
    // schema-version change or a gap rebuilds by folding the range up to this event from the store. The store save is
    // best-effort at the reactive DSL level, but here a maintained failure surfaces to the durable subscription for retry.
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
                annotation.resumeBehavior() != ResumeBehavior.DEFAULT,
                annotation.startupMode() != StartupMode.DEFAULT);

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Object descriptor = invokeSnapshotFactory(method, bean);
        int everyNEvents = annotation.everyNEvents();
        if (everyNEvents < 1) {
            throw new IllegalArgumentException("@Snapshot '%s' everyNEvents must be at least 1, but was %d.".formatted(id, everyNEvents));
        }
        if (descriptor instanceof DcbSnapshotView<?, ?> rawDcb) {
            processDcbSnapshot(id, annotation, synchronous, converter, (DcbSnapshotView<S, E>) rawDcb, this.<S>resolveReactiveSnapshotStore(annotation, method, id), everyNEvents);
            return;
        }
        if (!(descriptor instanceof SnapshotView<?, ?>)) {
            throw new IllegalArgumentException("@Snapshot '%s' method %s#%s must return a SnapshotView or DcbSnapshotView, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
        }
        SnapshotView<S, E> snapshotView = (SnapshotView<S, E>) descriptor;
        ReactiveSnapshotStore<S> store = resolveReactiveSnapshotStore(annotation, method, id);
        int schemaVersion = snapshotView.schemaVersion();
        View<S, E> view = snapshotView.view();
        Filter eventFilter = snapshotFilterFor(converter, snapshotView);
        org.occurrent.eventstore.api.reactor.EventStore eventStore = applicationContext.getBean(org.occurrent.eventstore.api.reactor.EventStore.class);

        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) -> {
            String key = metadata.getStreamId();
            long eventVersion = metadata.getStreamVersion();
            return store.findLatest(key).map(Optional::of).defaultIfEmpty(Optional.empty()).flatMap(loaded -> {
                // A snapshot version at or beyond this delivery is normally a redelivery, but if the stream was reset
                // below the snapshot the snapshot is stale and resuming from it would freeze the maintainer forever.
                // Only in that ambiguous case do we probe the true head (a suffix read returns the real stream version
                // regardless of skip/limit); the happy path (eventVersion beyond the snapshot) pays no extra read. A
                // head below the snapshot version means a reset, so resolveBase demotes to initial and the range-fold
                // below rebuilds and self-heals (the save overwrites the stale snapshot at the reset version). Caching
                // this probe was tried and reverted: a cached confirmation cannot detect a reset that happens after it
                // was cached, which reintroduces the exact freeze this guard exists to prevent, so every ambiguous
                // delivery is probed fresh.
                Mono<Long> observedHead;
                if (loaded.isPresent() && loaded.get().schemaVersion() == schemaVersion && eventVersion <= loaded.get().version()) {
                    int snapshotVersion = SnapshotSupport.requireInt(loaded.get().version(), "the snapshot version used as the head-probe read offset");
                    observedHead = eventStore.read(key, snapshotVersion, 1).map(org.occurrent.eventstore.api.reactor.EventStream::version);
                } else {
                    observedHead = Mono.just(Long.MAX_VALUE);
                }
                return observedHead.flatMap(head -> {
                    if (SnapshotSupport.isRedelivery(loaded, schemaVersion, eventVersion, head)) {
                        return Mono.<Void>empty(); // already folded (a redelivery within the head), keep folding idempotent
                    }
                    SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(loaded, schemaVersion, head, view::initialState);
                    if (eventVersion - base.version() < everyNEvents) {
                        return Mono.<Void>empty(); // throttle: too few new events since the last saved snapshot
                    }
                    Mono<S> newState;
                    if (eventVersion == base.version() + 1) {
                        newState = Mono.just(view.evolve(base.state(), event));
                    } else {
                        newState = eventStore.read(key, (int) base.version(), (int) (eventVersion - base.version()))
                                .flatMap(es -> es.events().collectList())
                                .map(cloudEvents -> view.evolve(base.state(), converter.toDomainEvents(cloudEvents.stream()).toList()));
                    }
                    return newState.flatMap(state -> store.save(key, new Snapshot<>(state, eventVersion, schemaVersion)));
                });
            });
        };

        boolean stream = annotation.capability() == org.occurrent.annotation.Capability.STREAM;
        if (synchronous) {
            Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentReactiveAnnotationBeanPostProcessor.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
            synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), StartAt.subscriptionModelDefault(), consumer);
            return;
        }
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (replaysHistory && stream && !startPositionSupport.streamHistoryReplaySupported()) {
            throw new IllegalArgumentException("@Snapshot '%s' (capability = STREAM) asks to replay history, but this store does not support reactive stream history replay. Use capability = AGNOSTIC, or startAt = NOW/DEFAULT.".formatted(id));
        }
        if (replaysHistory && !stream && !startPositionSupport.positionReplaySupported()) {
            throw new IllegalArgumentException("@Snapshot '%s' asks to replay history, but this store does not write a global position, so the reactive position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.".formatted(id));
        }
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        boolean waitUntilStarted = shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        startPositionSupport.applyStartupWorkarounds();
        if (stream) {
            StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);
            var result = streamSubscriptions.subscribe(id, filter(eventFilter), startAt, consumer);
            if (waitUntilStarted) {
                result.waitUntilStarted().block();
            }
        } else {
            Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);
            var result = subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), startAt, consumer);
            if (waitUntilStarted) {
                result.waitUntilStarted().block();
            }
        }
    }

    // A DCB @Snapshot maintains one snapshot per boundary, keyed by the canonical criteria key and versioned by the
    // global DCB position, all composed reactively. On each matching event it folds the events after the stored
    // snapshot's position onto the stored state and saves at the current position, so a rebuild after a schema change or
    // a gap re-reads the boundary. everyNEvents throttles by the number of matching events folded since the last save.
    @SuppressWarnings("unchecked")
    private <E, S> void processDcbSnapshot(String id, org.occurrent.annotation.Snapshot annotation, boolean synchronous,
                                           CloudEventConverter<E> converter, DcbSnapshotView<S, E> dcbSnapshotView,
                                           ReactiveSnapshotStore<S> store, int everyNEvents) {
        if (synchronous) {
            throw new IllegalArgumentException("@Snapshot '%s' returns a DcbSnapshotView with mode = SYNCHRONOUS, which is not supported. Use the default asynchronous mode for a DCB snapshot, or maintain a synchronous DCB snapshot through the DSL.".formatted(id));
        }
        org.occurrent.eventstore.api.dcb.DcbCriteria criteria = dcbSnapshotView.criteria();
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        View<S, E> view = dcbSnapshotView.snapshotView().view();
        int schemaVersion = dcbSnapshotView.schemaVersion();
        org.occurrent.eventstore.api.dcb.reactor.DcbEventStore dcbEventStore = applicationContext.getBean(org.occurrent.eventstore.api.dcb.reactor.DcbEventStore.class);

        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        startPositionSupport.applyStartupWorkarounds();
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);
        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, (dcbMetadata, event) -> {
            long position = dcbMetadata.eventMetadata().getPosition();
            return store.findLatest(key).map(Optional::of).defaultIfEmpty(Optional.empty()).flatMap(loaded -> {
                // DCB positions are global and monotonic, they never reset, so a snapshot can never be ahead of the true
                // head: no head probe is needed and the 3-arg isRedelivery is correct (unlike the stream path above).
                if (SnapshotSupport.isRedelivery(loaded, schemaVersion, position)) {
                    return Mono.<Void>empty(); // already folded (a redelivery), keep folding idempotent
                }
                SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(loaded, schemaVersion, view::initialState);
                if (position - base.version() < everyNEvents) {
                    return Mono.<Void>empty(); // throttle before reading, matching events cannot exceed the position gap since the snapshot
                }
                return dcbEventStore.read(criteria, DcbReadOptions.between(base.version(), position)).flatMap(eventStream -> {
                    List<E> range = converter.toDomainEvents(eventStream.events().stream()).toList();
                    if (range.size() < everyNEvents) {
                        return Mono.<Void>empty(); // throttle: too few matching events since the last saved snapshot
                    }
                    S newState = view.evolve(base.state(), range);
                    return store.save(key, new Snapshot<>(newState, position, schemaVersion));
                });
            });
        });
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
            subscription.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <S> ReactiveSnapshotStore<S> resolveReactiveSnapshotStore(org.occurrent.annotation.Snapshot annotation, Method factoryMethod, String id) {
        Class<?> storeType = annotation.store();
        String storeName = annotation.storeName();
        boolean typeSet = storeType != Void.class;
        boolean nameSet = !storeName.isBlank();
        if (typeSet || nameSet) {
            Object bean = typeSet && nameSet ? applicationContext.getBean(storeName, storeType)
                    : typeSet ? applicationContext.getBean(storeType) : applicationContext.getBean(storeName);
            if (!(bean instanceof ReactiveSnapshotStore<?>)) {
                throw new IllegalArgumentException("@Snapshot '%s' store bean must be a ReactiveSnapshotStore, but was %s.".formatted(id, bean.getClass().getName()));
            }
            return (ReactiveSnapshotStore<S>) bean;
        }
        String[] names = applicationContext.getBeanNamesForType(ReactiveSnapshotStore.class);
        if (names.length == 1) {
            return (ReactiveSnapshotStore<S>) applicationContext.getBean(names[0]);
        }
        if (names.length > 1) {
            throw new IllegalStateException("@Snapshot '%s' found %d ReactiveSnapshotStore beans (%s) and cannot pick one. Name one with storeName = \"beanName\".".formatted(id, names.length, String.join(", ", names)));
        }
        ReactiveMongoOperations mongoOperations = applicationContext.getBean(ReactiveMongoOperations.class);
        Class<S> stateType = (Class<S>) reflectSnapshotStateType(factoryMethod, id);
        return new ReactiveSpringMongoSnapshotStore<>(mongoOperations, stateType, "occurrent-snapshot-" + id);
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
                throw new IllegalStateException("@Snapshot factory method %s#%s returned null.".formatted(bean.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke @Snapshot factory method %s#%s".formatted(bean.getClass().getName(), method.getName()), e);
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
        throw new IllegalArgumentException(("@Snapshot '%s' needs a snapshot store: either name one with store or storeName (a ReactiveSnapshotStore bean), " +
                "or declare the factory return type with a concrete state type (for example SnapshotView<MyState, MyEvent>) so the snapshot can default to MongoDB.").formatted(id));
    }
}
