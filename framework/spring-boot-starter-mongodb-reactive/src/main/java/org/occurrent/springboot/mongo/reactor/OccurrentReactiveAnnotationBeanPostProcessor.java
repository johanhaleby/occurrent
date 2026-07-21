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

package org.occurrent.springboot.mongo.reactor;

import kotlin.jvm.functions.Function2;
import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.projection.reactor.ReactiveDcbProjectionRunner;
import org.occurrent.dsl.projection.reactor.DomainEventFeed;
import org.occurrent.dsl.projection.reactor.Projections;
import org.occurrent.dsl.projection.reactor.ReactiveProjectionRunner;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.condition.Condition;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.mongo.common.OccurrentProperties;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations;
import org.occurrent.springboot.mongo.common.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Reactive counterpart of the blocking {@code OccurrentBlockingAnnotationBeanPostProcessor}. It supports the
 * {@link Subscription}, {@link StreamSubscription} and {@link DcbSubscription} annotations for the reactive (Project
 * Reactor) stack. The stack-neutral reflection and event-type resolution is shared with the blocking processor through
 * {@link SubscriptionAnnotations}.
 * <p>
 * The reactive stream (non-DCB) catch-up model replays only by position, so a {@link StreamSubscription} that starts
 * at a specific time ({@code startAtISO8601} or {@code startAtTimeEpochMillis}) fails loud, position replay cannot
 * resolve a wall-clock time to a position. {@code BEGINNING_OF_TIME} replays from position 0 on any STREAM store
 * that writes position, including a combined STREAM and DCB store, and fails loud otherwise. {@code NOW} and
 * {@code DEFAULT} are always supported. DCB subscriptions replay history by position via the reactive DCB catch-up
 * model, matching the blocking behavior. The capability-agnostic {@link Subscription} replays over the unified global
 * position, so {@code BEGINNING} replays from position 0 and {@code startAtGlobalPosition} from a specific position,
 * both delivering events of every capability.
 */
class OccurrentReactiveAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware, SmartInitializingSingleton {

    /**
     * The bean name of the synchronous {@code Subscriptions} DSL declared by the auto-configuration. Resolved by name
     * (rather than by type) so it does not collide with the asynchronous {@code Subscriptions} bean, which is of the
     * same type.
     */
    static final String SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME = "occurrentSynchronousSubscriptions";

    private ApplicationContext applicationContext;

    // Every subscription and projection id must be unique, since it is the durable checkpoint key. Subscription ids are
    // added as their annotations are processed (before singletons finish), projection ids when they register below.
    private final Set<String> registeredIds = new HashSet<>();
    // Domain-push feeds collected during projection registration, caught up once after all are registered.
    private final Set<DomainEventFeed<?>> domainFeedsToCatchUp = Collections.newSetFromMap(new IdentityHashMap<>());

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, @NonNull String beanName) throws BeansException {
        Class<?> managedBeanClass = bean.getClass();
        for (Method method : managedBeanClass.getDeclaredMethods()) {
            StreamSubscription streamSubscription = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            DcbSubscription dcbSubscription = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
            SynchronousSubscription synchronousSubscription = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription, synchronousSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription, use only one.".formatted(bean.getClass().getName(), method.getName()));
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processAgnosticSubscribeAnnotation(bean, method, subscription);
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, dcbSubscription);
            } else if (synchronousSubscription != null) {
                processSynchronousSubscribeAnnotation(beanName, bean, method, synchronousSubscription);
            }
        }
        return bean;
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, subscription.eventTypes(), subscription.annotationName(), applicationContext.getBean(CloudEventConverter.class));
        List<Class<?>> parameterTypes = resolved.parameterTypes();
        Filter filter = resolved.filter();

        boolean streamHistoryReplaySupported = streamHistoryReplaySupported();
        StartAt startAt = generateStreamStartAt(subscription, streamHistoryReplaySupported);

        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadata, SubscriptionAnnotations::isStreamMetadataParameter));

        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(subscription.startAt() == StartPosition.BEGINNING_OF_TIME && streamHistoryReplaySupported, subscription.startupMode());
        StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);

        applyStartupWorkarounds();

        var result = streamSubscriptions.subscribe(id, filter(filter), startAt, consumer);
        if (shouldWaitUntilStarted) {
            result.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processAgnosticSubscribeAnnotation(Object bean, Method method, Subscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@Subscription", applicationContext.getBean(CloudEventConverter.class));
        List<Class<?>> parameterTypes = resolved.parameterTypes();
        Filter filter = resolved.filter();

        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadata, SubscriptionAnnotations::isStreamMetadataParameter));

        long startAtGlobalPosition = annotation.startAtGlobalPosition();
        if (startAtGlobalPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (replaysHistory && !positionReplaySupported()) {
            throw new IllegalArgumentException(("@Subscription '%s' asks to replay history (BEGINNING or startAtGlobalPosition), but this store does not write a global position, so the reactive " +
                    "position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.").formatted(id));
        }
        StartAt startAt = generateAgnosticStartAt(id, annotation.startAt(), startAtGlobalPosition, annotation.resumeBehavior());
        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);

        applyStartupWorkarounds();

        var result = subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), startAt, consumer);
        if (shouldWaitUntilStarted) {
            result.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSynchronousSubscribeAnnotation(String beanName, Object bean, Method method, SynchronousSubscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@SynchronousSubscription", applicationContext.getBean(CloudEventConverter.class));
        List<Class<?>> parameterTypes = resolved.parameterTypes();
        Filter filter = resolved.filter();

        // Resolve the handler from the ApplicationContext lazily, at dispatch time, rather than closing over the raw
        // bean instance captured here. This BeanPostProcessor runs in postProcessBeforeInitialization, before Spring
        // wraps the bean in its AOP proxy, so the instance handed to us is the raw target. Invoking through it would
        // bypass any handler-side @Transactional (or other) advice. Looking the bean up by name yields the proxy,
        // so a handler-side @Transactional is honored when the synchronous handler is invoked.
        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) -> {
            Object target = applicationContext.getBean(beanName);
            return invokeMono(method, target, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadata, SubscriptionAnnotations::isStreamMetadataParameter));
        };

        Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
        // The synchronous subscription model has no lifecycle, start position, or background subscription, so there is
        // no start position to resolve and nothing to wait for.
        synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), StartAt.subscriptionModelDefault(), consumer);
    }

    // A capability-agnostic subscription replays over the unified global position, so replay is supported whenever the
    // store writes a position, regardless of which capabilities are enabled (unlike stream replay, which also requires
    // the STREAM capability).
    private boolean positionReplaySupported() {
        ReactorMongoEventStore eventStore = applicationContext.getBeanProvider(ReactorMongoEventStore.class).getIfAvailable();
        return eventStore != null && eventStore.writesPosition();
    }

    // Build the neutral StartAt over the unified global position. BEGINNING replays from global position 0,
    // startAtGlobalPosition replays after a specific position, both applying the same replay-then-resume logic. NOW and
    // DEFAULT go straight to live.
    private StartAt generateAgnosticStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtGlobalPosition, ResumeBehavior resumeBehavior) {
        if (startAtGlobalPosition >= 0) {
            return replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(startAtGlobalPosition)), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> StartAt.now();
            case DEFAULT -> StartAt.subscriptionModelDefault();
            case BEGINNING -> replayThenResumeAgnostic(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT disables durable position storage by delegating to the parent subscription
    // model, so an in-memory read model rebuilt on every boot sees every event and keeps no checkpoint. There is no
    // reactive competing-consumer model, so only the durable layer is considered. Mirrors the DCB replayThenResume.
    private StartAt replayThenResumeAgnostic(String subscriptionId, StartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> StartAt.dynamic(ctx -> {
                boolean isDurableSubscription = ReactorDurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> StartAt.dynamic(ctx -> {
                CheckpointStorage storage = applicationContext.getBean(CheckpointStorage.class);
                return storage.read(subscriptionId).blockOptional().isPresent() ? StartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, DcbSubscription annotation) {
        String id = annotation.id();
        final DcbCriteria criteria;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = SubscriptionAnnotations.analyzeParameters(method, SubscriptionAnnotations::isDcbMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) SubscriptionAnnotations.eventTypeOf(parameterTypes, SubscriptionAnnotations::isDcbMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = SubscriptionAnnotations.resolveDomainEventTypes(id, bean, method, specifiedEventType, annotation.eventTypes(), "@DcbSubscription");
            List<String> cloudEventTypes = domainEventTypesToSubscribeTo.stream().map(cloudEventConverter::getCloudEventType).toList();
            List<Tag> tags = new ArrayList<>();
            for (String tag : annotation.tags()) {
                try {
                    tags.add(Tag.parse(tag));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("@DcbSubscription(id=\"%s\") has a malformed tag \"%s\": %s".formatted(id, tag, e.getMessage()), e);
                }
            }
            criteria = SubscriptionAnnotations.buildDcbCriteria(cloudEventTypes, tags);
        } else {
            throw new IllegalArgumentException("A @DcbSubscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        BiFunction<DcbEventMetadata, E, Mono<Void>> consumer = (dcbMetadata, event) -> {
            Object metadataArgument = parameterTypes.contains(DcbEventMetadata.class) ? dcbMetadata : dcbMetadata.eventMetadata();
            return invokeMono(method, bean, SubscriptionAnnotations.bindArguments(parameterTypes, event, metadataArgument, SubscriptionAnnotations::isDcbMetadataParameter));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        applyStartupWorkarounds();

        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, consumer);
        if (shouldWaitUntilStarted) {
            subscription.waitUntilStarted().block();
        }
    }

    // Invokes the annotated method for a delivered event. The method is expected to return a Mono<Void> (a null or
    // non-Mono return, for example a void method, is treated as an already-completed action).
    private static Mono<Void> invokeMono(Method method, Object bean, Object[] arguments) {
        try {
            method.setAccessible(true);
            Object result = method.invoke(bean, arguments);
            if (result instanceof Mono<?> mono) {
                return mono.then();
            }
            return Mono.empty();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void applyStartupWorkarounds() {
        // These are workarounds for https://github.com/spring-projects/spring-framework/issues/32904
        applicationContext.getBean(ReactiveMongoOperations.class);
        try {
            applicationContext.getBean("springApplicationAdminRegistrar");
        } catch (NoSuchBeanDefinitionException ignored) {
        }
        // End workarounds
    }

    private static boolean shouldWaitUntilStarted(boolean replaysHistory, StartupMode startupMode) {
        return switch (startupMode) {
            // A subscription that replays history may have a lot to read, so by default it starts in the background.
            case DEFAULT -> !replaysHistory;
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    // A @StreamSubscription can replay history when the store has the STREAM capability and writes stream position,
    // which wires a catch-up model that replays stream filters by position. A combined STREAM+DCB store replays too. A
    // DCB-only store also writes position but has no stream events, so it does not support stream history replay.
    private boolean streamHistoryReplaySupported() {
        ReactorMongoEventStore eventStore = applicationContext.getBeanProvider(ReactorMongoEventStore.class).getIfAvailable();
        if (eventStore == null || !eventStore.writesPosition()) {
            return false;
        }
        OccurrentProperties occurrentProperties = applicationContext.getBean(OccurrentProperties.class);
        return occurrentProperties.getEventStore().getCapabilities().contains(EventStoreCapability.STREAM);
    }

    // A stream subscription's start position. A specific start time (startAtISO8601 or startAtTimeEpochMillis) always
    // fails loud, since position replay cannot resolve a wall-clock time to a position. BEGINNING_OF_TIME replays
    // history when replay is supported (a STREAM store that writes position), and fails loud otherwise rather than
    // silently starting live. NOW and DEFAULT are always supported.
    private StartAt generateStreamStartAt(StreamSubscriptionDefinition subscription, boolean historyReplaySupported) {
        boolean specificTimeStart = !subscription.startAtISO8601().isBlank()
                || subscription.startAtTimeEpochMillis() >= 0;
        if (specificTimeStart) {
            throw new IllegalArgumentException(("@StreamSubscription '%s' specifies a specific start time (startAtISO8601 or startAtTimeEpochMillis), but the reactive stack's position-based " +
                    "stream catch-up cannot honor a specific historical start time, it can only replay from BEGINNING_OF_TIME, NOW, or DEFAULT. Use startAt = BEGINNING_OF_TIME to replay all history, " +
                    "or NOW/DEFAULT, instead of a specific start time.").formatted(subscription.id()));
        }
        boolean beginningOfTimeStart = subscription.startAt() == StartPosition.BEGINNING_OF_TIME;
        if (beginningOfTimeStart && !historyReplaySupported) {
            throw new IllegalArgumentException(("@StreamSubscription '%s' asks to replay history (BEGINNING_OF_TIME), but this store does not support reactive stream history replay " +
                    "(it has no STREAM capability, or stream position is off). Enable stream position (on by default) for a STREAM store, use startAt = NOW or DEFAULT, or use @DcbSubscription for a DCB store.").formatted(subscription.id()));
        }
        if (beginningOfTimeStart) {
            // Map BEGINNING_OF_TIME to position 0, which the reactive stream catch-up model replays before going live.
            return StartAt.checkpoint(GlobalCheckpoint.of(0));
        }
        return switch (subscription.startAt()) {
            case NOW -> StartAt.now();
            // DEFAULT resumes from the durably stored position, falling back to the subscription model default on first start.
            case DEFAULT -> StartAt.subscriptionModelDefault();
            case BEGINNING_OF_TIME -> throw new IllegalStateException("Unreachable: BEGINNING_OF_TIME handled above");
        };
    }

    private DcbStartAt generateDcbStartAt(String subscriptionId, org.occurrent.annotation.StartPosition startPosition, long startAtDcbPosition, ResumeBehavior resumeBehavior) {
        if (startAtDcbPosition >= 0) {
            // Start after a specific position, applying the same replay-then-resume logic BEGINNING uses.
            return replayThenResume(subscriptionId, DcbStartAt.afterPosition(startAtDcbPosition), resumeBehavior);
        }
        return switch (startPosition) {
            case NOW -> DcbStartAt.now();
            case DEFAULT -> DcbStartAt.subscriptionModelDefault();
            case BEGINNING -> replayThenResume(subscriptionId, DcbStartAt.beginning(), resumeBehavior);
        };
    }

    // Replay from replayStart, then on later restarts either resume from the stored position (DEFAULT) or replay again
    // (SAME_AS_START_AT). SAME_AS_START_AT disables durable position storage by delegating to the parent subscription
    // model, so an in-memory read model rebuilt on every boot sees every event and keeps no checkpoint. There is no
    // reactive competing-consumer model, so only the durable layer is considered.
    private DcbStartAt replayThenResume(String subscriptionId, DcbStartAt replayStart, ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                boolean isDurableSubscription = ReactorDurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> DcbStartAt.dynamic(ctx -> {
                CheckpointStorage storage = applicationContext.getBean(CheckpointStorage.class);
                return storage.read(subscriptionId).blockOptional().isPresent() ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    // @Projection factory methods are registered after all singletons are instantiated, not in
    // postProcessBeforeInitialization: the factory has to be invoked to obtain the descriptor, and its collaborators
    // (the store, the subscription model) must already be wired. First collect every subscription id so a projection
    // cannot reuse one, then register each projection.
    @Override
    public void afterSingletonsInstantiated() {
        if (applicationContext.getBeanProvider(Subscribable.class).getIfAvailable() == null
                && applicationContext.getBeanProvider(SynchronousSubscriptionModel.class).getIfAvailable() == null) {
            return;
        }
        List<Object[]> projectionMethods = new ArrayList<>();
        List<Object[]> snapshotMethods = new ArrayList<>();
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> type;
            try {
                type = applicationContext.getType(beanName);
            } catch (RuntimeException e) {
                continue;
            }
            if (type == null) {
                continue;
            }
            for (Method method : ClassUtils.getUserClass(type).getDeclaredMethods()) {
                collectSubscriptionId(method);
                org.occurrent.annotation.Projection projection = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Projection.class);
                if (projection != null) {
                    projectionMethods.add(new Object[]{beanName, method, projection});
                }
                org.occurrent.annotation.Snapshot snapshot = AnnotationUtils.findAnnotation(method, org.occurrent.annotation.Snapshot.class);
                if (snapshot != null) {
                    snapshotMethods.add(new Object[]{beanName, method, snapshot});
                }
            }
        }
        for (Object[] pm : projectionMethods) {
            processProjectionAnnotation(applicationContext.getBean((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
        }
        // Catch up each domain-push feed once, after all its projections are registered.
        for (DomainEventFeed<?> feed : domainFeedsToCatchUp) {
            feed.catchUpAll().block();
        }
        for (Object[] sm : snapshotMethods) {
            processSnapshotAnnotation(applicationContext.getBean((String) sm[0]), (Method) sm[1], (org.occurrent.annotation.Snapshot) sm[2]);
        }
    }

    private void collectSubscriptionId(Method method) {
        StreamSubscription s = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
        if (s != null) registeredIds.add(s.id());
        Subscription a = AnnotationUtils.findAnnotation(method, Subscription.class);
        if (a != null) registeredIds.add(a.id());
        DcbSubscription d = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
        if (d != null) registeredIds.add(d.id());
        SynchronousSubscription sy = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
        if (sy != null) registeredIds.add(sy.id());
    }

    @SuppressWarnings("unchecked")
    private <E, S, ID> void processProjectionAnnotation(Object bean, Method method, org.occurrent.annotation.Projection annotation) {
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
            DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
            applyStartupWorkarounds();
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
                if (replaysHistory && stream && !streamHistoryReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' (capability = STREAM) asks to replay history, but this store does not support reactive stream history replay. Use capability = AGNOSTIC, startAt = NOW/DEFAULT, or a DcbProjection.".formatted(id));
                }
                if (replaysHistory && !stream && !positionReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' asks to replay history, but this store does not write a global position, so the reactive position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.".formatted(id));
                }
                StartAt startAt = generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
                applyStartupWorkarounds();
                var subscription = projectAgnosticOrStream(runner, id, projection, resolveStore(annotation, id), startAt);
                if (shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                    subscription.waitUntilStarted().block();
                }
            }
        } else {
            throw new IllegalArgumentException("@Projection '%s' method %s#%s must return a Projection or DcbProjection, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
        }
    }

    // Register a source=PUSH projection whose feed bean is a PushSubscriptionModel (CloudEvents), wrapped in a
    // replay-then-push catch-up so a new or rebuilt projection is backfilled from the event store.
    @SuppressWarnings("unchecked")
    private <E, S, ID> void registerPushProjection(String id, CloudEventConverter<E> converter, Object descriptor, boolean synchronous, org.occurrent.annotation.Projection annotation, PushSubscriptionModel pushModel) {
        Projection<S, E, ID> projection = validatePushDescriptor(annotation, id, descriptor, synchronous);
        PositionOrderedReader reader = applicationContext.getBean(PositionOrderedReader.class);
        CheckpointStorage catchupMarker = applicationContext.getBean(CheckpointStorage.class);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, pushModel, catchupMarker);
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

    // A @Snapshot maintains a per-stream, resume-ready snapshot: for each handled event it folds the event onto the
    // stored snapshot for that stream and saves the new state at the event's stream version, all composed reactively. A
    // schema-version change or a gap rebuilds by folding the range up to this event from the store. The store save is
    // best-effort at the reactive DSL level, but here a maintained failure surfaces to the durable subscription for retry.
    @SuppressWarnings("unchecked")
    private <E, S> void processSnapshotAnnotation(Object bean, Method method, org.occurrent.annotation.Snapshot annotation) {
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
            throw new IllegalArgumentException("@Snapshot '%s' method %s#%s must return a SnapshotView, but returned %s.".formatted(id, bean.getClass().getName(), method.getName(), descriptor == null ? "null" : descriptor.getClass().getName()));
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
            Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
            synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(eventFilter), StartAt.subscriptionModelDefault(), consumer);
            return;
        }
        boolean replaysHistory = annotation.startAtGlobalPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (replaysHistory && stream && !streamHistoryReplaySupported()) {
            throw new IllegalArgumentException("@Snapshot '%s' (capability = STREAM) asks to replay history, but this store does not support reactive stream history replay. Use capability = AGNOSTIC, or startAt = NOW/DEFAULT.".formatted(id));
        }
        if (replaysHistory && !stream && !positionReplaySupported()) {
            throw new IllegalArgumentException("@Snapshot '%s' asks to replay history, but this store does not write a global position, so the reactive position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.".formatted(id));
        }
        StartAt startAt = generateAgnosticStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        boolean waitUntilStarted = shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        applyStartupWorkarounds();
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
        DcbCriteria criteria = dcbSnapshotView.criteria();
        String key = DcbSnapshotKeys.canonicalKey(criteria);
        View<S, E> view = dcbSnapshotView.snapshotView().view();
        int schemaVersion = dcbSnapshotView.schemaVersion();
        org.occurrent.eventstore.api.dcb.reactor.DcbEventStore dcbEventStore = applicationContext.getBean(org.occurrent.eventstore.api.dcb.reactor.DcbEventStore.class);

        DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), annotation.startAtGlobalPosition(), annotation.resumeBehavior());
        applyStartupWorkarounds();
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

}
