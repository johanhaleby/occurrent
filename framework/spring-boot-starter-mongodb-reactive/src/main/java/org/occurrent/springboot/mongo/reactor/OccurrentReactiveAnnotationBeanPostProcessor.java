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
import org.occurrent.dsl.projection.reactor.ReactiveDcbProjectionRunner;
import org.occurrent.dsl.projection.reactor.ReactiveProjectionRunner;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
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
        if (startAtGlobalPosition >= 0 && annotation.startAt() != Subscription.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == Subscription.StartPosition.BEGINNING;
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
    private StartAt generateAgnosticStartAt(String subscriptionId, Subscription.StartPosition startPosition, long startAtGlobalPosition, ResumeBehavior resumeBehavior) {
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
        if (startAtDcbPosition >= 0 && annotation.startAt() != DcbSubscription.DcbStartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == DcbSubscription.DcbStartPosition.BEGINNING;
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

    private DcbStartAt generateDcbStartAt(String subscriptionId, DcbSubscription.DcbStartPosition startPosition, long startAtDcbPosition, ResumeBehavior resumeBehavior) {
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
            }
        }
        for (Object[] pm : projectionMethods) {
            processProjectionAnnotation(applicationContext.getBean((String) pm[0]), (Method) pm[1], (org.occurrent.annotation.Projection) pm[2]);
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
        boolean synchronous = annotation.mode() == org.occurrent.annotation.Projection.Mode.SYNCHRONOUS;
        if (synchronous && (annotation.startAt() != org.occurrent.annotation.Projection.StartPosition.DEFAULT || annotation.startAtPosition() >= 0 || annotation.resumeBehavior() != ResumeBehavior.DEFAULT)) {
            throw new IllegalArgumentException("@Projection '%s' uses mode = SYNCHRONOUS, which cannot be combined with startAt, startAtPosition, or resumeBehavior (those configure catch-up for an async projection).".formatted(id));
        }

        CloudEventConverter<E> converter = applicationContext.getBean(CloudEventConverter.class);
        Object descriptor = invokeFactory(method, bean);

        if (descriptor instanceof DcbProjection<?, ?, ?> raw) {
            DcbProjection<S, E, ID> dcbProjection = (DcbProjection<S, E, ID>) raw;
            if (synchronous) {
                throw new IllegalArgumentException("@Projection '%s' returns a DcbProjection with mode = SYNCHRONOUS, which the reactive stack does not support in this version. Use mode = ASYNC for a DCB read model, or an agnostic Projection for synchronous read-your-writes.".formatted(id));
            }
            ReactiveDcbProjectionRunner<E> runner = new ReactiveDcbProjectionRunner<>(applicationContext.getBean(SubscriptionModel.class), converter);
            boolean replaysHistory = annotation.startAtPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.Projection.StartPosition.BEGINNING;
            DcbStartAt startAt = generateDcbStartAt(id, toDcbStartPosition(annotation.startAt()), annotation.startAtPosition(), annotation.resumeBehavior());
            applyStartupWorkarounds();
            var subscription = projectDcb(runner, id, dcbProjection, resolveStore(annotation, id), startAt);
            if (shouldWaitUntilStarted(replaysHistory, annotation.startupMode())) {
                subscription.waitUntilStarted().block();
            }
        } else if (descriptor instanceof Projection<?, ?, ?> raw) {
            Projection<S, E, ID> projection = (Projection<S, E, ID>) raw;
            boolean stream = annotation.capability() == org.occurrent.annotation.Projection.Capability.STREAM;
            if (synchronous) {
                // The synchronous subscription model has no lifecycle or start position, so nothing to wait for. It
                // delivers the just-written events on the write path (read-your-writes); the fold ignores unhandled types.
                ReactiveProjectionRunner<E> runner = ReactiveProjectionRunner.agnostic(applicationContext.getBean(SynchronousSubscriptionModel.class), converter);
                projectAgnosticOrStream(runner, id, projection, resolveStore(annotation, id), null);
            } else {
                Subscribable subscribable = applicationContext.getBean(Subscribable.class);
                ReactiveProjectionRunner<E> runner = stream ? ReactiveProjectionRunner.stream(subscribable, converter) : ReactiveProjectionRunner.agnostic(subscribable, converter);
                boolean replaysHistory = annotation.startAtPosition() >= 0 || annotation.startAt() == org.occurrent.annotation.Projection.StartPosition.BEGINNING;
                if (replaysHistory && stream && !streamHistoryReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' (capability = STREAM) asks to replay history, but this store does not support reactive stream history replay. Use capability = AGNOSTIC, startAt = NOW/DEFAULT, or a DcbProjection.".formatted(id));
                }
                if (replaysHistory && !stream && !positionReplaySupported()) {
                    throw new IllegalArgumentException("@Projection '%s' asks to replay history, but this store does not write a global position, so the reactive position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.".formatted(id));
                }
                StartAt startAt = generateAgnosticStartAt(id, toAgnosticStartPosition(annotation.startAt()), annotation.startAtPosition(), annotation.resumeBehavior());
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
            return applicationContext.getBean(storeName);
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

    private static Subscription.StartPosition toAgnosticStartPosition(org.occurrent.annotation.Projection.StartPosition p) {
        return switch (p) {
            case BEGINNING -> Subscription.StartPosition.BEGINNING;
            case NOW -> Subscription.StartPosition.NOW;
            case DEFAULT -> Subscription.StartPosition.DEFAULT;
        };
    }

    private static DcbSubscription.DcbStartPosition toDcbStartPosition(org.occurrent.annotation.Projection.StartPosition p) {
        return switch (p) {
            case BEGINNING -> DcbSubscription.DcbStartPosition.BEGINNING;
            case NOW -> DcbSubscription.DcbStartPosition.NOW;
            case DEFAULT -> DcbSubscription.DcbStartPosition.DEFAULT;
        };
    }
}
