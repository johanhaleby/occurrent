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
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.StreamSubscription.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.occurrent.filter.Filter.CompositionOperator.OR;
import static org.occurrent.subscription.OccurrentSubscriptionFilter.filter;

/**
 * Reactive counterpart of the blocking {@code OccurrentAnnotationBeanPostProcessor}. It supports the
 * {@link StreamSubscription} and {@link DcbSubscription} annotations, and the deprecated {@link Subscription} alias,
 * for the reactive (Project Reactor) stack.
 * <p>
 * The reactive stack has no stream (non-DCB) catch-up model, so a {@link StreamSubscription} that asks to replay
 * history (a start time, or {@code BEGINNING_OF_TIME}) fails loud. Only {@code NOW} and {@code DEFAULT} are supported
 * for stream subscriptions, giving live delivery plus durable resume across restarts. DCB subscriptions can replay
 * history by dcbposition via the reactive DCB catch-up model, matching the blocking behavior.
 */
class OccurrentReactiveAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

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
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @StreamSubscription, @DcbSubscription and the deprecated @Subscription, use only one.".formatted(bean.getClass().getName(), method.getName()));
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(subscription));
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, dcbSubscription);
            }
        }
        return bean;
    }

    /**
     * The normalized form of a stream subscription declaration, built from either the {@link StreamSubscription}
     * annotation or the deprecated {@link Subscription} alias. The deprecated annotation's enums are mapped to the
     * canonical {@link StreamSubscription} enums by name, since the constants are identical.
     */
    private record StreamSubscriptionDefinition(String id, Class<?>[] eventTypes, String startAtISO8601,
                                                long startAtTimeEpochMillis, StartPosition startAt,
                                                ResumeBehavior resumeBehavior, StartupMode startupMode, String annotationName) {

        static StreamSubscriptionDefinition from(StreamSubscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), subscription.startAt(), subscription.resumeBehavior(), subscription.startupMode(), "@StreamSubscription");
        }

        @SuppressWarnings("deprecation")
        static StreamSubscriptionDefinition from(Subscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), StartPosition.valueOf(subscription.startAt().name()),
                    ResumeBehavior.valueOf(subscription.resumeBehavior().name()), StartupMode.valueOf(subscription.startupMode().name()), "@Subscription");
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        final Filter filter;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = analyzeParameters(method, OccurrentReactiveAnnotationBeanPostProcessor::isStreamMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameterTypes, OccurrentReactiveAnnotationBeanPostProcessor::isStreamMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = resolveDomainEventTypes(id, bean, method, specifiedEventType, subscription.eventTypes(), subscription.annotationName());

            if (domainEventTypesToSubscribeTo.size() == 1) {
                filter = Filter.type(cloudEventConverter.getCloudEventType(domainEventTypesToSubscribeTo.get(0)));
            } else {
                List<Filter> typeFilters = domainEventTypesToSubscribeTo.stream()
                        .map(cloudEventConverter::getCloudEventType)
                        .map(Filter::type)
                        .toList();
                filter = new Filter.CompositionFilter(OR, typeFilters);
            }
        } else {
            throw new IllegalArgumentException("A subscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        StartAt startAt = generateStreamStartAt(subscription);

        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(method, bean, bindArguments(parameterTypes, event, metadata, OccurrentReactiveAnnotationBeanPostProcessor::isStreamMetadataParameter));

        boolean shouldWaitUntilStarted = shouldWaitUntilStarted(subscription.startupMode());
        StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);

        applyStartupWorkarounds();

        var result = streamSubscriptions.subscribe(id, filter(filter), startAt, consumer);
        if (shouldWaitUntilStarted) {
            result.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, DcbSubscription annotation) {
        String id = annotation.id();
        final DcbQuery query;
        final List<Class<?>> parameterTypes;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameterTypes = analyzeParameters(method, OccurrentReactiveAnnotationBeanPostProcessor::isDcbMetadataParameter);
            Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameterTypes, OccurrentReactiveAnnotationBeanPostProcessor::isDcbMetadataParameter);
            List<Class<E>> domainEventTypesToSubscribeTo = resolveDomainEventTypes(id, bean, method, specifiedEventType, annotation.eventTypes(), "@DcbSubscription");
            List<String> cloudEventTypes = domainEventTypesToSubscribeTo.stream().map(cloudEventConverter::getCloudEventType).toList();
            query = buildDcbQuery(cloudEventTypes, List.of(annotation.tagsAllOf()));
        } else {
            throw new IllegalArgumentException("A @DcbSubscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }

        BiFunction<DcbEventMetadata, E, Mono<Void>> consumer = (dcbMetadata, event) -> {
            Object metadataArgument = parameterTypes.contains(DcbEventMetadata.class) ? dcbMetadata : dcbMetadata.eventMetadata();
            return invokeMono(method, bean, bindArguments(parameterTypes, event, metadataArgument, OccurrentReactiveAnnotationBeanPostProcessor::isDcbMetadataParameter));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != DcbSubscription.DcbStartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == DcbSubscription.DcbStartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = shouldWaitUntilStartedDcb(replaysHistory, annotation.startupMode());
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        applyStartupWorkarounds();

        var subscription = dcbSubscriptions.subscribeWithMetadata(id, query, startAt, consumer);
        if (shouldWaitUntilStarted) {
            subscription.waitUntilStarted().block();
        }
    }

    private static boolean isStreamMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType);
    }

    private static boolean isDcbMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType) || DcbEventMetadata.class.isAssignableFrom(parameterType);
    }

    private static List<Class<?>> analyzeParameters(Method method, Predicate<Class<?>> isMetadataParameter) {
        List<Class<?>> parameterTypes = new ArrayList<>();
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (isMetadataParameter.test(parameterType)) {
                if (parameterTypes.stream().anyMatch(isMetadataParameter)) {
                    throw new IllegalArgumentException("A subscription method may declare at most one metadata parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                parameterTypes.add(parameterType);
            } else {
                if (parameterTypes.isEmpty()) {
                    parameterTypes.add(parameterType);
                } else if (parameterTypes.size() == 2) {
                    throw new IllegalArgumentException("A subscription method may declare an event parameter and at most one metadata parameter, but %s#%s declares more.".formatted(method.getDeclaringClass().getName(), method.getName()));
                } else if (parameterTypes.stream().anyMatch(isMetadataParameter)) {
                    parameterTypes.add(parameterType);
                } else {
                    throw new IllegalArgumentException("A subscription method may declare only one event parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
            }
        }
        return parameterTypes;
    }

    private static Class<?> eventTypeOf(List<Class<?>> parameterTypes, Predicate<Class<?>> isMetadataParameter) {
        return parameterTypes.stream().filter(not(isMetadataParameter)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("You need to declare an event type"));
    }

    @SuppressWarnings("unchecked")
    private static <E> List<Class<E>> resolveDomainEventTypes(String id, Object bean, Method method, Class<E> specifiedEventType, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName) {
        if (eventTypesSpecifiedInAnnotation.length == 0) {
            return getConcreteEventTypes(id, specifiedEventType);
        }
        return Arrays.stream(eventTypesSpecifiedInAnnotation)
                .flatMap(e -> getConcreteEventTypes(id, (Class<E>) e).stream())
                .peek(e -> {
                    if (!specifiedEventType.isAssignableFrom(e)) {
                        throw new IllegalStateException("Event type %s specified in the %s annotation with id %s is not assignable from the event type specified in %s#%s(..).".formatted(e.getName(), annotationName, id, bean.getClass().getName(), method.getName()));
                    }
                })
                .toList();
    }

    private static DcbQuery buildDcbQuery(List<String> cloudEventTypes, List<String> tagsAllOf) {
        boolean hasTypes = !cloudEventTypes.isEmpty();
        boolean hasTags = !tagsAllOf.isEmpty();
        if (!hasTypes && !hasTags) {
            return DcbQuery.all();
        } else if (hasTypes && hasTags) {
            return DcbQuery.types(cloudEventTypes.get(0), cloudEventTypes.stream().skip(1).toArray(String[]::new)).tags(tagsAllOf);
        } else if (hasTypes) {
            return DcbQuery.types(cloudEventTypes.get(0), cloudEventTypes.stream().skip(1).toArray(String[]::new));
        } else {
            return DcbQuery.tags(tagsAllOf.get(0), tagsAllOf.stream().skip(1).toArray(String[]::new));
        }
    }

    private static Object[] bindArguments(List<Class<?>> parameterTypes, Object event, Object metadata, Predicate<Class<?>> isMetadataParameter) {
        if (parameterTypes.size() == 1) {
            return new Object[]{event};
        }
        // Place each argument by which declared parameter slot is the metadata type, not by runtime assignability. A
        // broad event parameter (for example Object) is assignable from the metadata value too, so an isInstance check
        // would misplace it, this keys off the declared types instead and honors a metadata-first parameter order.
        Object first = isMetadataParameter.test(parameterTypes.get(0)) ? metadata : event;
        Object second = first == metadata ? event : metadata;
        return new Object[]{first, second};
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

    private static boolean shouldWaitUntilStarted(StartupMode startupMode) {
        return switch (startupMode) {
            // Stream subscriptions never replay history on the reactive stack, so the default is to wait until started.
            case DEFAULT, WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    private static boolean shouldWaitUntilStartedDcb(boolean replaysHistory, DcbSubscription.StartupMode startupMode) {
        return switch (startupMode) {
            // A subscription that replays history may have a lot to read, so by default it starts in the background.
            case DEFAULT -> !replaysHistory;
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    // Stream subscriptions on the reactive stack support only NOW and DEFAULT, since there is no reactive stream
    // catch-up model to replay history. Any time-based start (an ISO8601 start, an epoch start, or BEGINNING_OF_TIME)
    // fails loud rather than silently behaving like a live start.
    private StartAt generateStreamStartAt(StreamSubscriptionDefinition subscription) {
        boolean timeBasedStart = !subscription.startAtISO8601().isBlank()
                || subscription.startAtTimeEpochMillis() >= 0
                || subscription.startAt() == StartPosition.BEGINNING_OF_TIME;
        if (timeBasedStart) {
            throw new IllegalArgumentException(("@StreamSubscription '%s' asks to replay history (a start time or BEGINNING_OF_TIME), but the reactive stack has no stream catch-up model, " +
                    "so history replay is not supported for stream subscriptions. Use startAt = NOW or DEFAULT, or use @DcbSubscription for dcbposition replay.").formatted(subscription.id()));
        }
        return switch (subscription.startAt()) {
            case NOW -> StartAt.now();
            // DEFAULT resumes from the durably stored position, falling back to the subscription model default on first start.
            case DEFAULT -> StartAt.subscriptionModelDefault();
            case BEGINNING_OF_TIME -> throw new IllegalStateException("Unreachable: BEGINNING_OF_TIME handled above");
        };
    }

    private DcbStartAt generateDcbStartAt(String subscriptionId, DcbSubscription.DcbStartPosition startPosition, long startAtDcbPosition, DcbSubscription.ResumeBehavior resumeBehavior) {
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
    private DcbStartAt replayThenResume(String subscriptionId, DcbStartAt replayStart, DcbSubscription.ResumeBehavior resumeBehavior) {
        return switch (resumeBehavior) {
            case SAME_AS_START_AT -> DcbStartAt.dynamic(ctx -> {
                boolean isDurableSubscription = ReactorDurableSubscriptionModel.class.isAssignableFrom(ctx.subscriptionModelType());
                return isDurableSubscription ? null : replayStart;
            });
            case DEFAULT -> DcbStartAt.dynamic(ctx -> {
                SubscriptionPositionStorage storage = applicationContext.getBean(SubscriptionPositionStorage.class);
                return storage.read(subscriptionId).blockOptional().isPresent() ? DcbStartAt.subscriptionModelDefault() : replayStart;
            });
        };
    }

    private static <E> @NonNull List<Class<E>> getConcreteEventTypes(String subscriptionId, Class<E> specifiedEventType) {
        final List<Class<E>> domainEventTypesToSubscribeTo;
        if (specifiedEventType.isSealed()) {
            //noinspection unchecked
            Class<E>[] permittedSubclasses = (Class<E>[]) specifiedEventType.getPermittedSubclasses();
            domainEventTypesToSubscribeTo = Arrays.stream(permittedSubclasses).flatMap(c -> getConcreteEventTypes(subscriptionId, c).stream()).toList();
        } else if (specifiedEventType.isInterface() || specifiedEventType.isArray() || Modifier.isAbstract(specifiedEventType.getModifiers())) {
            String msg = "You cannot subscribe to a non-sealed interface, abstract type, or array (problem is with %s for subscription '%s'). A concrete or sealed event type is required, or list the event types explicitly with the annotation's eventTypes attribute (for example eventTypes = {MyEvent1.class, MyEvent2.class}).";
            throw new IllegalArgumentException(msg.formatted(specifiedEventType.getName(), subscriptionId));
        } else {
            domainEventTypesToSubscribeTo = List.of(specifiedEventType);
        }
        return domainEventTypesToSubscribeTo;
    }
}
