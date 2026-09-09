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
import org.occurrent.annotation.*;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions;
import org.occurrent.dsl.subscription.blocking.Subscriptions;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.springboot.common.SubscriptionAnnotations;
import org.occurrent.springboot.common.SubscriptionAnnotations.StreamSubscriptionDefinition;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.StartAt;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Wires the {@link Subscription}, {@link StreamSubscription}, {@link DcbSubscription} and {@link SynchronousSubscription}
 * handler methods of a single bean. Invoked once per bean from the coordinator's {@code afterSingletonsInstantiated},
 * after every singleton exists, the same point {@code @Projection} is already registered from.
 */
class SubscriptionAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;

    SubscriptionAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
    }

    // Resolves the bean to invoke the handler on, and the Method to invoke on it, once, when the bean has already
    // left creation, so the result can be cached for the life of the subscription instead of re-resolved on every
    // delivery. Called only from afterSingletonsInstantiated, so bean is always the fully proxied singleton already,
    // never the raw target: unlike the @Projection registration this mirrors, this class used to run early from
    // postProcessBeforeInitialization and fall back to the still-being-created raw bean, and that fallback is what
    // moving here removes, not narrows.
    //
    // A JDK interface proxy (spring.aop.proxy-target-class=false) may not implement the handler method at all, when
    // the method was declared on the concrete class rather than an interface. A final handler method is never
    // overridden by a CGLIB proxy either, but that is only a problem once bean actually is a CGLIB proxy: a final
    // method on a bean nothing proxies runs directly, with no advice to lose. Both proxy cases leave no way to
    // invoke the method through the proxy at all, so both are refused rather than silently invoked on the raw bean
    // with no advice applied.
    private HandlerInvocation resolveHandlerInvocation(Object bean, Method method) {
        Method invocableMethod;
        try {
            invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        } catch (IllegalStateException e) {
            throw new SubscriptionHandlerNotInvocableException(method,
                    "The proxy does not implement it. Either the method is private, so a CGLIB proxy cannot override it, or the bean is a JDK interface proxy implementing none of the interfaces the method is declared on. Make the method non-private, expose it on an interface, or set spring.aop.proxy-target-class=true so a CGLIB proxy is used instead.");
        }
        if (AopUtils.isCglibProxy(bean) && Modifier.isFinal(invocableMethod.getModifiers())) {
            throw new SubscriptionHandlerNotInvocableException(method,
                    "The method is final, so the CGLIB proxy cannot override it. Remove final from the method.");
        }
        return new HandlerInvocation(bean, invocableMethod);
    }

    private record HandlerInvocation(Object target, Method method) {
    }

    // userClass, not bean.getClass(): bean is already the resolved proxy, and a JDK interface proxy's class
    // implements only interfaces, so scanning it here would miss a method declared on the concrete class.
    void registerSubscriptions(Object bean, Class<?> userClass) {
        for (Method method : userClass.getDeclaredMethods()) {
            StreamSubscription streamSubscription = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            DcbSubscription dcbSubscription = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
            SynchronousSubscription synchronousSubscription = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription, synchronousSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription, use only one.".formatted(userClass.getName(), method.getName()));
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processAgnosticSubscribeAnnotation(bean, method, subscription);
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, dcbSubscription);
            } else if (synchronousSubscription != null) {
                processSynchronousSubscribeAnnotation(bean, method, synchronousSubscription);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, subscription.eventTypes(), subscription.annotationName(), applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation.method(), invocation.target(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        var startPositionToUse = StartPositionSupport.findStartPositionToUseOrThrow(subscription.id(), subscription.startAtISO8601(), subscription.startAtTimeEpochMillis(), subscription.startAt());
        ResumeBehavior resumeBehavior = subscription.resumeBehavior();
        StartAt startAt = startPositionSupport.generateStartAt(subscription.id(), startPositionToUse, resumeBehavior);

        boolean shouldWaitUntilStarted = StartPositionSupport.shouldWaitUntilStarted(startPositionToUse, subscription.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        StreamSubscriptions<E> subscribable = applicationContext.getBean(StreamSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        subscribable.subscribe(id, filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processAgnosticSubscribeAnnotation(Object bean, Method method, Subscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@Subscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation.method(), invocation.target(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        long startAtGlobalPosition = annotation.startAtGlobalPosition();
        if (startAtGlobalPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), startAtGlobalPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        Subscriptions<E> subscribable = applicationContext.getBean(Subscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        subscribable.subscribe(id, AgnosticSubscriptionFilter.filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processSynchronousSubscribeAnnotation(Object bean, Method method, SynchronousSubscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@SynchronousSubscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation.method(), invocation.target(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
        // The synchronous subscription model has no start position or background thread, so there is no start
        // position to resolve and nothing to wait for. Pass the default StartAt (the model ignores it) rather than
        // null to honor the Subscribable contract.
        synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), StartAt.subscriptionModelDefault(), false, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, DcbSubscription annotation) {
        String id = annotation.id();
        final DcbCriteria criteria;
        final List<SubscriptionAnnotations.HandlerParameter> parameters;
        if (method.getParameterCount() >= 1) {
            CloudEventConverter<E> cloudEventConverter = applicationContext.getBean(CloudEventConverter.class);
            parameters = SubscriptionAnnotations.analyzeParameters(method, SubscriptionAnnotations::isDcbMetadataParameter, false);
            Class<E> specifiedEventType = (Class<E>) SubscriptionAnnotations.eventTypeOf(parameters);
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

        HandlerInvocation invocation = resolveHandlerInvocation(bean, method);
        BiConsumer<DcbEventMetadata, E> consumer = (dcbMetadata, event) -> {
            boolean hasDcbEventMetadataParam = parameters.stream().anyMatch(p -> p.type() == DcbEventMetadata.class);
            Object metadataArgument = hasDcbEventMetadataParam ? dcbMetadata : dcbMetadata.eventMetadata();
            invoke(invocation.method(), invocation.target(), SubscriptionAnnotations.bindArguments(parameters, event, metadataArgument, dcbMetadata.eventMetadata()));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, consumer);
        if (shouldWaitUntilStarted) {
            subscription.waitUntilStarted();
        }
    }

    private static void invoke(Method method, Object bean, Object[] arguments) {
        try {
            method.setAccessible(true);
            method.invoke(bean, arguments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
