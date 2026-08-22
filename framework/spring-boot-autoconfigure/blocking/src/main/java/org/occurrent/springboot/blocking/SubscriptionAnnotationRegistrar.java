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
import org.springframework.context.ConfigurableApplicationContext;
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
 * handler methods of a single bean. Invoked eagerly, per bean, from the coordinator's
 * {@code postProcessBeforeInitialization}.
 */
class SubscriptionAnnotationRegistrar {

    private final ApplicationContext applicationContext;
    private final StartPositionSupport startPositionSupport;

    SubscriptionAnnotationRegistrar(ApplicationContext applicationContext, StartPositionSupport startPositionSupport) {
        this.applicationContext = applicationContext;
        this.startPositionSupport = startPositionSupport;
    }

    // Resolves the bean to invoke the handler on, and the Method to invoke on it, falling back to the raw bean
    // whenever the proxy isn't something the handler can safely run on. Three such cases, and each one ran fine on
    // the raw bean before this class started resolving the proxy at all, so falling back here never regresses that.
    // A startAt = BEGINNING, startupMode = WAIT_UNTIL_STARTED subscription replays its history synchronously inside
    // this BeanPostProcessor, on the thread that is still creating beanName, and looking that bean up hangs, because
    // the bean factory's lenient singleton locking re-enters bean creation on this delivering thread instead of
    // blocking it. A JDK interface proxy (spring.aop.proxy-target-class=false) may not implement the handler method
    // at all, since method was captured from the concrete pre-proxy class, and invoking it on such a proxy throws.
    // A private or final handler method is never overridden by a CGLIB proxy either, so invoking it there runs
    // against the proxy's own uninitialized fields instead of the real bean's, since Spring builds that proxy
    // without ever running its constructor.
    private HandlerInvocation resolveHandlerInvocation(Object bean, String beanName, Method method) {
        boolean beanStillBeingCreated = ((ConfigurableApplicationContext) applicationContext).getBeanFactory().isCurrentlyInCreation(beanName);
        if (beanStillBeingCreated) {
            return new HandlerInvocation(bean, method);
        }
        Object target = applicationContext.getBean(beanName);
        if (target == bean) {
            return new HandlerInvocation(target, method);
        }
        Method invocableMethod;
        try {
            invocableMethod = AopUtils.selectInvocableMethod(method, target.getClass());
        } catch (IllegalStateException e) {
            return new HandlerInvocation(bean, method);
        }
        if (Modifier.isFinal(invocableMethod.getModifiers())) {
            return new HandlerInvocation(bean, method);
        }
        return new HandlerInvocation(target, invocableMethod);
    }

    private record HandlerInvocation(Object target, Method method) {
    }

    void registerSubscriptions(Object bean, String beanName) {
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
                processSubscribeAnnotation(beanName, bean, method, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processAgnosticSubscribeAnnotation(beanName, bean, method, subscription);
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(beanName, bean, method, dcbSubscription);
            } else if (synchronousSubscription != null) {
                processSynchronousSubscribeAnnotation(beanName, bean, method, synchronousSubscription);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(String beanName, Object bean, Method method, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, subscription.eventTypes(), subscription.annotationName(), applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        // See resolveHandlerInvocation for why this is not just applicationContext.getBean(beanName).
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            HandlerInvocation invocation = resolveHandlerInvocation(bean, beanName, method);
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
    private <E> void processAgnosticSubscribeAnnotation(String beanName, Object bean, Method method, Subscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@Subscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        // See resolveHandlerInvocation for why this is not just applicationContext.getBean(beanName).
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            HandlerInvocation invocation = resolveHandlerInvocation(bean, beanName, method);
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
    private <E> void processSynchronousSubscribeAnnotation(String beanName, Object bean, Method method, SynchronousSubscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@SynchronousSubscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        // See resolveHandlerInvocation for why this is not just applicationContext.getBean(beanName).
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            HandlerInvocation invocation = resolveHandlerInvocation(bean, beanName, method);
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
    private <E> void processDcbSubscribeAnnotation(String beanName, Object bean, Method method, DcbSubscription annotation) {
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

        // See resolveHandlerInvocation for why this is not just applicationContext.getBean(beanName).
        BiConsumer<DcbEventMetadata, E> consumer = (dcbMetadata, event) -> {
            HandlerInvocation invocation = resolveHandlerInvocation(bean, beanName, method);
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
