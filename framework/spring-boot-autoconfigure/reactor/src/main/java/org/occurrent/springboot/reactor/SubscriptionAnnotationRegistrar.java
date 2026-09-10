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

package org.occurrent.springboot.reactor;

import kotlin.jvm.functions.Function2;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.annotation.SynchronousSubscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions;
import org.occurrent.dsl.subscription.reactor.Subscriptions;
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
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.occurrent.springboot.common.SubscriptionAnnotations.shouldWaitUntilStarted;
import static org.occurrent.springboot.common.SubscriptionAnnotations.subscriptionsStartOnTheirOwn;
import static org.occurrent.subscription.StreamSubscriptionFilter.filter;

/**
 * Wires the {@link Subscription}, {@link StreamSubscription}, {@link DcbSubscription} and {@link SynchronousSubscription}
 * handler methods of a single bean. Invoked once per bean from the coordinator's {@code afterSingletonsInstantiated},
 * after every singleton exists, the same point {@code @Projection} is already registered from. The stack-neutral
 * reflection and event-type resolution is shared with the blocking processor through {@link SubscriptionAnnotations}.
 * The reactive-specific start-position logic lives in {@link StartPositionSupport}.
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
    // overridden by a CGLIB proxy either, but that is only a problem once a CGLIB proxy is actually somewhere in the
    // chain: a final method on a bean nothing proxies runs directly, with no advice to lose. Both proxy cases leave
    // no way to invoke the method through the proxy at all, so both are refused rather than silently invoked on the
    // raw bean with no advice applied.
    //
    // Checking bean itself for a CGLIB proxy is not enough: a JDK interface proxy forwards reflectively using the
    // interface's Method, so Java's own virtual dispatch resolves it against whatever bean wraps, a nested CGLIB
    // proxy included, and that inner layer's advice is what silently goes missing. SubscriptionAnnotations.
    // anyProxyLayerIsCglib walks the whole chain instead of only the outer layer, and checks the original method's
    // own modifiers rather than invocableMethod's, since selectInvocableMethod resolves to the interface's method
    // when the JDK-proxy branch is taken, which is never final regardless of what the concrete method is.
    //
    // A static method is refused unconditionally, proxied or not. Method.invoke ignores its target argument for a
    // static method and dispatches on the declaring class alone, so it always runs the same way a direct static call
    // would, with no proxy in the invocation at all for any advice to apply through.
    private HandlerInvocation resolveHandlerInvocation(Object bean, Supplier<Object> handlerTarget, Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new SubscriptionHandlerNotInvocableException(method,
                    "The method is static, so invoking it never goes through the bean's proxy. Make the method an instance method.");
        }
        Method invocableMethod;
        try {
            invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        } catch (IllegalStateException e) {
            throw new SubscriptionHandlerNotInvocableException(method,
                    "The proxy does not implement it. Either the method is private, so a CGLIB proxy cannot override it, or the bean is a JDK interface proxy implementing none of the interfaces the method is declared on. Make the method non-private, expose it on an interface, or set spring.aop.proxy-target-class=true so a CGLIB proxy is used instead.");
        }
        if (Modifier.isFinal(method.getModifiers()) && SubscriptionAnnotations.anyProxyLayerIsCglib(bean)) {
            throw new SubscriptionHandlerNotInvocableException(method,
                    "The method is final, so a CGLIB proxy in the chain cannot override it. Remove final from the method.");
        }
        return new HandlerInvocation(handlerTarget, invocableMethod);
    }

    // target is a supplier rather than the instance, because a bean created after startup registers from
    // postProcessAfterInitialization, where the singleton is not published yet. Asking the context for it by name
    // there throws BeanCurrentlyInCreationException, and holding on to the instance that callback receives would
    // keep whichever proxy layer existed at that moment, losing the advice of any layer a later
    // BeanPostProcessor adds. Resolving by name per delivery, once creation has finished, always reaches the
    // published singleton.
    // The startup path supplies the instance it already resolved, so nothing about it changes.
    private record HandlerInvocation(Supplier<Object> target, Method method) {
    }

    // userClass, not bean.getClass(): bean is already the resolved proxy, and a JDK interface proxy's class
    // implements only interfaces, so scanning it here would miss a method declared on the concrete class.
    //
    // shouldRegister decides per method, so a bean scanned a second time (its real class revealing a handler the
    // first scan's predicted type did not declare) registers only what is new. The validation above every branch
    // still runs for every method, since a misconfigured handler must fail whether or not it registers.
    void registerSubscriptions(Object bean, Class<?> userClass, Supplier<Object> handlerTarget, Predicate<Method> shouldRegister) {
        for (Method method : userClass.getDeclaredMethods()) {
            StreamSubscription streamSubscription = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            DcbSubscription dcbSubscription = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
            SynchronousSubscription synchronousSubscription = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription, synchronousSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription, use only one.".formatted(userClass.getName(), method.getName()));
            }
            if (annotationCount == 1 && !shouldRegister.test(method)) {
                continue;
            }
            if (streamSubscription != null) {
                processSubscribeAnnotation(bean, method, handlerTarget, StreamSubscriptionDefinition.from(streamSubscription));
            } else if (subscription != null) {
                processAgnosticSubscribeAnnotation(bean, method, handlerTarget, subscription);
            } else if (dcbSubscription != null) {
                processDcbSubscribeAnnotation(bean, method, handlerTarget, dcbSubscription);
            } else if (synchronousSubscription != null) {
                processSynchronousSubscribeAnnotation(bean, method, handlerTarget, synchronousSubscription);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, subscription.eventTypes(), subscription.annotationName(), applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        boolean streamHistoryReplaySupported = startPositionSupport.streamHistoryReplaySupported();
        StartAt startAt = startPositionSupport.generateStreamStartAt(subscription, streamHistoryReplaySupported);

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(invocation.method(), invocation.target().get(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));

        boolean shouldWaitUntilStarted = subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(subscription.startAt() == StartPosition.BEGINNING_OF_TIME && streamHistoryReplaySupported, subscription.startupMode());
        StreamSubscriptions<E> streamSubscriptions = applicationContext.getBean(StreamSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        var result = streamSubscriptions.subscribe(id, filter(filter), startAt, consumer);
        if (shouldWaitUntilStarted) {
            result.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processAgnosticSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, Subscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@Subscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(invocation.method(), invocation.target().get(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));

        long startAtGlobalPosition = annotation.startAtGlobalPosition();
        if (startAtGlobalPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        if (replaysHistory && !startPositionSupport.positionReplaySupported()) {
            throw new IllegalArgumentException(("@Subscription '%s' asks to replay history (BEGINNING or startAtGlobalPosition), but this store does not write a global position, so the reactive " +
                    "position-based catch-up cannot replay. Use startAt = NOW or DEFAULT.").formatted(id));
        }
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), startAtGlobalPosition, annotation.resumeBehavior());
        boolean shouldWaitUntilStarted = subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        Subscriptions<E> subscriptions = applicationContext.getBean(Subscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        var result = subscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), startAt, consumer);
        if (shouldWaitUntilStarted) {
            result.waitUntilStarted().block();
        }
    }

    @SuppressWarnings("unchecked")
    private <E> void processSynchronousSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, SynchronousSubscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@SynchronousSubscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Mono<Void>> consumer = (metadata, event) ->
                invokeMono(invocation.method(), invocation.target().get(), SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));

        Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentReactorBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
        // The synchronous subscription model has no start position or background subscription, so there is no
        // start position to resolve and nothing to wait for.
        synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), StartAt.subscriptionModelDefault(), consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, DcbSubscription annotation) {
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

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        BiFunction<DcbEventMetadata, E, Mono<Void>> consumer = (dcbMetadata, event) -> {
            boolean hasDcbEventMetadataParam = parameters.stream().anyMatch(p -> p.type() == DcbEventMetadata.class);
            Object metadataArgument = hasDcbEventMetadataParam ? dcbMetadata : dcbMetadata.eventMetadata();
            return invokeMono(invocation.method(), invocation.target().get(), SubscriptionAnnotations.bindArguments(parameters, event, metadataArgument, dcbMetadata.eventMetadata()));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = subscriptionsStartOnTheirOwn(applicationContext) && shouldWaitUntilStarted(replaysHistory, annotation.startupMode());
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

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
}
