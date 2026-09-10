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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
        return new HandlerInvocation(handlerTarget, method, invocableMethod, bean.getClass());
    }

    // Both the object to invoke on and the method to invoke depend on when the delivery happens. A handler
    // registered after startup runs on the instance its creation callback received while its bean is still being
    // created, and on the published singleton afterwards, and a BeanPostProcessor after that callback can make
    // those two different classes. A concrete method resolved against one is not invocable on the other, so the
    // method is resolved from the object actually being invoked and the last answer is kept. A handler that always
    // runs on the same class therefore resolves once, which is every handler registered at startup.
    private static final class HandlerInvocation {
        private final Supplier<Object> target;
        private final Method declaredMethod;
        // The class and the method resolved for it are one value, so a delivery can never read the class from one
        // resolution and the method from the next. Two separate fields allow exactly that, and the method that
        // comes back then belongs to a class the target is not, which fails the reflective call.
        private volatile Resolution resolution;

        HandlerInvocation(Supplier<Object> target, Method declaredMethod, Method resolved, Class<?> resolvedFor) {
            this.target = target;
            this.declaredMethod = declaredMethod;
            this.resolution = new Resolution(resolvedFor, resolved);
        }

        Object target() {
            return target.get();
        }

        Method methodFor(Object target) {
            Class<?> targetClass = target.getClass();
            Resolution current = resolution;
            if (targetClass == current.forClass()) {
                return current.method();
            }
            Method method;
            try {
                method = AopUtils.selectInvocableMethod(declaredMethod, targetClass);
            } catch (IllegalStateException e) {
                throw new SubscriptionHandlerNotInvocableException(declaredMethod,
                        "The proxy does not implement it. Either the method is private, so a CGLIB proxy cannot override it, or the bean is a JDK interface proxy implementing none of the interfaces the method is declared on. Make the method non-private, expose it on an interface, or set spring.aop.proxy-target-class=true so a CGLIB proxy is used instead.");
            }
            // The same guard registration ran, against the object actually in hand. A CGLIB proxy added after
            // registration cannot override a final method, so selecting one succeeds while invoking it reaches the
            // inherited method directly and every layer's advice is skipped. Refusing here is the only place left
            // to catch that, since the proxy did not exist when the guards first ran.
            if (Modifier.isFinal(declaredMethod.getModifiers()) && SubscriptionAnnotations.anyProxyLayerIsCglib(target)) {
                throw new SubscriptionHandlerNotInvocableException(declaredMethod,
                        "The method is final, so a CGLIB proxy in the chain cannot override it. Remove final from the method.");
            }
            resolution = new Resolution(targetClass, method);
            return method;
        }

        private record Resolution(Class<?> forClass, Method method) {
        }
    }

    // userClass, not bean.getClass(): bean is already the resolved proxy, and a JDK interface proxy's class
    // implements only interfaces, so scanning it here would miss a method declared on the concrete class.
    //
    // shouldRegister decides per method, so a bean scanned a second time (its real class revealing a handler the
    // first scan's predicted type did not declare) registers only what is new. The validation above every branch
    // still runs for every method, since a misconfigured handler must fail whether or not it registers.
    // mayBlockForReplay is false for a bean the container is still building. Waiting there would run the whole
    // history replay inside that bean's creation callback, delivering to a handler on an object the context has not
    // published yet, so advice a later BeanPostProcessor adds is not on it. Not waiting lets creation finish
    // alongside the replay rather than behind it, and every delivery after publication resolves the published bean.
    // A replay on its own thread can still deliver before creation finishes, which is the race the coordinator and
    // ADR 127 both describe. It also matches what WAIT_UNTIL_STARTED means, which is finishing before the
    // application is up, and the application is already up by the time a lazily built bean is asked for.
    void registerSubscriptions(Object bean, Class<?> userClass, Supplier<Object> handlerTarget, boolean mayBlockForReplay,
                               Predicate<Method> reserveHandler, Consumer<String> claimId,
                               Consumer<Method> releaseHandler, Consumer<String> releaseId) {
        // Tracked apart, because a call can hold a handler reservation without holding the id. claimId throws when
        // the id belongs to another registration, and at that moment this call has reserved the handler and
        // acquired nothing else, so releasing the id here would hand away what that other registration owns and
        // let a third one claim the same durable checkpoint key.
        List<PendingRegistration> reservedHandlers = new ArrayList<>();
        List<PendingRegistration> claimedIds = new ArrayList<>();
        List<PendingRegistration> pending = new ArrayList<>();
        try {
            claimAndValidate(bean, userClass, handlerTarget, reserveHandler, claimId, pending, reservedHandlers, claimedIds);
        } catch (RuntimeException | Error e) {
            claimedIds.forEach(h -> releaseId.accept(h.id()));
            reservedHandlers.forEach(h -> releaseHandler.accept(h.method()));
            throw e;
        }
        for (PendingRegistration handler : pending) {
            try {
                if (handler.streamSubscription() != null) {
                    processSubscribeAnnotation(bean, handler.method(), handlerTarget, mayBlockForReplay, StreamSubscriptionDefinition.from(handler.streamSubscription()));
                } else if (handler.subscription() != null) {
                    processAgnosticSubscribeAnnotation(bean, handler.method(), handlerTarget, mayBlockForReplay, handler.subscription());
                } else if (handler.dcbSubscription() != null) {
                    processDcbSubscribeAnnotation(bean, handler.method(), handlerTarget, mayBlockForReplay, handler.dcbSubscription());
                } else {
                    processSynchronousSubscribeAnnotation(bean, handler.method(), handlerTarget, mayBlockForReplay, handler.synchronousSubscription());
                }
            } catch (RuntimeException | Error e) {
                claimedIds.forEach(h -> releaseId.accept(h.id()));
                reservedHandlers.forEach(h -> releaseHandler.accept(h.method()));
                throw e;
            }
            claimedIds.remove(handler);
            reservedHandlers.remove(handler);
        }
    }

    // Every handler on the bean is claimed and checked before any of them subscribes, so a second handler that
    // cannot be registered means the first one never subscribes, rather than being live against a bean whose
    // creation is about to fail. What stays outside this is a failure from subscribe itself, a store refusing for
    // example, since undoing that one needs the subscription cancelled rather than never started.
    private void claimAndValidate(Object bean, Class<?> userClass, Supplier<Object> handlerTarget, Predicate<Method> reserveHandler,
                                  Consumer<String> claimId, List<PendingRegistration> pending,
                                  List<PendingRegistration> reservedHandlers, List<PendingRegistration> claimedIds) {
        for (Method method : userClass.getDeclaredMethods()) {
            StreamSubscription streamSubscription = AnnotationUtils.findAnnotation(method, StreamSubscription.class);
            Subscription subscription = AnnotationUtils.findAnnotation(method, Subscription.class);
            DcbSubscription dcbSubscription = AnnotationUtils.findAnnotation(method, DcbSubscription.class);
            SynchronousSubscription synchronousSubscription = AnnotationUtils.findAnnotation(method, SynchronousSubscription.class);
            long annotationCount = Stream.of(streamSubscription, subscription, dcbSubscription, synchronousSubscription).filter(Objects::nonNull).count();
            if (annotationCount > 1) {
                throw new IllegalArgumentException("Method %s#%s is annotated with more than one of @Subscription, @StreamSubscription, @DcbSubscription and @SynchronousSubscription, use only one.".formatted(userClass.getName(), method.getName()));
            }
            // Reserving is a single atomic add rather than a check followed by an add, so two threads building
            // the same prototype cannot both find the handler free. The one that loses the reservation skips it
            // instead of going on to the id claim and failing its bean's creation as a duplicate.
            if (annotationCount == 0 || !reserveHandler.test(method)) {
                continue;
            }
            // The id comes from the annotation this loop actually read, never from one a caller resolved earlier
            // against a predicted type, because an overriding method may declare a different id than the one it
            // overrides.
            String id = streamSubscription != null ? streamSubscription.id()
                    : subscription != null ? subscription.id()
                    : dcbSubscription != null ? dcbSubscription.id()
                    : synchronousSubscription.id();
            PendingRegistration reserved = new PendingRegistration(method, id, streamSubscription, subscription, dcbSubscription, synchronousSubscription);
            reservedHandlers.add(reserved);
            claimId.accept(id);
            claimedIds.add(reserved);
            resolveHandlerInvocation(bean, handlerTarget, method);
            pending.add(reserved);
        }
    }

    private record PendingRegistration(Method method, String id, StreamSubscription streamSubscription, Subscription subscription,
                                       DcbSubscription dcbSubscription, SynchronousSubscription synchronousSubscription) {
    }

    @SuppressWarnings("unchecked")
    private <E> void processSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, boolean mayBlockForReplay, StreamSubscriptionDefinition subscription) {
        String id = subscription.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, subscription.eventTypes(), subscription.annotationName(), applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation, SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        var startPositionToUse = StartPositionSupport.findStartPositionToUseOrThrow(subscription.id(), subscription.startAtISO8601(), subscription.startAtTimeEpochMillis(), subscription.startAt());
        ResumeBehavior resumeBehavior = subscription.resumeBehavior();
        StartAt startAt = startPositionSupport.generateStartAt(subscription.id(), startPositionToUse, resumeBehavior);

        boolean shouldWaitUntilStarted = mayBlockForReplay && StartPositionSupport.shouldWaitUntilStarted(startPositionToUse, subscription.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        StreamSubscriptions<E> subscribable = applicationContext.getBean(StreamSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        subscribable.subscribe(id, filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processAgnosticSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, boolean mayBlockForReplay, Subscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@Subscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation, SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        long startAtGlobalPosition = annotation.startAtGlobalPosition();
        if (startAtGlobalPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtGlobalPosition for @Subscription '%s', not both.".formatted(id));
        }
        StartAt startAt = startPositionSupport.generateAgnosticStartAt(id, annotation.startAt(), startAtGlobalPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtGlobalPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = mayBlockForReplay && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        Subscriptions<E> subscribable = applicationContext.getBean(Subscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        subscribable.subscribe(id, AgnosticSubscriptionFilter.filter(filter), startAt, shouldWaitUntilStarted, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processSynchronousSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, boolean mayBlockForReplay, SynchronousSubscription annotation) {
        String id = annotation.id();
        SubscriptionAnnotations.ResolvedTypeFilter resolved = SubscriptionAnnotations.<E>resolveTypeFilter(id, bean, method, annotation.eventTypes(), "@SynchronousSubscription", applicationContext.getBean(CloudEventConverter.class));
        List<SubscriptionAnnotations.HandlerParameter> parameters = resolved.parameters();
        Filter filter = resolved.filter();

        HandlerInvocation invocation = resolveHandlerInvocation(bean, handlerTarget, method);
        Function2<EventMetadata, E, Unit> consumer = (metadata, event) -> {
            invoke(invocation, SubscriptionAnnotations.bindArguments(parameters, event, metadata, metadata));
            return Unit.INSTANCE;
        };

        Subscriptions<E> synchronousSubscriptions = applicationContext.getBean(OccurrentBlockingBeanNames.SYNCHRONOUS_SUBSCRIPTION_DSL_BEAN_NAME, Subscriptions.class);
        // The synchronous subscription model has no start position or background thread, so there is no start
        // position to resolve and nothing to wait for. Pass the default StartAt (the model ignores it) rather than
        // null to honor the Subscribable contract.
        synchronousSubscriptions.subscribe(id, AgnosticSubscriptionFilter.filter(filter), StartAt.subscriptionModelDefault(), false, consumer);
    }

    @SuppressWarnings("unchecked")
    private <E> void processDcbSubscribeAnnotation(Object bean, Method method, Supplier<Object> handlerTarget, boolean mayBlockForReplay, DcbSubscription annotation) {
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
        BiConsumer<DcbEventMetadata, E> consumer = (dcbMetadata, event) -> {
            boolean hasDcbEventMetadataParam = parameters.stream().anyMatch(p -> p.type() == DcbEventMetadata.class);
            Object metadataArgument = hasDcbEventMetadataParam ? dcbMetadata : dcbMetadata.eventMetadata();
            invoke(invocation, SubscriptionAnnotations.bindArguments(parameters, event, metadataArgument, dcbMetadata.eventMetadata()));
        };

        long startAtDcbPosition = annotation.startAtDcbPosition();
        if (startAtDcbPosition >= 0 && annotation.startAt() != org.occurrent.annotation.StartPosition.DEFAULT) {
            throw new IllegalArgumentException("Specify either startAt or startAtDcbPosition for @DcbSubscription '%s', not both.".formatted(id));
        }
        DcbStartAt startAt = startPositionSupport.generateDcbStartAt(id, annotation.startAt(), startAtDcbPosition, annotation.resumeBehavior());
        boolean replaysHistory = startAtDcbPosition >= 0 || annotation.startAt() == org.occurrent.annotation.StartPosition.BEGINNING;
        boolean shouldWaitUntilStarted = mayBlockForReplay && SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory, annotation.startupMode()) && SubscriptionAnnotations.subscriptionsStartOnTheirOwn(applicationContext);
        DcbSubscriptions<E> dcbSubscriptions = applicationContext.getBean(DcbSubscriptions.class);

        startPositionSupport.applyStartupWorkarounds();

        var subscription = dcbSubscriptions.subscribeWithMetadata(id, criteria, startAt, consumer);
        if (shouldWaitUntilStarted) {
            subscription.waitUntilStarted();
        }
    }

    // Resolves the object first, then the method against that object, so the two always agree even when a late
    // handler's target changes class once its bean is published.
    private static void invoke(HandlerInvocation invocation, Object[] arguments) {
        Object target = invocation.target();
        invoke(invocation.methodFor(target), target, arguments);
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
