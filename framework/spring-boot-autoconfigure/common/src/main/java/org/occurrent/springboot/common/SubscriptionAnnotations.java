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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.occurrent.annotation.*;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.filter.internal.EventTypeExpansion;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.util.ClassUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.occurrent.filter.Filter.CompositionOperator.OR;

/**
 * Stack-neutral helpers for processing the {@link StreamSubscription}, {@link DcbSubscription} and the
 * capability-agnostic {@link Subscription} annotations. Shared by the blocking and reactive annotation
 * {@code BeanPostProcessor}s so the reflection and event-type resolution logic lives in one place and cannot drift. The
 * stack-specific parts (how the consumer is invoked, how the start position is resolved, and which subscription DSL is
 * used) stay in each processor.
 */
public final class SubscriptionAnnotations {

    private SubscriptionAnnotations() {
    }

    /**
     * What a subscription handler parameter binds to. An {@code EVENT} parameter receives the domain event, a
     * {@code METADATA} parameter receives the {@link EventMetadata} (or {@link DcbEventMetadata} on the DCB path), and
     * a {@code STREAM_ID}/{@code STREAM_VERSION} parameter receives the stream id/version pulled from the metadata
     * (declared with the {@link StreamId}/{@link StreamVersion} annotations).
     */
    public enum HandlerParameterKind {
        EVENT, METADATA, STREAM_ID, STREAM_VERSION
    }

    /**
     * A classified subscription handler parameter: its declared type and what it binds to.
     */
    public record HandlerParameter(Class<?> type, HandlerParameterKind kind) {
    }

    /**
     * The normalized form of a stream subscription declaration, built from the {@link StreamSubscription} annotation.
     */
    public record StreamSubscriptionDefinition(String id, Class<?>[] eventTypes, String startAtISO8601,
                                               long startAtTimeEpochMillis, StartPosition startAt,
                                               ResumeBehavior resumeBehavior, StartupMode startupMode, String annotationName) {

        public static StreamSubscriptionDefinition from(StreamSubscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), subscription.startAt(), subscription.resumeBehavior(), subscription.startupMode(), "@StreamSubscription");
        }
    }

    public static boolean isStreamMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType);
    }

    public static boolean isDcbMetadataParameter(Class<?> parameterType) {
        return EventMetadata.class.isAssignableFrom(parameterType) || DcbEventMetadata.class.isAssignableFrom(parameterType);
    }

    /**
     * Classify each parameter of a subscription handler by declared type and by the {@link StreamId}/{@link StreamVersion}
     * annotations. A parameter is an event parameter unless it is a metadata parameter (per {@code isMetadataParameter})
     * or carries {@code @StreamId}/{@code @StreamVersion}. At most one of each kind is allowed. When
     * {@code supportsStreamAccessors} is {@code false} (the DCB path) a {@code @StreamId}/{@code @StreamVersion}
     * parameter is rejected, because a DCB handler's stream id/version are internal partition values, not domain ones.
     */
    public static List<HandlerParameter> analyzeParameters(Method method, Predicate<Class<?>> isMetadataParameter, boolean supportsStreamAccessors) {
        return analyzeParameters(method, method.getDeclaringClass(), isMetadataParameter, supportsStreamAccessors);
    }

    /**
     * Classify each parameter of a subscription handler as {@link #analyzeParameters(Method, Predicate, boolean)}
     * does, reading each parameter's type as {@code implementation} sees it. A handler that {@code implementation}
     * inherits from a generic superclass or interface, such as {@code on(E event)} in a {@code Base<E>} that
     * {@code implementation} extends as {@code Base<OrderPlaced>}, then has {@code OrderPlaced} as its event type. A type
     * variable that {@code implementation} does not bind, through a raw supertype for example, keeps the type the
     * method declares, its bound or {@code Object}.
     *
     * @param method                  the handler method
     * @param implementation          the class whose instances the handler runs on, see {@link #implementationClassOf(Object)}
     * @param isMetadataParameter     whether a parameter type is a metadata parameter
     * @param supportsStreamAccessors whether {@code @StreamId}/{@code @StreamVersion} parameters are allowed
     * @return the classified parameters, in declaration order
     */
    public static List<HandlerParameter> analyzeParameters(Method method, Class<?> implementation, Predicate<Class<?>> isMetadataParameter, boolean supportsStreamAccessors) {
        List<HandlerParameter> parameters = new ArrayList<>();
        boolean hasEvent = false;
        boolean hasMetadata = false;
        boolean hasStreamId = false;
        boolean hasStreamVersion = false;
        Parameter[] declaredParameters = method.getParameters();
        for (int index = 0; index < declaredParameters.length; index++) {
            Parameter parameter = declaredParameters[index];
            Class<?> type = ResolvableType.forMethodParameter(method, index, implementation).resolve(parameter.getType());
            boolean streamIdAnnotated = parameter.isAnnotationPresent(StreamId.class);
            boolean streamVersionAnnotated = parameter.isAnnotationPresent(StreamVersion.class);
            if (streamIdAnnotated && streamVersionAnnotated) {
                throw new IllegalArgumentException("A subscription parameter may not be annotated with both @StreamId and @StreamVersion, but %s#%s declares one that is.".formatted(method.getDeclaringClass().getName(), method.getName()));
            }
            HandlerParameterKind kind;
            if (streamIdAnnotated) {
                if (!supportsStreamAccessors) {
                    throw new IllegalArgumentException("@StreamId is only supported on @Subscription, @StreamSubscription, and @SynchronousSubscription handlers, but %s#%s declares it.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                if (type != String.class) {
                    throw new IllegalArgumentException("A @StreamId parameter must be of type String, but %s#%s declares it as %s.".formatted(method.getDeclaringClass().getName(), method.getName(), type.getName()));
                }
                if (hasStreamId) {
                    throw new IllegalArgumentException("A subscription method may declare at most one @StreamId parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasStreamId = true;
                kind = HandlerParameterKind.STREAM_ID;
            } else if (streamVersionAnnotated) {
                if (!supportsStreamAccessors) {
                    throw new IllegalArgumentException("@StreamVersion is only supported on @Subscription, @StreamSubscription, and @SynchronousSubscription handlers, but %s#%s declares it.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                if (type != long.class && type != Long.class) {
                    throw new IllegalArgumentException("A @StreamVersion parameter must be of type long or Long, but %s#%s declares it as %s.".formatted(method.getDeclaringClass().getName(), method.getName(), type.getName()));
                }
                if (hasStreamVersion) {
                    throw new IllegalArgumentException("A subscription method may declare at most one @StreamVersion parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasStreamVersion = true;
                kind = HandlerParameterKind.STREAM_VERSION;
            } else if (isMetadataParameter.test(type)) {
                if (hasMetadata) {
                    throw new IllegalArgumentException("A subscription method may declare at most one metadata parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasMetadata = true;
                kind = HandlerParameterKind.METADATA;
            } else {
                if (hasEvent) {
                    throw new IllegalArgumentException("A subscription method may declare only one event parameter, but %s#%s declares more than one.".formatted(method.getDeclaringClass().getName(), method.getName()));
                }
                hasEvent = true;
                kind = HandlerParameterKind.EVENT;
            }
            parameters.add(new HandlerParameter(type, kind));
        }
        return parameters;
    }

    public static Class<?> eventTypeOf(List<HandlerParameter> parameters) {
        return parameters.stream().filter(p -> p.kind() == HandlerParameterKind.EVENT).map(HandlerParameter::type).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("You need to declare an event type"));
    }

    @SuppressWarnings("unchecked")
    public static <E> List<Class<E>> resolveDomainEventTypes(String id, Object bean, Method method, Class<E> specifiedEventType, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName) {
        if (eventTypesSpecifiedInAnnotation.length == 0) {
            return typesToSubscribeOn(id, List.of(specifiedEventType));
        }
        List<Class<E>> declaredTypes = Arrays.stream(eventTypesSpecifiedInAnnotation).map(e -> (Class<E>) e).toList();
        // The handler takes one parameter, so every type it is asked to receive has to fit it. Checked over the concrete
        // types rather than over what the filter names, because a declared supertype does not fit a narrower parameter
        // that its own concrete types do.
        for (Class<E> declaredType : declaredTypes) {
            for (Class<E> concreteType : getConcreteEventTypes(id, declaredType)) {
                if (!specifiedEventType.isAssignableFrom(concreteType)) {
                    throw new IllegalStateException("Event type %s specified in the %s annotation with id %s is not assignable from the event type specified in %s#%s(..).".formatted(concreteType.getName(), annotationName, id, bean.getClass().getName(), method.getName()));
                }
            }
        }
        return typesToSubscribeOn(id, declaredTypes);
    }

    /**
     * The parameters of a subscription handler plus the {@link Filter} that selects the events it subscribes to,
     * resolved together from the annotated method.
     */
    public record ResolvedTypeFilter(List<HandlerParameter> parameters, Filter filter) {
    }

    /**
     * Resolve a type-based subscription handler: validate it declares an event parameter, resolve the domain event
     * types it subscribes to (expanding a sealed type, or using the annotation's explicit {@code eventTypes}), and build
     * the {@link Filter} that matches those types. Shared by the stream, capability-agnostic, and synchronous
     * annotation paths so the resolution and filter construction live in one place. The DCB path uses
     * {@link #buildDcbCriteria(List, List)} instead.
     *
     * @param id                            the subscription id
     * @param bean                          the bean the handler runs on, whose class the event parameter's type is read against
     * @param method                        the handler method
     * @param eventTypesSpecifiedInAnnotation the annotation's {@code eventTypes} (empty to derive from the parameter)
     * @param annotationName                the annotation name, for error messages
     * @param cloudEventConverter           resolves domain event types to cloud event types
     * @param <E>                           the domain event type
     * @return the handler's parameters and the type filter
     */
    public static <E> ResolvedTypeFilter resolveTypeFilter(String id, Object bean, Method method, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName, CloudEventConverter<E> cloudEventConverter) {
        if (method.getParameterCount() < 1) {
            throw new IllegalArgumentException("A subscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }
        List<HandlerParameter> parameters = analyzeParameters(method, implementationClassOf(bean), SubscriptionAnnotations::isStreamMetadataParameter, true);
        @SuppressWarnings("unchecked")
        Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameters);
        List<Class<E>> domainEventTypes = resolveDomainEventTypes(id, bean, method, specifiedEventType, eventTypesSpecifiedInAnnotation, annotationName);

        Filter filter;
        if (domainEventTypes.size() == 1) {
            filter = Filter.type(cloudEventConverter.getCloudEventType(domainEventTypes.get(0)));
        } else {
            List<Filter> typeFilters = domainEventTypes.stream()
                    .map(cloudEventConverter::getCloudEventType)
                    .map(Filter::type)
                    .toList();
            filter = new Filter.CompositionFilter(OR, typeFilters);
        }
        return new ResolvedTypeFilter(parameters, filter);
    }

    public static DcbCriteria buildDcbCriteria(List<String> cloudEventTypes, List<Tag> tags) {
        boolean hasTypes = !cloudEventTypes.isEmpty();
        boolean hasTags = !tags.isEmpty();
        if (!hasTypes && !hasTags) {
            return DcbCriteria.all();
        } else if (hasTypes && hasTags) {
            return DcbCriteria.types(cloudEventTypes.get(0), cloudEventTypes.stream().skip(1).toArray(String[]::new)).tags(tags);
        } else if (hasTypes) {
            return DcbCriteria.types(cloudEventTypes.get(0), cloudEventTypes.stream().skip(1).toArray(String[]::new));
        } else {
            return DcbCriteria.tags(tags);
        }
    }

    /**
     * Build the argument array for a subscription handler, one slot per {@link HandlerParameter} in declaration order.
     * An event slot gets {@code event}, a metadata slot gets {@code metadataArgument} (an {@link EventMetadata} on the
     * stream path, an {@link EventMetadata} or {@link DcbEventMetadata} on the DCB path), and a stream-id/version slot
     * gets the value read from {@code eventMetadata}. A {@code long} stream-version slot binds the boxed {@code Long}
     * through reflective unboxing.
     */
    public static Object[] bindArguments(List<HandlerParameter> parameters, Object event, Object metadataArgument, EventMetadata eventMetadata) {
        Object[] arguments = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            arguments[i] = switch (parameters.get(i).kind()) {
                case EVENT -> event;
                case METADATA -> metadataArgument;
                case STREAM_ID -> eventMetadata.getStreamId();
                case STREAM_VERSION -> eventMetadata.getStreamVersion();
            };
        }
        return arguments;
    }

    /**
     * Validate the mode and catch-up start knobs of a moded read-model annotation ({@link org.occurrent.annotation.Projection}
     * or {@link org.occurrent.annotation.Snapshot}). The synchronous mode is read-your-writes on the write path and has
     * no catch-up phase, so it cannot carry any catch-up start knob, and startAt and startAtGlobalPosition are two ways to
     * express the same start point so at most one may be set. Shared by the blocking and reactive processors for both
     * annotations so this rule and its message live in one tested place and cannot drift.
     *
     * @param annotationName           the annotation name for error messages, for example {@code "@Projection"}
     * @param id                       the annotation id for error messages
     * @param synchronous              whether the annotation declares the synchronous mode
     * @param startAtSet               whether startAt is set to something other than its default
     * @param startAtGlobalPositionSet whether startAtGlobalPosition is set
     * @param resumeBehaviorSet        whether resumeBehavior is set to something other than its default
     * @param startupModeSet           whether startupMode is set to something other than its default
     */
    public static void validateModeStartKnobs(String annotationName, String id, boolean synchronous,
                                              boolean startAtSet, boolean startAtGlobalPositionSet, boolean resumeBehaviorSet, boolean startupModeSet) {
        if (synchronous && (startAtSet || startAtGlobalPositionSet || resumeBehaviorSet || startupModeSet)) {
            String noun = annotationName.replace("@", "").toLowerCase(Locale.ROOT);
            throw new IllegalArgumentException("%s '%s' uses mode = SYNCHRONOUS, which cannot be combined with startAt, startAtGlobalPosition, resumeBehavior, or startupMode (those configure catch-up for an async %s).".formatted(annotationName, id, noun));
        }
        if (startAtSet && startAtGlobalPositionSet) {
            throw new IllegalArgumentException("%s '%s' sets both startAt and startAtGlobalPosition, which are two ways to express the same start point, so set only one.".formatted(annotationName, id));
        }
    }

    /**
     * Invoke a descriptor factory method ({@code @Projection}, {@code @Snapshot} or {@code @Saga}), unwrapping
     * {@code bean} to its ultimate AOP target first. Shared by every registrar that invokes one of these factories, on
     * both stacks, so the unwrap only has to be right in one place.
     * <p>
     * A descriptor factory runs exactly once at startup to build a value, so there is no request for advice to
     * usefully observe, and invoking it through a proxy is a hazard rather than a feature. The scan reads
     * {@code method} from the bean's class, its superclasses and its interfaces, so the factory runs when the
     * unwrapped target's own user class, {@link ClassUtils#getUserClass}, declares {@code method} or inherits it
     * unchanged. Otherwise the factory refuses instead of guessing which method to run. That happens for a factory
     * bean that built a different implementation than the one the scan recorded, such as a subclass overriding
     * {@code method}, which would run code the scan never read.
     * <p>
     * Two proxies are left proxied by {@link #ultimateTarget}, because their target source is not a fixed singleton.
     * A JDK interface proxy runs the factory through the proxy, by the method {@link #invocableMethodOn} selects, when
     * one of the proxy's interfaces declares the factory the scan read from the class behind it, and refuses any other
     * method, since the proxy does not implement it. A CGLIB proxy passes the check above, since {@code getUserClass} of the proxy's own
     * class is the class it subclasses, so a factory invoked through it still runs its advice. It refuses an inherited
     * instance {@code method} the proxy cannot override, one that is private, final, or package-private and declared in
     * another package than that class, since invoking it would run the method's body on the proxy instance instead of
     * on a target. A private or final factory that class declares runs on the proxy instance, as it did before
     * inherited factories were found, and a static factory runs, since it reads no instance.
     *
     * @param annotationName the annotation name for error messages, for example {@code "@Projection"}
     * @param bean           the (possibly proxied) bean the factory method was found on
     * @param method         the no-argument factory method the scan found for {@code bean}
     * @return the descriptor the factory method returned, never {@code null}
     */
    public static Object invokeDescriptorFactory(String annotationName, Object bean, Method method) {
        Object target = ultimateTarget(bean);
        Class<?> declaringClass = method.getDeclaringClass();
        Method invoked = method;
        if (java.lang.reflect.Proxy.isProxyClass(target.getClass())) {
            try {
                invoked = invocableMethodOn(target, method);
            } catch (IllegalStateException e) {
                throw new IllegalStateException("%s factory %s#%s cannot run because the bean is a JDK interface proxy whose target is not a fixed singleton, so it cannot be unwrapped safely, and the proxy does not implement %s, which declares the factory. Set spring.aop.proxy-target-class=true so this bean is proxied by subclassing instead of by interface, or move the factory method off the advised bean.".formatted(annotationName, bean.getClass().getName(), method.getName(), declaringClass.getName()));
            }
        } else {
            Class<?> targetUserClass = ClassUtils.getUserClass(target.getClass());
            if (!runsUnchanged(targetUserClass, method)) {
                throw new IllegalStateException("%s factory %s#%s cannot run because %s does not run the factory the scan read from %s, it either overrides it or does not inherit it. This happens when the factory bean built a different implementation than the one the scan recorded, so return the same implementation on every call.".formatted(annotationName, bean.getClass().getName(), method.getName(), targetUserClass.getName(), declaringClass.getName()));
            }
            int modifiers = method.getModifiers();
            if (AopUtils.isCglibProxy(target) && !Modifier.isStatic(modifiers) && !declaringClass.equals(targetUserClass)) {
                if (Modifier.isPrivate(modifiers) || Modifier.isFinal(modifiers)) {
                    throw new IllegalStateException("%s factory %s#%s cannot run because it is inherited from %s and %s, so the CGLIB proxy around the bean cannot override it and would run it on the proxy instead of on the bean. Make the method public or protected and not final, or declare the factory on %s.".formatted(annotationName, bean.getClass().getName(), method.getName(), declaringClass.getName(), Modifier.isPrivate(modifiers) ? "private" : "final", targetUserClass.getName()));
                }
                if (isPackagePrivateOutsidePackageOf(method, targetUserClass)) {
                    throw new IllegalStateException("%s factory %s#%s cannot run because it is package-private and declared in %s, outside the package of %s, so the CGLIB proxy around the bean cannot override it and would run it on the proxy instead of on the bean. Make the method public or protected.".formatted(annotationName, bean.getClass().getName(), method.getName(), declaringClass.getName(), targetUserClass.getName()));
                }
            }
        }
        try {
            invoked.setAccessible(true);
            Object result = invoked.invoke(target);
            if (result == null) {
                throw new IllegalStateException("%s factory %s#%s returned null.".formatted(annotationName, target.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke %s factory %s#%s.".formatted(annotationName, target.getClass().getName(), method.getName()), e);
        }
    }

    /**
     * Unwraps through any number of nested AOP proxies to the innermost fixed target
     * ({@link AopProxyUtils#getSingletonTarget} stops at one layer, hence the loop). Returns {@code bean} itself
     * when it is not a proxy, or when a proxy's {@code TargetSource} is not a fixed singleton, a prototype- or
     * pool-backed source is left proxied rather than risking a side-effecting {@code getTarget()} call, or
     * unwrapping to a different target instance than the one a caller registered a descriptor or handler against.
     *
     * @param bean the (possibly proxied, possibly nested-proxied) bean to unwrap
     * @return the innermost fixed target, or {@code bean} itself when there is none to unwrap to
     */
    public static Object ultimateTarget(Object bean) {
        Object current = bean;
        Object next;
        while ((next = AopProxyUtils.getSingletonTarget(current)) != null) {
            current = next;
        }
        return current;
    }

    /**
     * Walks the same nested-proxy chain {@link #ultimateTarget} unwraps, checking every layer for a CGLIB proxy
     * instead of returning the innermost target. A JDK interface proxy forwards a call reflectively using the
     * interface's {@link Method}, so Java's own virtual dispatch resolves it against whatever the wrapped target
     * actually is, a nested CGLIB proxy included. Checking only {@code bean} itself misses exactly that case, since
     * an outer JDK proxy is never itself a CGLIB proxy.
     *
     * @param bean the (possibly proxied, possibly nested-proxied) bean to check
     * @return {@code true} if {@code bean} or any proxy layer it unwraps to is CGLIB-backed
     */
    public static boolean anyProxyLayerIsCglib(Object bean) {
        Object current = bean;
        while (true) {
            if (AopUtils.isCglibProxy(current)) {
                return true;
            }
            Object next = AopProxyUtils.getSingletonTarget(current);
            if (next == null) {
                return false;
            }
            current = next;
        }
    }

    /**
     * Answers whether invoking {@code method} on {@code bean} runs the implementation the scan read the method from.
     * The scan reads a handler from one class and the object it is invoked on can be another, a prototype whose
     * factory returns a different implementation on a later call for example. Reflective invocation dispatches
     * virtually, so an unrelated class sharing an interface with the scanned one, or a subclass overriding the
     * method, would run its own code under the scanned method's subscription id.
     * <p>
     * The bean is unwrapped with {@link #ultimateTarget} first, so a proxy around the right implementation passes. A
     * subclass that inherits the method unchanged passes too, since it runs the same code. A proxy whose target
     * source is not a fixed singleton, a scoped proxy for example, is not unwrapped, since that would mean asking it
     * for a target. A JDK interface proxy like that passes, and a CGLIB one is checked by the class it subclasses, so
     * neither checks the objects its target source hands out. The handler keeps running through that proxy.
     *
     * @param bean   the (possibly proxied) object the handler is about to be invoked on
     * @param method the handler method the scan read
     * @return {@code true} if the unwrapped object's class inherits {@code method} without overriding it
     */
    public static boolean runsImplementationOf(Object bean, Method method) {
        Object target = ultimateTarget(bean);
        if (java.lang.reflect.Proxy.isProxyClass(target.getClass())) {
            return true;
        }
        return runsUnchanged(ClassUtils.getUserClass(target.getClass()), method);
    }

    private static boolean runsUnchanged(Class<?> implementation, Method method) {
        return method.getDeclaringClass().isAssignableFrom(implementation)
                && ClassUtils.getMostSpecificMethod(method, implementation).equals(method);
    }

    /**
     * The class whose code runs when a handler or factory is invoked on {@code bean}: the user class,
     * {@link ClassUtils#getUserClass}, of the target class {@link AopProxyUtils#ultimateTargetClass} reads from
     * {@link #ultimateTarget}. A proxy whose target source is not a fixed singleton is not unwrapped, but its target
     * source names the class it hands out, so a JDK interface proxy like that answers with its target's class rather
     * than its own. Both annotation post-processors scan this class, and handler and factory generics are read
     * against it, so the two agree on one {@link Method} per handler.
     *
     * @param bean the (possibly proxied) bean
     * @return the class a bean is scanned for handlers in, and a handler's or factory's generic types are read against
     */
    public static Class<?> implementationClassOf(Object bean) {
        // ultimateTargetClass answers with the proxy's own class when the target source names no target class, so
        // such a bean still scans the proxy class and its handlers are keyed by the interface method
        return ClassUtils.getUserClass(AopProxyUtils.ultimateTargetClass(ultimateTarget(bean)));
    }

    /**
     * The method to invoke on {@code bean} to run {@code method}, the one {@link AopUtils#selectInvocableMethod} selects
     * for {@code bean}'s class. A JDK interface proxy implements only its interfaces, and the scan reads the class
     * behind it, so when the scanned method overrides a generic interface method, such as {@code on(Placed)} for
     * {@code on(E)}, no interface declares the scanned signature and the selection fails. The interface method that
     * the class behind the proxy implements with {@code method} is returned instead, so the call still goes through
     * the proxy and its advice.
     *
     * @param bean   the (possibly proxied) object the method is about to be invoked on
     * @param method the method the scan read
     * @return the method to invoke on {@code bean}
     * @throws IllegalStateException when {@code bean} has no method that runs {@code method}
     */
    public static Method invocableMethodOn(Object bean, Method method) {
        try {
            return AopUtils.selectInvocableMethod(method, bean.getClass());
        } catch (IllegalStateException e) {
            Method interfaceMethod = proxyInterfaceMethodRunning(bean, method);
            if (interfaceMethod == null) {
                throw e;
            }
            return interfaceMethod;
        }
    }

    private static @Nullable Method proxyInterfaceMethodRunning(Object bean, Method method) {
        if (!java.lang.reflect.Proxy.isProxyClass(bean.getClass())) {
            return null;
        }
        Class<?> implementation = implementationClassOf(bean);
        for (Class<?> proxiedInterface : bean.getClass().getInterfaces()) {
            for (Method interfaceMethod : proxiedInterface.getMethods()) {
                if (!Modifier.isStatic(interfaceMethod.getModifiers()) && interfaceMethod.getName().equals(method.getName())
                        && interfaceMethod.getParameterCount() == method.getParameterCount()
                        && AopUtils.getMostSpecificMethod(interfaceMethod, implementation).equals(method)) {
                    return interfaceMethod;
                }
            }
        }
        return null;
    }

    /**
     * Answers whether {@code method} is package-private and declared in another package than {@code implementation}.
     * A CGLIB proxy of {@code implementation} is generated in {@code implementation}'s package, so it cannot override
     * such a method, and invoking the method on the proxy runs the method's own body on the proxy instance, with no
     * advice and none of the bean's state.
     *
     * @param method         the handler or factory method
     * @param implementation the class a CGLIB proxy in the bean's chain subclasses
     * @return {@code true} if a CGLIB proxy of {@code implementation} cannot override {@code method} for being package-private
     */
    public static boolean isPackagePrivateOutsidePackageOf(Method method, Class<?> implementation) {
        int modifiers = method.getModifiers();
        return !Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers) && !Modifier.isPrivate(modifiers)
                && !method.getDeclaringClass().getPackageName().equals(implementation.getPackageName());
    }

    /**
     * Read type argument {@code index} of {@code method}'s return type against {@code implementation} and return its
     * class. A factory that {@code implementation} inherits from a generic superclass or interface, such as
     * {@code Projection<S, MyEvent, String> projection()} in a {@code Base<S>} that {@code implementation} extends as
     * {@code Base<MyView>}, then names {@code MyView}. A parameterized argument names its raw class.
     *
     * @param method         the factory method
     * @param implementation the class whose instance the factory runs on, see {@link #implementationClassOf(Object)}
     * @param index          the type argument to read
     * @return the class, or {@code null} when the return type has no such type argument, or the argument is a wildcard
     * or a type variable that {@code implementation} does not bind, through a raw supertype or a type parameter of its
     * own for example, or one the method itself declares
     */
    public static @Nullable Class<?> returnTypeArgument(Method method, Class<?> implementation, int index) {
        if (!(method.getGenericReturnType() instanceof ParameterizedType returnType) || returnType.getActualTypeArguments().length <= index) {
            return null;
        }
        Type argument = returnType.getActualTypeArguments()[index];
        // ResolvableType.resolve() falls back to a variable's bound, which names no class the factory chose, so the
        // binding is followed by hand and an unbound variable answers null
        while (argument instanceof TypeVariable<?> variable) {
            if (!(variable.getGenericDeclaration() instanceof Class<?> declaringClass)) {
                return null;
            }
            int position = Arrays.asList(declaringClass.getTypeParameters()).indexOf(variable);
            Type binding = ResolvableType.forClass(implementation).as(declaringClass).getGeneric(position).getType();
            if (binding.equals(variable)) {
                return null;
            }
            argument = binding;
        }
        if (argument instanceof Class<?> type) {
            return type;
        }
        if (argument instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return null;
    }

    /**
     * The methods of {@code type} annotated with any of {@code annotationTypes}, read from {@code type}, its
     * superclasses and its interfaces, so an inherited handler and an interface default method count the way a
     * declared one does.
     * Each method is the one {@code type} actually runs, so an override is found once, under its own annotation if it
     * repeats one and under the overridden method's if it doesn't. Bridge methods are left out, so a generic handler
     * is found once too. Overriding follows the JVM's rules, under which a package-private method is overridden only
     * from its own package or through a chain of overrides that starts there. Spring's annotation search ignores
     * packages, so it can give a method the annotation of a package-private method the method doesn't override. Such a
     * method is refused with an {@link IllegalStateException}, since nothing says which of the two bodies the handler
     * is, unless {@code type} or a superclass below it declares a method of the same signature, which is then checked
     * instead. A package-private method that a method of {@code type} or one of its superclasses overrides is left out, since
     * {@code type} never runs it. A static method an interface declares is left out, since a class does not inherit
     * it. A static method a superclass declares is returned like any other, so a static subscription handler there is
     * refused the way one the class declares is. Both annotation post-processors call this, so they find the same
     * methods.
     * <p>
     * The scan misses this case. When {@code type} is in the package of a package-private annotated method, Spring
     * takes the first method of the same signature up from {@code type} as the one it runs, even a method in another
     * package that doesn't override it. When that method is private, or has a handler annotation of its own, the
     * annotated method isn't returned, though the JVM runs it.
     *
     * @param type            the concrete class the scan reads
     * @param annotationTypes the handler annotations to look for, searched with {@link SearchStrategy#TYPE_HIERARCHY}
     * @return the matching methods, empty if there are none
     * @throws IllegalStateException when a method has an annotation Spring reads from a method it does not override
     */
    public static Set<Method> handlerMethods(Class<?> type, List<Class<? extends Annotation>> annotationTypes) {
        if (!AnnotationUtils.isCandidateClass(type, annotationTypes)) {
            return Set.of();
        }
        return MethodIntrospector.selectMethods(type, (MethodIntrospector.MetadataLookup<Boolean>) method -> {
            if (Modifier.isStatic(method.getModifiers()) && method.getDeclaringClass().isInterface()) {
                return null;
            }
            MergedAnnotations annotations = MergedAnnotations.from(method, SearchStrategy.TYPE_HIERARCHY);
            // MethodIntrospector can hand over a bridge method and keeps the method it bridges instead, so that is the
            // method the checks below compare against
            Method bridged = BridgeMethodResolver.findBridgedMethod(method);
            boolean annotated = false;
            for (Class<? extends Annotation> annotationType : annotationTypes) {
                MergedAnnotation<? extends Annotation> annotation = annotations.get(annotationType);
                if (!annotation.isPresent()) {
                    continue;
                }
                // MethodIntrospector keeps a package-private method of another package than type even when a method of
                // its own package overrides it, since Spring decides overriding by type's package alone
                if (!methodTheJvmRuns(bridged, type).equals(bridged)) {
                    return null;
                }
                if (annotation.getSource() instanceof Method declaration && !methodTheJvmRuns(declaration, type).equals(bridged)) {
                    // A method of the same signature declared below it is the one the class shows for that
                    // signature, and it is checked on its own, as a handler or refused
                    if (declaredBelow(bridged, type)) {
                        return null;
                    }
                    throw annotationFromAMethodNotOverridden(bridged, declaration, annotation);
                }
                annotated = true;
            }
            return annotated ? Boolean.TRUE : null;
        }).keySet();
    }

    // The method the JVM runs when method is invoked on an instance of type, bridges resolved to the method they
    // bridge. The JVM picks the first method of the same signature up from type that overrides method, and Spring's
    // ClassUtils.getMostSpecificMethod decides overriding for a package-private method by type's package instead of
    // the overriding method's, which is wrong when a superclass in method's own package overrides it
    private static Method methodTheJvmRuns(Method method, Class<?> type) {
        Class<?> declaringClass = method.getDeclaringClass();
        int modifiers = method.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers)) {
            return method;
        }
        if (declaringClass.isInterface() || !declaringClass.isAssignableFrom(type)) {
            return BridgeMethodResolver.findBridgedMethod(AopUtils.getMostSpecificMethod(method, type));
        }
        for (Class<?> candidateClass = type; candidateClass != null && candidateClass != declaringClass; candidateClass = candidateClass.getSuperclass()) {
            Method candidate = declaredMethod(candidateClass, method);
            if (candidate != null && overrides(candidate, method)) {
                return BridgeMethodResolver.findBridgedMethod(candidate);
            }
        }
        return BridgeMethodResolver.findBridgedMethod(method);
    }

    // Whether overrider, declared in a subclass of overridden's declaring class, overrides it under the JVM's rules
    // (JVMS 5.4.5). A package-private method is overridden from its own runtime package, or by overriding a method in
    // between that overrides it
    private static boolean overrides(Method overrider, Method overridden) {
        if (Modifier.isPrivate(overrider.getModifiers()) || Modifier.isStatic(overrider.getModifiers())) {
            return false;
        }
        int modifiers = overridden.getModifiers();
        if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)) {
            return false;
        }
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || inSameRuntimePackage(overrider.getDeclaringClass(), overridden.getDeclaringClass())) {
            return true;
        }
        for (Class<?> between = overrider.getDeclaringClass().getSuperclass(); between != null && between != overridden.getDeclaringClass(); between = between.getSuperclass()) {
            Method intermediate = declaredMethod(between, overridden);
            if (intermediate != null && overrides(intermediate, overridden) && overrides(overrider, intermediate)) {
                return true;
            }
        }
        return false;
    }

    // Whether type or a superclass of type below method's declaring class declares an instance method of method's
    // signature that isn't private
    private static boolean declaredBelow(Method method, Class<?> type) {
        for (Class<?> candidateClass = type; candidateClass != null && candidateClass != method.getDeclaringClass(); candidateClass = candidateClass.getSuperclass()) {
            Method candidate = declaredMethod(candidateClass, method);
            if (candidate != null && !Modifier.isPrivate(candidate.getModifiers()) && !Modifier.isStatic(candidate.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    private static boolean inSameRuntimePackage(Class<?> a, Class<?> b) {
        return a.getPackageName().equals(b.getPackageName()) && a.getClassLoader() == b.getClassLoader();
    }

    private static @Nullable Method declaredMethod(Class<?> type, Method signature) {
        try {
            return type.getDeclaredMethod(signature.getName(), signature.getParameterTypes());
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Answers whether {@code beanName} names the proxy of a scoped bean, a definition that decorates its target's
     * definition under {@link ScopedProxyUtils#getTargetBeanName}. Both annotation post-processors skip such a name,
     * so a scoped bean's handlers register once, under its target's name. Internal to the Spring Boot starters.
     *
     * @param beanFactory the bean factory that defines {@code beanName}
     * @param beanName    the bean name the scan is about to read
     * @return {@code true} when {@code beanName} is a scoped proxy whose target has a definition of its own
     */
    public static boolean isScopedProxy(ConfigurableListableBeanFactory beanFactory, String beanName) {
        if (!beanFactory.containsBeanDefinition(beanName)) {
            return false;
        }
        String targetBeanName = ScopedProxyUtils.getTargetBeanName(beanName);
        BeanDefinitionHolder decorated = beanFactory.getMergedBeanDefinition(beanName) instanceof RootBeanDefinition root ? root.getDecoratedDefinition() : null;
        return decorated != null && decorated.getBeanName().equals(targetBeanName) && beanFactory.containsBeanDefinition(targetBeanName);
    }

    private static IllegalStateException annotationFromAMethodNotOverridden(Method method, Method declaration, MergedAnnotation<? extends Annotation> annotation) {
        String annotationName = "@" + annotation.getType().getSimpleName();
        String handler = annotation.getValue("id", String.class).map(id -> "%s with id \"%s\"".formatted(annotationName, id)).orElse(annotationName);
        if (Modifier.isStatic(method.getModifiers())) {
            return new IllegalStateException(("Method %s is static, so it does not override %s, which declares %s, but Spring's annotation search finds the annotation on both, so which body is the handler is ambiguous. "
                    + "Rename %s to keep %s as the handler.").formatted(signatureOf(method), signatureOf(declaration), handler, signatureOf(method), signatureOf(declaration)));
        }
        return new IllegalStateException(("Method %s does not override %s, which declares %s, but Spring's annotation search finds the annotation on both, so which body is the handler is ambiguous. "
                + "Make both %s and %s protected or public so %s overrides it and runs as the handler, or rename %s to keep %s as the handler.").formatted(
                signatureOf(method), signatureOf(declaration), handler, signatureOf(declaration), signatureOf(method), signatureOf(method), signatureOf(method), signatureOf(declaration)));
    }

    private static String signatureOf(Method method) {
        return method.getDeclaringClass().getName() + '#' + method.getName()
                + Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.joining(", ", "(", ")"));
    }

    /**
     * Resolve the push feed bean of a {@code source = PUSH} projection, selected by {@code subscriptionModelType}
     * (the annotation's {@code subscriptionModel}) or {@code subscriptionModelName}, or the unique bean of one of
     * {@code candidateTypes} when neither is set. Shared by the blocking and reactive processors so the resolution rules
     * and error messages live in one place and cannot drift. The caller branches on the returned bean's runtime type.
     *
     * @param applicationContext    the Spring context to resolve beans from
     * @param annotationName        the annotation the message should name, for example {@code "@Projection"}
     * @param subscriptionModelType the annotation's {@code subscriptionModel} type, or {@code Void.class} if unset
     * @param subscriptionModelName the annotation's {@code subscriptionModelName}, or blank if unset
     * @param id                    the projection or saga id, for error messages
     * @param candidateTypes        the allowed feed bean types, which the messages are worded from, so an annotation
     *                              accepting one kind of feed does not offer the reader a second one it will reject
     * @return the resolved feed bean
     */
    public static Object resolveFeedBean(ApplicationContext applicationContext, String annotationName, Class<?> subscriptionModelType,
                                         String subscriptionModelName, String id, Class<?>... candidateTypes) {
        boolean byType = subscriptionModelType != Void.class;
        boolean byName = !subscriptionModelName.isBlank();
        // Two forms, because "found no a PushSubscriptionModel bean" is not a sentence.
        String acceptedTypes = Arrays.stream(candidateTypes).map(Class::getSimpleName).collect(Collectors.joining(" or "));
        String acceptedWithArticles = Arrays.stream(candidateTypes).map(Class::getSimpleName).collect(Collectors.joining(" or a ", "a ", ""));
        if (byType && Arrays.stream(candidateTypes).noneMatch(candidate -> candidate.isAssignableFrom(subscriptionModelType))) {
            throw new IllegalArgumentException("%s '%s' subscriptionModel type %s must be %s for source=PUSH.".formatted(annotationName, id, subscriptionModelType.getName(), acceptedWithArticles));
        }
        try {
            if (byName) {
                return byType ? applicationContext.getBean(subscriptionModelName, subscriptionModelType) : applicationContext.getBean(subscriptionModelName);
            }
            if (byType) {
                return applicationContext.getBean(subscriptionModelType);
            }
            List<String> names = candidateBeanNames(applicationContext, true, candidateTypes);
            if (names.isEmpty()) {
                throw new IllegalStateException("%s '%s' with source=PUSH found no %s bean. Declare one, or name it with subscriptionModelName.".formatted(annotationName, id, acceptedTypes));
            }
            if (names.size() > 1) {
                throw new IllegalStateException("%s '%s' with source=PUSH found several push feed beans (%s). Pick one with subscriptionModel or subscriptionModelName.".formatted(annotationName, id, String.join(", ", names)));
            }
            return applicationContext.getBean(names.get(0));
        } catch (BeansException e) {
            throw new IllegalArgumentException("%s '%s' with source=PUSH could not resolve a push feed bean (subscriptionModel=%s, subscriptionModelName='%s'): %s".formatted(annotationName, id, byType ? subscriptionModelType.getName() : "unset", subscriptionModelName, e.getMessage()), e);
        }
    }

    // The bean names matching one of candidateTypes, shared by resolveFeedBean's own unique-bean fallback and by
    // resolveFeedBeanType's read-only mirror of it, so the two cannot silently drift on what counts as a candidate.
    // resolveFeedBean passes allowEagerInit = true, since it is about to call getBean on the result anyway.
    // resolveFeedBeanType passes false, so a FactoryBean-backed candidate is not asked to build its product just to
    // be counted.
    private static List<String> candidateBeanNames(ApplicationContext applicationContext, boolean allowEagerInit, Class<?>... candidateTypes) {
        List<String> names = new ArrayList<>();
        for (Class<?> candidateType : candidateTypes) {
            Collections.addAll(names, applicationContext.getBeanNamesForType(candidateType, true, allowEagerInit));
        }
        return names;
    }

    /**
     * The push feed bean's type, resolved by the same rule {@link #resolveFeedBean} uses to pick the bean itself
     * (an explicit {@code subscriptionModelType}, {@code subscriptionModelName}, or the unique bean of one of
     * {@code candidateTypes}), but never creating the feed bean itself. An explicit type is read straight off the
     * annotation attribute, and a name or unique-type lookup goes through the {@code allowFactoryBeanInit = false}
     * overloads of {@link ApplicationContext#getType(String, boolean)} and
     * {@link ApplicationContext#getBeanNamesForType(Class, boolean, boolean)}, so answering the type question never
     * builds a {@code FactoryBean}'s product and never creates a plain {@code @Lazy} feed bean, safe to call once
     * every singleton already exists, which is the only time a caller needing this asks.
     * <p>
     * Returns {@code null} when the type cannot be determined this way, an unnamed bean with zero or several
     * candidates, or a name or type Spring cannot resolve without creating it. A caller getting {@code null} back
     * should treat the feed's flavor as unknown rather than guess one, since {@link #resolveFeedBean} is what
     * raises the real, detailed error once the bean is actually resolved, at registration.
     *
     * @param applicationContext    the Spring context to read bean metadata from
     * @param subscriptionModelType the annotation's {@code subscriptionModel} type, or {@code Void.class} if unset
     * @param subscriptionModelName the annotation's {@code subscriptionModelName}, or blank if unset
     * @param candidateTypes        the allowed feed bean types, matching what {@link #resolveFeedBean} was called
     *                              with for the same annotation
     * @return the feed bean's type, or {@code null} when it cannot be determined this way
     */
    public static @Nullable Class<?> resolveFeedBeanType(ApplicationContext applicationContext, Class<?> subscriptionModelType,
                                                          String subscriptionModelName, Class<?>... candidateTypes) {
        if (subscriptionModelType != Void.class) {
            return subscriptionModelType;
        }
        if (!subscriptionModelName.isBlank()) {
            try {
                return applicationContext.getType(subscriptionModelName, false);
            } catch (BeansException e) {
                return null;
            }
        }
        try {
            List<String> names = candidateBeanNames(applicationContext, false, candidateTypes);
            if (names.size() != 1) {
                return null;
            }
            return applicationContext.getType(names.get(0), false);
        } catch (BeansException e) {
            return null;
        }
    }

    /**
     * A bean a catching-up push subscription needs, or a failure that names the way out. Shared by {@code @Saga} and
     * {@code @Projection} so the message is the same on both. An application whose push feed carries another
     * application's events has no event store to replay and so has neither a reader nor a checkpoint marker bean,
     * which is the application most likely to reach this, since catching up is the default. A bare
     * {@code NoSuchBeanDefinitionException} would send it looking for a missing store rather than at
     * {@code catchup = NONE}.
     */
    public static <T> T resolveCatchupBean(ApplicationContext applicationContext, String annotationName, Class<T> type, String id) {
        T bean = applicationContext.getBeanProvider(type).getIfAvailable();
        if (bean == null) {
            throw new IllegalStateException(("%s '%s' with source=PUSH catches up from the event store before going live, which needs a %s bean, and there is none. " +
                    "Set catchup = NONE if the feed carries events this application's event store does not hold, which is the case when another application writes them.").formatted(annotationName, id, type.getSimpleName()));
        }
        return bean;
    }

    /**
     * The concrete event types {@code specifiedEventType} covers, which is the type itself unless it is sealed or an
     * enum. Leaves
     * the declared type out, because a caller checks these against the handler's own parameter type, and a declared
     * supertype is not assignable to a narrower parameter its concrete types are. {@link #typesToSubscribeOn} is what
     * the filter is built from.
     */
    @SuppressWarnings("unchecked")
    private static <E> @NonNull List<Class<E>> getConcreteEventTypes(String subscriptionId, Class<E> specifiedEventType) {
        return (List<Class<E>>) (List<?>) EventTypeExpansion.concreteTypesOf(specifiedEventType,
                type -> cannotSubscribeOn(subscriptionId, type));
    }

    /**
     * The types a subscription's filter names, the declared types plus the concrete types each covers. The declared type
     * stays in, so an event stored under its own CloudEvent type still matches, which is the case for a
     * {@code CloudEventTypeMapper} that maps a hierarchy onto the type string of the type it was declared with. An extra
     * type in the filter can only widen what matches.
     */
    @SuppressWarnings("unchecked")
    private static <E> @NonNull List<Class<E>> typesToSubscribeOn(String subscriptionId, List<Class<E>> declaredTypes) {
        Set<Class<? extends E>> declared = new LinkedHashSet<>(declaredTypes);
        return (List<Class<E>>) (List<?>) List.copyOf(
                EventTypeExpansion.expand(declared, type -> cannotSubscribeOn(subscriptionId, type)));
    }

    private static IllegalArgumentException cannotSubscribeOn(String subscriptionId, Class<?> eventType) {
        if (eventType.isArray()) {
            String msg = "%s cannot be a declared event type for subscription '%s', since this expansion does not support an array. If it comes from the handler method's own event parameter, change that parameter to a concrete event type instead. If it comes from the annotation's eventTypes attribute, list concrete event types there instead.";
            return new IllegalArgumentException(msg.formatted(eventType.getTypeName(), subscriptionId));
        }
        String msg = "the concrete event types dispatch would accept for %s cannot all be enumerated, so a filter derived from it for subscription '%s' would miss some of them. Declare the concrete event types with the annotation's eventTypes attribute instead (for example eventTypes = {MyEvent1.class, MyEvent2.class}), or make %s and every level below it final or sealed.";
        return new IllegalArgumentException(msg.formatted(eventType.getName(), subscriptionId, eventType.getSimpleName()));
    }

    /**
     * Whether a registrar should wait for a subscription to start at all. Under {@link SubscriptionMode#MANUAL} nothing
     * starts until the application says so, so waiting would block until it times out no matter what
     * {@link StartupMode} the annotation asked for.
     *
     * @param applicationContext The context to read {@link OccurrentProperties} from.
     * @return {@code false} when subscriptions are not started for you.
     */
    public static boolean subscriptionsStartOnTheirOwn(ApplicationContext applicationContext) {
        return applicationContext.getBean(OccurrentProperties.class).getSubscription().resolveMode() == SubscriptionMode.AUTO;
    }

    /**
     * Decide whether a subscription should block until it has started, given whether it replays history and its
     * configured {@link StartupMode}. Shared verbatim by the blocking and reactive annotation processors, whose
     * start-position handling otherwise diverges.
     */
    public static boolean shouldWaitUntilStarted(boolean replaysHistory, StartupMode startupMode) {
        return switch (startupMode) {
            // A subscription that replays history may have a lot to read, so by default it starts in the background.
            case DEFAULT -> !replaysHistory;
            case WAIT_UNTIL_STARTED -> true;
            case BACKGROUND -> false;
        };
    }

    /**
     * The same decision for a {@code @Projection(source = PUSH)}, where only an explicit {@code BACKGROUND} keeps the
     * catch-up off the startup path.
     * <p>
     * Deliberately not {@link #shouldWaitUntilStarted(boolean, StartupMode)}, which maps {@code DEFAULT} to
     * "background if it replays history". A push catch-up always replays from the beginning, so reusing that would
     * move every existing push projection off the startup path without anyone asking for it. Shared by all four push
     * registration paths (a {@code PushSubscriptionModel} or a {@code DomainEventFeed}, on either stack) so they
     * cannot drift apart on this.
     */
    public static boolean pushCatchUpShouldWaitUntilStarted(StartupMode startupMode) {
        return startupMode != StartupMode.BACKGROUND;
    }
}
