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
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
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
        List<HandlerParameter> parameters = new ArrayList<>();
        boolean hasEvent = false;
        boolean hasMetadata = false;
        boolean hasStreamId = false;
        boolean hasStreamVersion = false;
        for (Parameter parameter : method.getParameters()) {
            Class<?> type = parameter.getType();
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
     * @param bean                          the bean declaring the handler
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
        List<HandlerParameter> parameters = analyzeParameters(method, SubscriptionAnnotations::isStreamMetadataParameter, true);
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
     * A descriptor factory runs exactly once at startup to build a value; there is no request for advice to usefully
     * observe, so invoking it through a proxy is a hazard rather than a feature. A CGLIB proxy happens to survive it,
     * since the proxy subclasses the target and {@code method} (found by scanning the bean's declared or predicted
     * type) is inherited on it either way. A JDK interface proxy does not survive it: for a bean not yet a singleton
     * when its type was scanned (a {@code @Lazy} bean, most commonly), the scan can predict the concrete class the
     * bean declares before that bean is ever proxied, and the proxy created afterwards implements only its interfaces,
     * not that class, so invoking a method declared on it throws {@link IllegalArgumentException}. Unwrapping to the
     * target and re-resolving {@code method} against the target's own class first makes both proxy kinds behave the
     * same way when the proxy has a fixed singleton target: the factory runs once, directly, with no advice and no
     * exception. A proxy backed by a prototype- or pool-scoped target source is left proxied by {@link #ultimateTarget},
     * so a JDK interface proxy of that kind still cannot run the factory, and fails with a clear message instead of a
     * bare reflection error.
     *
     * @param annotationName the annotation name for error messages, for example {@code "@Projection"}
     * @param bean           the (possibly proxied) bean the factory method was found on
     * @param method         the no-argument factory method found by scanning {@code bean}'s declared or predicted type
     * @return the descriptor the factory method returned, never {@code null}
     */
    public static Object invokeDescriptorFactory(String annotationName, Object bean, Method method) {
        Object target = ultimateTarget(bean);
        Method targetMethod = method;
        if (target != bean) {
            targetMethod = ReflectionUtils.findMethod(target.getClass(), method.getName());
            if (targetMethod == null) {
                throw new IllegalStateException("%s factory %s#%s could not be resolved on the unwrapped proxy target %s.".formatted(annotationName, bean.getClass().getName(), method.getName(), target.getClass().getName()));
            }
        } else if (!targetMethod.getDeclaringClass().isInstance(target)) {
            // ultimateTarget leaves a proxy alone when its TargetSource is not a fixed singleton (prototype- or
            // pool-scoped), so a JDK interface proxy backed by one still cannot run a factory declared on the
            // concrete class. Without this check, Method.invoke fails with a bare "object is not an instance of
            // declaring class", naming neither the annotation nor a way out.
            throw new IllegalStateException("%s factory %s#%s cannot run: %s does not implement %s, and its target is not a fixed singleton, so it cannot be unwrapped safely. Set spring.aop.proxy-target-class=true so this bean is proxied by subclassing instead of by interface, or move the factory method off the advised bean.".formatted(annotationName, bean.getClass().getName(), method.getName(), bean.getClass().getName(), targetMethod.getDeclaringClass().getName()));
        }
        try {
            targetMethod.setAccessible(true);
            Object result = targetMethod.invoke(target);
            if (result == null) {
                throw new IllegalStateException("%s factory %s#%s returned null.".formatted(annotationName, target.getClass().getName(), method.getName()));
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to invoke %s factory %s#%s.".formatted(annotationName, target.getClass().getName(), method.getName()), e);
        }
    }

    // Unwraps through any number of nested AOP proxies to the innermost fixed target (AopProxyUtils.getSingletonTarget
    // stops at one layer, hence the loop). Returns bean itself when it is not a proxy, or when a proxy's TargetSource
    // is not a fixed singleton (a prototype- or pool-backed source is left proxied rather than risking a
    // side-effecting getTarget() call, or invoking a different target instance than the one the descriptor id was
    // registered against).
    private static Object ultimateTarget(Object bean) {
        Object current = bean;
        Object next;
        while ((next = AopProxyUtils.getSingletonTarget(current)) != null) {
            current = next;
        }
        return current;
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
