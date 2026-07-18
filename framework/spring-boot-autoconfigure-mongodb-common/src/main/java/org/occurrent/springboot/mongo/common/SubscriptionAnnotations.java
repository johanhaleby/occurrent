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

package org.occurrent.springboot.mongo.common;

import org.jspecify.annotations.NonNull;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

import static java.util.function.Predicate.not;
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

    public static List<Class<?>> analyzeParameters(Method method, Predicate<Class<?>> isMetadataParameter) {
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

    public static Class<?> eventTypeOf(List<Class<?>> parameterTypes, Predicate<Class<?>> isMetadataParameter) {
        return parameterTypes.stream().filter(not(isMetadataParameter)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("You need to declare an event type"));
    }

    @SuppressWarnings("unchecked")
    public static <E> List<Class<E>> resolveDomainEventTypes(String id, Object bean, Method method, Class<E> specifiedEventType, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName) {
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

    /**
     * The event parameter types of a subscription handler plus the {@link Filter} that selects the events it
     * subscribes to, resolved together from the annotated method.
     */
    public record ResolvedTypeFilter(List<Class<?>> parameterTypes, Filter filter) {
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
     * @return the handler's parameter types and the type filter
     */
    public static <E> ResolvedTypeFilter resolveTypeFilter(String id, Object bean, Method method, Class<?>[] eventTypesSpecifiedInAnnotation, String annotationName, CloudEventConverter<E> cloudEventConverter) {
        if (method.getParameterCount() < 1) {
            throw new IllegalArgumentException("A subscription method must declare an event parameter, but %s#%s has none.".formatted(bean.getClass().getName(), method.getName()));
        }
        List<Class<?>> parameterTypes = analyzeParameters(method, SubscriptionAnnotations::isStreamMetadataParameter);
        @SuppressWarnings("unchecked")
        Class<E> specifiedEventType = (Class<E>) eventTypeOf(parameterTypes, SubscriptionAnnotations::isStreamMetadataParameter);
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
        return new ResolvedTypeFilter(parameterTypes, filter);
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

    public static Object[] bindArguments(List<Class<?>> parameterTypes, Object event, Object metadata, Predicate<Class<?>> isMetadataParameter) {
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
     * Resolve the push feed bean of a {@code source = PUSH} projection, selected by {@code subscriptionModelType}
     * (the annotation's {@code subscriptionModel}) or {@code subscriptionModelName}, or the unique bean of one of
     * {@code candidateTypes} when neither is set. Shared by the blocking and reactive processors so the resolution rules
     * and error messages live in one place and cannot drift. The caller branches on the returned bean's runtime type.
     *
     * @param applicationContext    the Spring context to resolve beans from
     * @param subscriptionModelType the annotation's {@code subscriptionModel} type, or {@code Void.class} if unset
     * @param subscriptionModelName the annotation's {@code subscriptionModelName}, or blank if unset
     * @param id                    the projection id, for error messages
     * @param candidateTypes        the allowed feed bean types (the stack's PushSubscriptionModel and DomainEventFeed)
     * @return the resolved feed bean
     */
    public static Object resolveFeedBean(ApplicationContext applicationContext, Class<?> subscriptionModelType,
                                         String subscriptionModelName, String id, Class<?>... candidateTypes) {
        boolean byType = subscriptionModelType != Void.class;
        boolean byName = !subscriptionModelName.isBlank();
        if (byType && Arrays.stream(candidateTypes).noneMatch(candidate -> candidate.isAssignableFrom(subscriptionModelType))) {
            throw new IllegalArgumentException("@Projection '%s' subscriptionModel type %s must be a PushSubscriptionModel or a DomainEventFeed for source=PUSH.".formatted(id, subscriptionModelType.getName()));
        }
        try {
            if (byName) {
                return byType ? applicationContext.getBean(subscriptionModelName, subscriptionModelType) : applicationContext.getBean(subscriptionModelName);
            }
            if (byType) {
                return applicationContext.getBean(subscriptionModelType);
            }
            List<String> names = new ArrayList<>();
            for (Class<?> candidateType : candidateTypes) {
                Collections.addAll(names, applicationContext.getBeanNamesForType(candidateType));
            }
            if (names.isEmpty()) {
                throw new IllegalStateException("@Projection '%s' with source=PUSH found no PushSubscriptionModel or DomainEventFeed bean. Declare one, or name it with subscriptionModelName.".formatted(id));
            }
            if (names.size() > 1) {
                throw new IllegalStateException("@Projection '%s' with source=PUSH found several push feed beans (%s). Pick one with subscriptionModel or subscriptionModelName.".formatted(id, String.join(", ", names)));
            }
            return applicationContext.getBean(names.get(0));
        } catch (BeansException e) {
            throw new IllegalArgumentException("@Projection '%s' with source=PUSH could not resolve a push feed bean (subscriptionModel=%s, subscriptionModelName='%s'): %s".formatted(id, byType ? subscriptionModelType.getName() : "unset", subscriptionModelName, e.getMessage()), e);
        }
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
