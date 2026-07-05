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
import org.occurrent.annotation.StreamSubscription;
import org.occurrent.annotation.StreamSubscription.ResumeBehavior;
import org.occurrent.annotation.StreamSubscription.StartPosition;
import org.occurrent.annotation.StreamSubscription.StartupMode;
import org.occurrent.annotation.Subscription;
import org.occurrent.dsl.dcb.DcbEventMetadata;
import org.occurrent.dsl.subscription.EventMetadata;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static java.util.function.Predicate.not;

/**
 * Stack-neutral helpers for processing the {@link StreamSubscription} and {@link DcbSubscription} annotations, and the
 * deprecated {@link Subscription} alias. Shared by the blocking and reactive annotation {@code BeanPostProcessor}s so the
 * reflection and event-type resolution logic lives in one place and cannot drift. The stack-specific parts (how the
 * consumer is invoked, how the start position is resolved, and which subscription DSL is used) stay in each processor.
 */
public final class SubscriptionAnnotations {

    private SubscriptionAnnotations() {
    }

    /**
     * The normalized form of a stream subscription declaration, built from either the {@link StreamSubscription}
     * annotation or the deprecated {@link Subscription} alias. The deprecated annotation's enums are mapped to the
     * canonical {@link StreamSubscription} enums by name, since the constants are identical.
     */
    public record StreamSubscriptionDefinition(String id, Class<?>[] eventTypes, String startAtISO8601,
                                               long startAtTimeEpochMillis, StartPosition startAt,
                                               ResumeBehavior resumeBehavior, StartupMode startupMode, String annotationName) {

        public static StreamSubscriptionDefinition from(StreamSubscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), subscription.startAt(), subscription.resumeBehavior(), subscription.startupMode(), "@StreamSubscription");
        }

        @SuppressWarnings("deprecation")
        public static StreamSubscriptionDefinition from(Subscription subscription) {
            return new StreamSubscriptionDefinition(subscription.id(), subscription.eventTypes(), subscription.startAtISO8601(),
                    subscription.startAtTimeEpochMillis(), StartPosition.valueOf(subscription.startAt().name()),
                    ResumeBehavior.valueOf(subscription.resumeBehavior().name()), StartupMode.valueOf(subscription.startupMode().name()), "@Subscription");
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
