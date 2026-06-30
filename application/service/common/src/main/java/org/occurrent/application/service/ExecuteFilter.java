/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.application.service;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.StreamReadFilter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * An application-service-level stream read filter that resolves domain event classes to CloudEvent types
 * through a {@link CloudEventTypeGetter} at execution time.
 * <p>
 * This type exists to keep {@link StreamReadFilter} independent from application service concerns while
 * still allowing fluent filters based on domain event classes such as {@code type(MyEvent.class)}. It is shared
 * by both the blocking and the reactive application services.
 *
 * @param <E> The application service event type.
 */
@FunctionalInterface
@NullMarked
public interface ExecuteFilter<E> {

    /**
     * Resolve this filter into a concrete {@link StreamReadFilter} using the supplied {@link CloudEventTypeGetter}.
     *
     * @param cloudEventTypeGetter Resolves a domain event class to a CloudEvent type.
     * @return A concrete stream read filter.
     */
    StreamReadFilter resolve(CloudEventTypeGetter<? super E> cloudEventTypeGetter);

    /**
     * Adapt an already constructed {@link StreamReadFilter} to an {@link ExecuteFilter}.
     */
    static <E> ExecuteFilter<E> from(StreamReadFilter filter) {
        Objects.requireNonNull(filter, "filter cannot be null");
        return __ -> filter;
    }

    /**
     * Create a filter that includes events of the supplied domain event type.
     */
    static <E> ExecuteFilter<E> type(Class<? extends E> eventType) {
        Objects.requireNonNull(eventType, "eventType cannot be null");
        return cloudEventTypeGetter -> StreamReadFilter.type(cloudEventTypeGetter.getCloudEventType(eventType));
    }

    /**
     * Create a filter that includes events whose CloudEvent type matches any of the supplied domain event types.
     */
    @SafeVarargs
    static <E> ExecuteFilter<E> includeTypes(Class<? extends E> first, Class<? extends E>... more) {
        return cloudEventTypeGetter -> StreamReadFilter.type(Condition.in(resolveCloudEventTypes(cloudEventTypeGetter, first, more)));
    }

    /**
     * Create a filter that excludes events whose CloudEvent type matches any of the supplied domain event types.
     */
    @SafeVarargs
    static <E> ExecuteFilter<E> excludeTypes(Class<? extends E> first, Class<? extends E>... more) {
        return cloudEventTypeGetter -> StreamReadFilter.type(Condition.not(Condition.in(resolveCloudEventTypes(cloudEventTypeGetter, first, more))));
    }

    @SafeVarargs
    private static <E> List<String> resolveCloudEventTypes(CloudEventTypeGetter<? super E> cloudEventTypeGetter, Class<? extends E> first, Class<? extends E>... more) {
        Objects.requireNonNull(cloudEventTypeGetter, "cloudEventTypeGetter cannot be null");
        Objects.requireNonNull(first, "first event type cannot be null");
        Objects.requireNonNull(more, "additional event types cannot be null");

        return Stream.concat(Stream.of(first), Arrays.stream(more))
                .peek(eventType -> Objects.requireNonNull(eventType, "eventType cannot be null"))
                .map(cloudEventTypeGetter::getCloudEventType)
                .toList();
    }
}
