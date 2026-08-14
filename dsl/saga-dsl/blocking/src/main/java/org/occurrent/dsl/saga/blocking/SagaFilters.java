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

package org.occurrent.dsl.saga.blocking;

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.filter.Filter;

import java.util.List;

/**
 * Works out the plain {@link Filter} a {@link Saga} subscribes on. It starts from the saga's
 * {@link Saga#replacementFilter() replacementFilter} when it has one, and otherwise derives a type filter over its
 * handled event types, resolved to CloudEvent type strings, or {@link Filter#all()} when it declares none. A
 * {@link Saga#narrowingFilter() narrowingFilter} is then combined with that.
 */
final class SagaFilters {

    private SagaFilters() {
    }

    static <E> Filter filterFor(CloudEventConverter<E> cloudEventConverter, Saga<E, ?, ?> saga) {
        Filter replacement = saga.replacementFilter();
        Filter base = replacement != null ? replacement : derivedFrom(cloudEventConverter, saga);
        Filter narrowing = saga.narrowingFilter();
        if (narrowing == null || narrowing instanceof Filter.All) {
            return base;
        }
        if (base instanceof Filter.All) {
            return narrowing;
        }
        // The narrowing goes on the right so the cheaper type conditions are evaluated first. An AND is walked left to
        // right and stops at the first mismatch, so a Filter.data(..) narrowing is not read for an event whose type
        // already ruled it out.
        return base.and(narrowing);
    }

    private static <E> Filter derivedFrom(CloudEventConverter<E> cloudEventConverter, Saga<E, ?, ?> saga) {
        List<Condition<String>> typeConditions = saga.eventTypes().stream()
                .map(type -> Condition.eq(cloudEventConverter.getCloudEventType(type)))
                .toList();
        return switch (typeConditions.size()) {
            case 0 -> Filter.all();
            case 1 -> Filter.type(typeConditions.getFirst());
            default -> Filter.type(Condition.or(typeConditions));
        };
    }
}
