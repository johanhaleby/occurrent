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

package org.occurrent.dsl.saga.internal;

import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.addAll;

/**
 * Resolves a value registered per event type, falling back through superclasses and then interfaces (nearest superclass
 * first), and caching the resolved lookup per concrete class. The saga descriptor uses it for its evolve, react and
 * correlation maps. It mirrors the projection DSL's per-event-type handler dispatch, but the two are not yet unified.
 *
 * @param <V> the registered value type
 */
public final class TypeDispatch<V> {
    private static final Object NONE = new Object();

    private final Map<Class<?>, V> registrations;
    private final Map<Class<?>, Object> resolved = new ConcurrentHashMap<>();

    public TypeDispatch(Map<Class<?>, V> registrations) {
        this.registrations = new LinkedHashMap<>(registrations);
    }

    /** The value registered for {@code type} or its nearest supertype, or {@code null} if none is registered. */
    @SuppressWarnings("unchecked")
    public @Nullable V resolve(Class<?> type) {
        Object cached = resolved.computeIfAbsent(type, this::lookup);
        return cached == NONE ? null : (V) cached;
    }

    private Object lookup(Class<?> eventClass) {
        for (Class<?> c = eventClass; c != null; c = c.getSuperclass()) {
            V value = registrations.get(c);
            if (value != null) {
                return value;
            }
        }
        Deque<Class<?>> queue = new ArrayDeque<>();
        for (Class<?> c = eventClass; c != null; c = c.getSuperclass()) {
            addAll(queue, c.getInterfaces());
        }
        Set<Class<?>> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            Class<?> anInterface = queue.poll();
            if (!visited.add(anInterface)) {
                continue;
            }
            V value = registrations.get(anInterface);
            if (value != null) {
                return value;
            }
            addAll(queue, anInterface.getInterfaces());
        }
        return NONE;
    }
}
