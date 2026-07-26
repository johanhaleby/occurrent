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

package org.occurrent.springboot.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves the reactive event store that can be read in position order, which is what reactive history replay needs.
 * <p>
 * This is public because it is a seam between this module and a store starter, not an incidental convenience. Whether
 * replay is possible is decided here, in {@code StartPositionSupport}, and whether a catch-up subscription model is
 * layered in is decided by the store starter's auto-configuration. Those two answers must never disagree, since a
 * subscription that is told replay is supported but runs on a model that cannot replay silently skips history instead
 * of failing at startup. One resolution used by both is what keeps them from drifting apart.
 */
public final class PositionOrderedEventStores {

    private PositionOrderedEventStores() {
    }

    /**
     * The event store to ask about position replay, or {@code null} when the context has none that can be read in
     * position order. Asks the event store rather than any reader that happens to be in the context, since other beans
     * (an external feed, for example) can read in position order without being the store.
     *
     * @throws IllegalStateException when several event stores can be read in position order and none is
     *                               {@code @Primary}, rather than picking one in registration order.
     */
    public static @Nullable PositionOrderedReader find(ApplicationContext applicationContext) {
        // Narrowed by bean name first so that only an event store is ever instantiated, and never some unrelated
        // PositionOrderedReader. The reader test is on the instance, because a bean declared as EventStore hides
        // whether the implementation behind it also reads in position order.
        Map<String, PositionOrderedReader> candidates = new LinkedHashMap<>();
        for (String name : applicationContext.getBeanNamesForType(EventStore.class)) {
            if (applicationContext.getBean(name) instanceof PositionOrderedReader reader) {
                candidates.put(name, reader);
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }
        if (candidates.size() == 1) {
            return candidates.values().iterator().next();
        }
        // Several: let the container apply its own @Primary resolution before giving up, so an application that has
        // deliberately marked one store as primary keeps working.
        try {
            return applicationContext.getBean(EventStore.class) instanceof PositionOrderedReader reader ? reader : null;
        } catch (NoUniqueBeanDefinitionException e) {
            throw new IllegalStateException(("Found %d event store beans that can be read in position order (%s) and cannot pick one to replay history from. " +
                    "Declare a single reactive EventStore bean, or mark one @Primary.").formatted(candidates.size(), String.join(", ", candidates.keySet())), e);
        }
    }
}
