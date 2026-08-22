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

package org.occurrent.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The binding-derivation rule both bridge builders apply, per ADR 133 decision 5: explicit {@code bindings} if
 * given, otherwise {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for {@code bindingFilter} if
 * given, falling back to {@link DestinationResolver#catchAllDestination()} whenever the resolver cannot narrow it,
 * or {@code bindingFilter} was never given at all. Shared rather than written once per bridge, since both bridges'
 * builders offer exactly the same three ways to declare a topology. Public because the domain bridge lives in a
 * sub-package of this one.
 */
public final class RabbitMqTopology {

    private RabbitMqTopology() {
    }

    public static Set<RabbitMqDestination> destinationsToBind(@Nullable DestinationResolver<RabbitMqDestination> resolver,
                                                                @Nullable SubscriptionFilter bindingFilter,
                                                                @Nullable Set<RabbitMqDestination> bindings) {
        if (bindings != null) {
            if (bindings.isEmpty()) {
                throw new IllegalStateException("An explicit bindings(Set.of()) declares zero bindings, so the " +
                        "queue is created and bound to nothing, receiving no events while still reporting " +
                        "healthy. Use declareTopology(false) if a platform team owns the queue and its bindings " +
                        "already, or pass a non-empty bindings(...) set.");
            }
            return bindings;
        }
        DestinationResolver<RabbitMqDestination> nonNullResolver = requireNonNull(resolver,
                "A resolver, or explicit bindings(...), is required unless declareTopology(false) is set");
        if (bindingFilter != null) {
            return nonNullResolver.destinationsFor(bindingFilter).orElseGet(() -> Set.of(nonNullResolver.catchAllDestination()));
        }
        return Set.of(nonNullResolver.catchAllDestination());
    }
}
