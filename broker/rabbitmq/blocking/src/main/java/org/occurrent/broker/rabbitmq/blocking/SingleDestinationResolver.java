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

import io.cloudevents.CloudEvent;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A {@link DestinationResolver} that resolves every event to the same fixed destination, regardless of type. Backs
 * the {@link RabbitMqCloudEventSink} a bridge's parking publish goes through
 * ({@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}), which has one destination and no routing
 * decision to make. Not a general-purpose resolver, so it stays internal to this package rather than becoming public
 * API a caller could reach for by mistake.
 */
final class SingleDestinationResolver implements DestinationResolver<RabbitMqDestination> {

    private final RabbitMqDestination destination;

    SingleDestinationResolver(RabbitMqDestination destination) {
        this.destination = requireNonNull(destination, "destination cannot be null");
    }

    @Override
    public RabbitMqDestination destinationFor(CloudEvent cloudEvent) {
        return destination;
    }

    @Override
    public Optional<Set<RabbitMqDestination>> destinationsFor(SubscriptionFilter filter) {
        return Optional.of(Set.of(destination));
    }

    @Override
    public RabbitMqDestination catchAllDestination() {
        return destination;
    }
}
