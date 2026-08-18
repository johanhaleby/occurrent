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

import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.EventDestination;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Where an event goes on RabbitMQ: an exchange, a routing key and application headers. Publishing uses every
 * component. A consumer declaring a binding uses only the exchange and the routing key, so a
 * {@link DestinationResolver#destinationsFor(org.occurrent.subscription.SubscriptionFilter)} result leaves
 * {@code headers} empty and reads {@code routingKey} as a binding pattern rather than one message's exact key.
 *
 * @param exchange   The exchange to publish to, or to bind against.
 * @param routingKey The routing key, or the binding pattern.
 * @param headers    Application headers to carry on the message, never {@code null} and empty rather than absent
 *                    when there are none. No key may start with {@value RabbitMqCloudEventMapper#HEADER_PREFIX},
 *                    the namespace {@link RabbitMqCloudEventMapper} reserves for the CloudEvent attributes
 *                    themselves, since a colliding key would silently overwrite one of them, {@code streamid}
 *                    included, without anything failing.
 */
public record RabbitMqDestination(String exchange, String routingKey, Map<String, String> headers) implements EventDestination {

    public RabbitMqDestination {
        requireNonNull(exchange, "exchange cannot be null");
        requireNonNull(routingKey, "routingKey cannot be null");
        requireNonNull(headers, "headers cannot be null");
        for (String key : headers.keySet()) {
            if (key.startsWith(RabbitMqCloudEventMapper.HEADER_PREFIX)) {
                throw new IllegalArgumentException("Header \"" + key + "\" uses the \"" + RabbitMqCloudEventMapper.HEADER_PREFIX +
                        "\" prefix, which is reserved for the CloudEvent attributes " + RabbitMqCloudEventMapper.class.getSimpleName() + " writes");
            }
        }
        // A plain copy rather than Map.copyOf, so that a caller-visible NullPointerException from a null value reads
        // the same way it would for any other unmodifiable copy, and so an empty map stays cheap to construct.
        headers = Map.copyOf(headers);
    }

    /**
     * Create a destination with no application headers.
     */
    public static RabbitMqDestination of(String exchange, String routingKey) {
        return new RabbitMqDestination(exchange, routingKey, Map.of());
    }

    /**
     * A copy of this destination with {@code headers} replacing whatever headers it already had.
     */
    public RabbitMqDestination withHeaders(Map<String, String> headers) {
        return new RabbitMqDestination(exchange, routingKey, headers);
    }
}
