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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.EventTypeNarrowing;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The shipped {@link DestinationResolver} for RabbitMQ: a single topic exchange, with the routing key derived from
 * the cloud event type through a {@link CloudEventTypeMapper}, the same mapper an application already uses to
 * convert between a domain class and its cloud event type. Give it the exact mapper instance backing your
 * {@code CloudEventConverter}, so a publisher and a consumer agree by reading one mapping rather than by matching
 * two hand written strings.
 * <p>
 * Both {@link #destinationFor(CloudEvent)} and {@link #destinationsFor(SubscriptionFilter)} round-trip the cloud
 * event type through {@code typeMapper}, {@code getCloudEventType(getDomainEventType(type))}, rather than trusting
 * the string on the event or in the filter as-is. A type the mapper does not recognise makes that round trip throw
 * whatever {@code typeMapper} throws for it, which is a configuration bug the caller should see immediately rather
 * than a signal to fall back on some default routing.
 */
public final class RabbitMqTopicExchangeDestinationResolver implements DestinationResolver<RabbitMqDestination> {

    /**
     * The AMQP topic-exchange binding pattern that matches every routing key, so {@link #catchAllDestination()} can
     * bind to it without narrowing anything.
     */
    private static final String CATCH_ALL_ROUTING_KEY = "#";

    private final String exchange;
    private final CloudEventTypeMapper<?> typeMapper;

    /**
     * @param exchange   The topic exchange every destination this resolver derives publishes to, or binds against.
     * @param typeMapper The mapper that derives a routing key from a cloud event type, ideally the same instance
     *                   backing your {@code CloudEventConverter}.
     */
    public RabbitMqTopicExchangeDestinationResolver(String exchange, CloudEventTypeMapper<?> typeMapper) {
        this.exchange = requireNonNull(exchange, "exchange cannot be null");
        this.typeMapper = requireNonNull(typeMapper, CloudEventTypeMapper.class.getSimpleName() + " cannot be null");
    }

    @Override
    public RabbitMqDestination destinationFor(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        return RabbitMqDestination.of(exchange, canonicalRoutingKey(cloudEvent.getType()));
    }

    /**
     * The event-type narrowing {@link EventTypeNarrowing#narrow(SubscriptionFilter)} derives, one routing key per
     * type it finds, or {@link Optional#empty()} when {@code filter} cannot be narrowed, exactly as
     * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} requires.
     */
    @Override
    public Optional<Set<RabbitMqDestination>> destinationsFor(SubscriptionFilter filter) {
        requireNonNull(filter, "filter cannot be null");
        return EventTypeNarrowing.narrow(filter).map(types -> types.stream()
                .map(this::canonicalRoutingKey)
                .map(routingKey -> RabbitMqDestination.of(exchange, routingKey))
                .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public RabbitMqDestination catchAllDestination() {
        return RabbitMqDestination.of(exchange, CATCH_ALL_ROUTING_KEY);
    }

    private <T> String canonicalRoutingKey(String cloudEventType) {
        @SuppressWarnings("unchecked")
        CloudEventTypeMapper<T> mapper = (CloudEventTypeMapper<T>) typeMapper;
        Class<T> domainEventType = mapper.<T>getDomainEventType(cloudEventType);
        return mapper.getCloudEventType(domainEventType);
    }
}
