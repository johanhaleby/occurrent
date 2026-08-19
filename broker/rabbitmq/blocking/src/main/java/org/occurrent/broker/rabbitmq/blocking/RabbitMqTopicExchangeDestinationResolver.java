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
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.HashSet;
import java.util.List;
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
     * Works for {@link AgnosticSubscriptionFilter} and {@link StreamSubscriptionFilter}, both of which wrap a plain
     * {@link Filter}, and only for the part of that {@link Filter} that constrains {@value Filter#TYPE} by equality
     * or membership. Anything else, an {@link org.occurrent.subscription.DcbSubscriptionFilter}, a custom
     * {@link SubscriptionFilter}, a {@link Filter} on a different field, an {@code OR} branch that leaves one
     * alternative unconstrained, a range or negation condition on {@value Filter#TYPE}, resolves to
     * {@link Optional#empty()} rather than a guess, exactly as {@link DestinationResolver#destinationsFor(SubscriptionFilter)}
     * requires.
     */
    @Override
    public Optional<Set<RabbitMqDestination>> destinationsFor(SubscriptionFilter filter) {
        requireNonNull(filter, "filter cannot be null");
        return typesFrom(filter).map(types -> types.stream()
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

    // ---------------------------------------------------------------------------------------------------------
    // Filter-tree walk: the event-type part of a SubscriptionFilter, and nothing else.
    // ---------------------------------------------------------------------------------------------------------

    private static Optional<Set<String>> typesFrom(SubscriptionFilter subscriptionFilter) {
        return switch (subscriptionFilter) {
            case AgnosticSubscriptionFilter(Filter filter) -> typesIn(filter);
            case StreamSubscriptionFilter(Filter filter) -> typesIn(filter);
            default -> Optional.empty();
        };
    }

    private static Optional<Set<String>> typesIn(Filter filter) {
        return switch (filter) {
            case Filter.SingleConditionFilter(String fieldName, Condition<?> condition) when Filter.TYPE.equals(fieldName) -> valuesIn(condition);
            case Filter.SingleConditionFilter ignored -> Optional.empty();
            case Filter.CompositionFilter(Filter.CompositionOperator operator, List<Filter> filters) -> switch (operator) {
                case AND -> intersectWhatNarrows(filters);
                case OR -> unionOnlyIfEveryBranchResolves(filters);
            };
            case Filter.All ignored -> Optional.empty();
            case Filter.CapabilityFilter ignored -> Optional.empty();
        };
    }

    private static Optional<Set<String>> valuesIn(Condition<?> condition) {
        return switch (condition) {
            case Condition.SingleOperandCondition(var name, var operand, var ignored) when name == Condition.SingleOperandConditionName.EQ ->
                    Optional.of(Set.of(operand.toString()));
            case Condition.InOperandCondition(var operand, var ignored) ->
                    Optional.of(operand.stream().map(Object::toString).collect(Collectors.toUnmodifiableSet()));
            default -> Optional.empty();
        };
    }

    /**
     * An {@code AND} is narrower than any single one of its conjuncts, so the intersection of whichever conjuncts
     * resolve is still a safe (over-inclusive at worst) binding set. Resolves to {@link Optional#empty()} only when
     * none of the conjuncts constrain {@value Filter#TYPE} at all.
     */
    private static Optional<Set<String>> intersectWhatNarrows(List<Filter> filters) {
        Set<String> intersection = null;
        for (Filter filter : filters) {
            Optional<Set<String>> resolved = typesIn(filter);
            if (resolved.isPresent()) {
                if (intersection == null) {
                    intersection = new HashSet<>(resolved.get());
                } else {
                    intersection.retainAll(resolved.get());
                }
            }
        }
        return intersection == null ? Optional.empty() : Optional.of(Set.copyOf(intersection));
    }

    /**
     * An {@code OR} only narrows when every branch does, since an unconstrained branch could match a type none of
     * the other branches' destinations would carry.
     */
    private static Optional<Set<String>> unionOnlyIfEveryBranchResolves(List<Filter> filters) {
        Set<String> union = new HashSet<>();
        for (Filter filter : filters) {
            Optional<Set<String>> resolved = typesIn(filter);
            if (resolved.isEmpty()) {
                return Optional.empty();
            }
            union.addAll(resolved.get());
        }
        return Optional.of(Set.copyOf(union));
    }
}
