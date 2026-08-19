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

package org.occurrent.broker.api.blocking;

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

/**
 * Derives the set of cloud event types a {@link SubscriptionFilter} narrows to, the one part of a filter a
 * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} implementation can see. Named only
 * {@link SubscriptionFilter} and {@link Filter}, both stack-neutral and transport-neutral, so it lives here rather
 * than in either transport module, and both {@code RabbitMqTopicExchangeDestinationResolver} and
 * {@code KafkaTopicPerTypeDestinationResolver} call it instead of each walking the filter tree on its own. What
 * this decides is which events a consumer's binding lets through, so two copies of the walk drifting apart would
 * be a correctness divergence between the transports, not untidiness, which is exactly what ADR 133 decision 7
 * exists to rule out between them.
 * <p>
 * Works for {@link AgnosticSubscriptionFilter} and {@link StreamSubscriptionFilter}, both of which wrap a plain
 * {@link Filter}, and only for the part of that {@link Filter} that constrains {@value Filter#TYPE} by equality or
 * membership. Anything else, a {@link org.occurrent.subscription.DcbSubscriptionFilter}, a custom
 * {@link SubscriptionFilter}, a {@link Filter} on a different field, an {@code OR} branch that leaves one
 * alternative unconstrained, a range or negation condition on {@value Filter#TYPE}, resolves to
 * {@link Optional#empty()} rather than a guess, exactly as
 * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} requires of its own return value.
 */
public final class EventTypeNarrowing {

    private EventTypeNarrowing() {
    }

    /**
     * The cloud event types {@code filter} narrows to, or {@link Optional#empty()} when it cannot be narrowed.
     * Never returns an {@link Optional} holding an empty {@link Set}, empty and absent mean different things here,
     * absent means "could not narrow, bind everything", empty would wrongly mean "narrows to nothing".
     */
    public static Optional<Set<String>> narrow(SubscriptionFilter subscriptionFilter) {
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
