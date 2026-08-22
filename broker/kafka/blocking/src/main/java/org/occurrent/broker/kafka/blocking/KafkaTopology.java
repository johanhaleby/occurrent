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

package org.occurrent.broker.kafka.blocking;

import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The topic-derivation rule both bridge builders apply, per ADR 133 decision 5. Explicit {@code topics} if given,
 * otherwise {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for {@code bindingFilter} if given,
 * falling back to {@link DestinationResolver#catchAllDestination()} whenever the resolver cannot narrow it, or
 * {@code bindingFilter} was never given at all. Shared rather than written once per bridge, the same reasoning
 * {@code RabbitMqTopology} gives for itself. Public because the domain bridge lives in a sub-package of this one.
 * <p>
 * Unlike {@code RabbitMqTopology}, this does not declare anything on a broker, since a Kafka bridge never creates a
 * topic. It only decides what to subscribe to. {@link #subscribe(Consumer, Set)} is the other half. It reads
 * {@link KafkaDestination#topicIsPattern()} across the resolved set to decide between
 * {@code Consumer.subscribe(Collection)} and {@code Consumer.subscribe(Pattern)}, the discriminator PR 862 deferred
 * to this unit's own plan gate.
 */
public final class KafkaTopology {

    private KafkaTopology() {
    }

    public static Set<KafkaDestination> topicsToSubscribe(@Nullable DestinationResolver<KafkaDestination> resolver,
                                                            @Nullable SubscriptionFilter bindingFilter,
                                                            @Nullable Set<KafkaDestination> topics) {
        if (topics != null) {
            if (topics.isEmpty()) {
                throw new IllegalStateException("An explicit bindings(Set.of()) would subscribe to zero topics. " +
                        "Kafka's consumer.subscribe(Set.of()) is an unsubscribe, not \"bind everything,\" so this " +
                        "is refused instead, rather than building a bridge that stops consuming while still " +
                        "reporting healthy. Pass a non-empty bindings(...) set, or omit bindings() so a resolver " +
                        "or the catch-all destination applies.");
            }
            return topics;
        }
        DestinationResolver<KafkaDestination> nonNullResolver = requireNonNull(resolver,
                "A resolver, or explicit bindings(...), is required");
        if (bindingFilter != null) {
            return nonNullResolver.destinationsFor(bindingFilter).orElseGet(() -> Set.of(nonNullResolver.catchAllDestination()));
        }
        return Set.of(nonNullResolver.catchAllDestination());
    }

    /**
     * Subscribes {@code consumer} to {@code destinations}, by literal topic name when every destination's
     * {@link KafkaDestination#topicIsPattern()} is {@code false}, or by {@link Pattern} when exactly one
     * destination is pattern-typed. {@code destinations} is homogeneous by construction whenever it comes from
     * {@link #topicsToSubscribe(DestinationResolver, SubscriptionFilter, Set)}. {@code destinationsFor(...)} never
     * returns a pattern-typed destination on either shipped resolver, and {@code catchAllDestination()} is always
     * exactly one destination. A set mixing literal and pattern-typed destinations is only reachable through a
     * misused explicit {@code bindings(...)} escape hatch, and is refused here rather than guessed at.
     */
    public static void subscribe(Consumer<String, byte[]> consumer, Set<KafkaDestination> destinations) {
        long patternCount = destinations.stream().filter(KafkaDestination::topicIsPattern).count();
        if (patternCount == 0) {
            consumer.subscribe(destinations.stream().map(KafkaDestination::topic).collect(Collectors.toUnmodifiableSet()));
        } else if (patternCount == 1 && destinations.size() == 1) {
            consumer.subscribe(Pattern.compile(destinations.iterator().next().topic()));
        } else {
            throw new IllegalStateException("The resolved topics mix pattern-typed and literal destinations (" +
                    destinations + "), which cannot be subscribed to as one Kafka subscription. This only happens " +
                    "through an explicit bindings(...) call mixing KafkaDestination.of(...) and " +
                    "KafkaDestination.ofPattern(...) destinations; use one or the other, not both.");
        }
    }
}
