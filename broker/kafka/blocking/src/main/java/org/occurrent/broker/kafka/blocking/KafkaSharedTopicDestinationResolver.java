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

import io.cloudevents.CloudEvent;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.MAX_TOPIC_NAME_LENGTH;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.isLegalTopicName;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.streamIdOf;

/**
 * The shipped default {@link DestinationResolver} for Kafka. Every event goes to one topic you name, keyed by the
 * event's {@code streamid} extension when it has one and {@code null} otherwise, exactly the keying
 * {@link KafkaTopicPerTypeDestinationResolver} also uses. An event with no {@code streamid}, exactly what
 * {@code DomainEventSink.publish(E)} produces per decision 4, gets no ordering guarantee at all. Kafka's own
 * partitioner spreads a {@code null} key across every partition on its own, so that event's place relative to any
 * other is given up rather than kept. One topic is what makes stream-id keying actually deliver the guarantee ADR
 * 133 decision 7 states for it. A projection or saga reading one stream needs that stream's
 * events in order, and Kafka only orders within one partition of one topic. Two events of the same stream but
 * different types share this topic and therefore the same partition, so they stay in order against each other the
 * way an event-sourced stream that mixes types actually needs, which a per-type topic can never deliver since two
 * types never share a topic to begin with. That guarantee also assumes the producer actually partitions by the
 * record key, which Kafka's default partitioner does, unless {@code producerConfig} sets
 * {@code partitioner.ignore.keys} to {@code true}, in which case every key including this resolver's is ignored
 * and {@link KafkaCloudEventSink.Builder#build()} warns about exactly that. It also assumes the topic's partition
 * count stays put. Kafka hashes a key against the topic's current partition count, so increasing that count
 * between two sends for the same stream id can remap it onto a different partition and silently break that
 * stream's ordering from that point on. Choose the partition count before producing to this topic, and grow it
 * later only once losing cross-partition order for whatever streams are still in flight at that moment is
 * acceptable. {@link KafkaTopicPerTypeDestinationResolver} is the documented alternative, for a deployment that
 * wants per-type topics for retention or independent consumer scaling and either has single-type streams or
 * accepts that narrower guarantee.
 * <p>
 * The constructor takes the topic name. Nothing here invents one, the same reasoning ADR 133 decision 7 already
 * gives for refusing a parking bridge with no {@code parkingDestination} of its own. A default destination name is
 * precisely the thing an operator has to know, not something a library should guess on their behalf.
 * <p>
 * {@link #destinationsFor(SubscriptionFilter)} returns this one topic regardless of what {@code filter} asks for.
 * With a single topic, narrowing has nothing left to do, every consumer binds to the same place either way, so the
 * feed stays the sole decider of which events a subscription actually receives, ADR 133 decision 5 working exactly
 * as designed rather than a gap this resolver needs to fill. That is also why this resolver has no use for
 * {@code EventTypeNarrowing}, the shared filter-tree walk {@link KafkaTopicPerTypeDestinationResolver} and
 * {@code RabbitMqTopicExchangeDestinationResolver} both still need to turn a filter into one topic per type. A
 * single destination makes that walk pointless here rather than merely unused, so this resolver does not import it.
 */
public final class KafkaSharedTopicDestinationResolver implements DestinationResolver<KafkaDestination> {

    private final String topic;

    /**
     * @param topic Every event this resolver derives a destination for publishes to this one topic, and every
     *              consumer binds to it too. Refused at construction if it is not a legal Kafka topic name, rather
     *              than left for a broker round trip to discover.
     */
    public KafkaSharedTopicDestinationResolver(String topic) {
        requireNonNull(topic, "topic cannot be null");
        if (!isLegalTopicName(topic)) {
            throw new IllegalArgumentException("\"" + topic + "\" is not a legal Kafka topic name. A topic name " +
                    "must be 1-" + MAX_TOPIC_NAME_LENGTH + " characters, may only contain letters, digits, '.', " +
                    "'_' and '-', and may not be \".\" or \"..\".");
        }
        this.topic = topic;
    }

    @Override
    public KafkaDestination destinationFor(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        return KafkaDestination.of(topic, streamIdOf(cloudEvent));
    }

    /**
     * The one topic this resolver ever derives a destination for, regardless of {@code filter}. Never
     * {@link Optional#empty()}, since with a single topic there is nothing left to narrow, the feed remains the
     * decider of which events a subscription actually receives either way.
     */
    @Override
    public Optional<Set<KafkaDestination>> destinationsFor(SubscriptionFilter filter) {
        requireNonNull(filter, "filter cannot be null");
        return Optional.of(Set.of(KafkaDestination.of(topic)));
    }

    /**
     * The same one topic {@link #destinationFor(CloudEvent)} and {@link #destinationsFor(SubscriptionFilter)}
     * already return, since under this resolver it is also the destination that receives every event.
     */
    @Override
    public KafkaDestination catchAllDestination() {
        return KafkaDestination.of(topic);
    }
}
