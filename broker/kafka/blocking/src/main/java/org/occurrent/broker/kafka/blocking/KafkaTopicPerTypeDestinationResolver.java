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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.EventTypeNarrowing;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.MAX_TOPIC_NAME_LENGTH;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.isLegalTopicName;
import static org.occurrent.broker.kafka.blocking.KafkaDestinations.streamIdOf;

/**
 * The opt-in per-type {@link DestinationResolver} for Kafka, one topic per cloud event type, the topic name derived
 * from {@code topicPrefix} plus the type through a {@link CloudEventTypeMapper}, the same mapper an application
 * already uses to convert between a domain class and its cloud event type. {@link KafkaSharedTopicDestinationResolver}
 * is the shipped default. Reach for this one instead when per-type topics buy something a shared topic cannot,
 * retention tuned per type or independent consumer scaling per type, and either your streams carry a single event
 * type each or you accept the narrower ordering guarantee below. Give it the exact mapper instance backing your
 * {@code CloudEventConverter}, so a publisher and a consumer agree by reading one mapping rather than by matching
 * two hand written strings.
 * <p>
 * <b>The message key, and the ordering tradeoff it carries.</b> {@link #destinationFor(CloudEvent)} keys every
 * message by the event's {@code streamid} extension when the event has one, and leaves the key {@code null}
 * otherwise, since an event published through {@code DomainEventSink.publish(E)} has no stream identity at all to
 * key by. This is not a default to accept without reading what it costs. Kafka only orders messages within one
 * partition of one topic, and this resolver puts every cloud event type on its own topic, so stream-id keying
 * orders one stream's events of one type against each other, at the cost of that stream-and-type pair's throughput
 * being capped by whatever one partition can do, and of every other stream sharing the topic being ordered only
 * within itself, never against it. It does not order a stream's events across types, since two types of the same
 * stream go to two different topics and were never on the same partition to begin with. A projection or saga
 * reading a stream that carries only one event type gets full ordering this way. One reading a stream that mixes
 * several types does not, and has to tolerate or otherwise account for that, since this resolver's topology cannot
 * deliver it, which is exactly why {@link KafkaSharedTopicDestinationResolver} is the default instead. The
 * alternative this resolver actually offers by falling back to it, a {@code null} key, spreads records across
 * every partition instead and lets the topic's full partition count carry the throughput, at the cost of giving up
 * ordering entirely, even within one stream and type. A single fixed key used for every message is neither
 * alternative and is not what a caller wanting spread should reach for. Kafka hashes one key to exactly one
 * partition, so a fixed key sends every message on one topic, meaning every stream's events of that one type, to
 * the same partition, trading that topic's throughput away for order across every stream of that one type, still
 * not across types.
 * <p>
 * Both {@link #destinationFor(CloudEvent)} and {@link #destinationsFor(SubscriptionFilter)} round-trip the cloud
 * event type through {@code topicMapper}, {@code getCloudEventType(getDomainEventType(type))}, rather than trusting
 * the string on the event or in the filter as-is. A type the mapper does not recognise makes that round trip throw
 * whatever {@code topicMapper} throws for it, which is a configuration bug the caller should see immediately rather
 * than a signal to fall back on some default routing.
 */
public final class KafkaTopicPerTypeDestinationResolver implements DestinationResolver<KafkaDestination> {

    private final String topicPrefix;
    private final CloudEventTypeMapper<?> topicMapper;

    /**
     * @param topicPrefix Every topic this resolver derives is {@code topicPrefix} followed by a cloud event type
     *                    read through {@code topicMapper}, refused at derivation time if the result is not a legal
     *                    Kafka topic name. {@link #catchAllDestination()} returns a topic pattern matching every
     *                    topic this prefix produces, meant for {@code KafkaConsumer.subscribe(java.util.regex.Pattern)}
     *                    rather than for publishing.
     * @param topicMapper The mapper that derives a topic suffix from a cloud event type, ideally the same instance
     *                    backing your {@code CloudEventConverter}. {@code org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper.qualified()}
     *                    is refused for a nested or inner class, since {@link Class#getName()} writes its enclosing
     *                    class separator as {@code $}, a character Kafka does not allow in a topic name.
     *                    {@code ReflectionCloudEventTypeMapper.simple(...)} does not have that problem, since a
     *                    class's simple name carries no such separator.
     */
    public KafkaTopicPerTypeDestinationResolver(String topicPrefix, CloudEventTypeMapper<?> topicMapper) {
        this.topicPrefix = requireNonNull(topicPrefix, "topicPrefix cannot be null");
        this.topicMapper = requireNonNull(topicMapper, CloudEventTypeMapper.class.getSimpleName() + " cannot be null");
    }

    @Override
    public KafkaDestination destinationFor(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        return KafkaDestination.of(canonicalTopic(cloudEvent.getType()), streamIdOf(cloudEvent));
    }

    /**
     * The event-type narrowing {@link EventTypeNarrowing#narrow(SubscriptionFilter)} derives, one topic per type it
     * finds, or {@link Optional#empty()} when {@code filter} cannot be narrowed, exactly as
     * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} requires.
     */
    @Override
    public Optional<Set<KafkaDestination>> destinationsFor(SubscriptionFilter filter) {
        requireNonNull(filter, "filter cannot be null");
        return EventTypeNarrowing.narrow(filter).map(types -> types.stream()
                .map(this::canonicalTopic)
                .map(KafkaDestination::of)
                .collect(Collectors.toUnmodifiableSet()));
    }

    /**
     * A Kafka topic-matching pattern, {@code topicPrefix} followed by {@code .*}, covering every topic this
     * resolver's type-per-topic mapping could ever derive. Meant for
     * {@code KafkaConsumer.subscribe(java.util.regex.Pattern)}, not for publishing, since there is no one topic
     * that receives every event under a topic-per-type mapping.
     */
    @Override
    public KafkaDestination catchAllDestination() {
        return KafkaDestination.of(Pattern.quote(topicPrefix) + ".*");
    }

    private <T> String canonicalTopic(String cloudEventType) {
        @SuppressWarnings("unchecked")
        CloudEventTypeMapper<T> mapper = (CloudEventTypeMapper<T>) topicMapper;
        Class<T> domainEventType = mapper.<T>getDomainEventType(cloudEventType);
        String topic = topicPrefix + mapper.getCloudEventType(domainEventType);
        requireLegalTopicName(topic, cloudEventType);
        return topic;
    }

    /**
     * Refuses a {@code topic} Kafka itself would reject, rather than letting a caller discover it later at
     * publish time or, worse, at a consumer's bind time. Deliberately a refusal and not a sanitizing rewrite,
     * since a rewrite that maps two different cloud event types onto the same topic name would silently break the
     * one topic per type this resolver promises, and would still need reversing symmetrically by whatever consumes
     * this same mapping later.
     */
    private static void requireLegalTopicName(String topic, String cloudEventType) {
        if (!isLegalTopicName(topic)) {
            throw new IllegalArgumentException("Cloud event type \"" + cloudEventType + "\" resolved to topic \"" +
                    topic + "\", which is not a legal Kafka topic name. A topic name must be 1-" + MAX_TOPIC_NAME_LENGTH +
                    " characters, may only contain letters, digits, '.', '_' and '-', and may not be \".\" or \"..\". " +
                    "A nested or inner domain event class is a common cause, since Class#getName() writes its " +
                    "enclosing class separator as '$'.");
        }
    }
}
