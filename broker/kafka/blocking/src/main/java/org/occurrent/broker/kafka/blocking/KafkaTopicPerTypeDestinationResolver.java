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
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The shipped {@link DestinationResolver} for Kafka: one topic per cloud event type, the topic name derived from
 * {@code topicPrefix} plus the type through a {@link CloudEventTypeMapper}, the same mapper an application already
 * uses to convert between a domain class and its cloud event type. Give it the exact mapper instance backing your
 * {@code CloudEventConverter}, so a publisher and a consumer agree by reading one mapping rather than by matching
 * two hand written strings.
 * <p>
 * <b>The message key, and the ordering tradeoff it carries.</b> {@link #destinationFor(CloudEvent)} keys every
 * message by the event's {@code streamid} extension when the event has one, and leaves the key {@code null}
 * otherwise, since an event published through {@code DomainEventSink.publish(E)} has no stream identity at all to
 * key by. This is not a default to accept without reading what it costs. Kafka only orders messages within one
 * partition, so keying by stream id puts every event of one stream on the same partition and therefore in order
 * relative to each other, at the cost of that one stream's throughput being capped by whatever one partition can
 * do, and of every other stream sharing the topic being ordered only within itself, never against it. The
 * alternative this resolver actually offers by falling back to it, a {@code null} key, spreads records across
 * every partition instead and lets the topic's full partition count carry the throughput, at the cost of giving up
 * ordering entirely, even within one stream. A single fixed key used for every message is neither alternative and
 * is not what a caller wanting spread should reach for. Kafka hashes one key to exactly one partition, so a fixed
 * key sends every message to that same partition, trading the whole topic's throughput away for a global order
 * across every stream, which is a narrower guarantee than stream-id keying already gives and a worse throughput
 * cost than either alternative above. Occurrent picks stream-id keying as the shipped default because a
 * projection or saga reading one stream is the common case this library is built around, but this is a topology
 * choice your deployment makes, not a fact about Kafka, and an application that wants a different tradeoff
 * replaces this resolver with one of its own.
 * <p>
 * Both {@link #destinationFor(CloudEvent)} and {@link #destinationsFor(SubscriptionFilter)} round-trip the cloud
 * event type through {@code topicMapper}, {@code getCloudEventType(getDomainEventType(type))}, rather than trusting
 * the string on the event or in the filter as-is. A type the mapper does not recognise makes that round trip throw
 * whatever {@code topicMapper} throws for it, which is a configuration bug the caller should see immediately rather
 * than a signal to fall back on some default routing.
 */
public final class KafkaTopicPerTypeDestinationResolver implements DestinationResolver<KafkaDestination> {

    /**
     * Kafka's own rule for a legal topic name, {@code [a-zA-Z0-9._-]}. Not exposed through the client's public API,
     * so this resolver states it independently rather than depending on Kafka's {@code internals} package, and
     * refuses a derived name that breaks it rather than silently truncating or rewriting a name a caller's
     * {@code CloudEventTypeMapper} chose.
     */
    private static final Pattern LEGAL_TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]+");

    /**
     * Kafka's own limit on a topic name's length.
     */
    private static final int MAX_TOPIC_NAME_LENGTH = 249;

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
     * Works for {@link AgnosticSubscriptionFilter} and {@link StreamSubscriptionFilter}, both of which wrap a plain
     * {@link Filter}, and only for the part of that {@link Filter} that constrains {@value Filter#TYPE} by equality
     * or membership. Anything else, an {@link org.occurrent.subscription.DcbSubscriptionFilter}, a custom
     * {@link SubscriptionFilter}, a {@link Filter} on a different field, an {@code OR} branch that leaves one
     * alternative unconstrained, a range or negation condition on {@value Filter#TYPE}, resolves to
     * {@link Optional#empty()} rather than a guess, exactly as {@link DestinationResolver#destinationsFor(SubscriptionFilter)}
     * requires.
     */
    @Override
    public Optional<Set<KafkaDestination>> destinationsFor(SubscriptionFilter filter) {
        requireNonNull(filter, "filter cannot be null");
        return typesFrom(filter).map(types -> types.stream()
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
        if (topic.isEmpty() || topic.equals(".") || topic.equals("..")
                || topic.length() > MAX_TOPIC_NAME_LENGTH || !LEGAL_TOPIC_NAME.matcher(topic).matches()) {
            throw new IllegalArgumentException("Cloud event type \"" + cloudEventType + "\" resolved to topic \"" +
                    topic + "\", which is not a legal Kafka topic name. A topic name must be 1-" + MAX_TOPIC_NAME_LENGTH +
                    " characters, may only contain letters, digits, '.', '_' and '-', and may not be \".\" or \"..\". " +
                    "A nested or inner domain event class is a common cause, since Class#getName() writes its " +
                    "enclosing class separator as '$'.");
        }
    }

    /**
     * The event's {@code streamid} extension, or {@code null} when it has none. Read directly rather than through
     * {@code OccurrentExtensionGetter.getStreamId}, which throws when the extension is absent instead of answering
     * {@code null}, and an event published through {@code DomainEventSink.publish(E)} is documented to carry no
     * stream identity at all.
     */
    private static @Nullable String streamIdOf(CloudEvent cloudEvent) {
        if (!cloudEvent.getExtensionNames().contains(OccurrentCloudEventExtension.STREAM_ID)) {
            return null;
        }
        Object streamId = cloudEvent.getExtension(OccurrentCloudEventExtension.STREAM_ID);
        return streamId == null ? null : streamId.toString();
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
