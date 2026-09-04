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

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.EventDestination;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The topic, message key and application headers describing where an event goes on Kafka. Publishing uses every
 * component. A
 * consumer declaring a binding uses only the topic, so a
 * {@link DestinationResolver#destinationsFor(org.occurrent.filter.Filter)} result leaves
 * {@code key} and {@code headers} empty, the same convention {@code RabbitMqDestination} follows for its own
 * per-message components.
 * <p>
 * {@code topic} carries a dual meaning depending on which method produced it. {@link DestinationResolver#destinationFor(io.cloudevents.CloudEvent)}
 * and {@link DestinationResolver#destinationsFor(org.occurrent.filter.Filter)} always return a
 * literal topic name on both shipped resolvers. {@link KafkaTopicPerTypeDestinationResolver#catchAllDestination()}
 * is the one shipped case where {@code topic} is not a literal name but a Kafka topic-matching pattern instead,
 * meant for {@code KafkaConsumer.subscribe(java.util.regex.Pattern)} rather than for publishing, exactly as
 * {@code RabbitMqDestination}'s {@code routingKey} already doubles as a binding pattern when it comes back from
 * {@code destinationsFor}. {@link #topicIsPattern()} is the discriminator a consumer reads to tell the two apart
 * without guessing from the string's shape, which a legal literal topic name (itself allowed to contain
 * {@code .}, a regex metacharacter) cannot support. {@code false} on every destination {@link #of(String)} and
 * {@link #of(String, String)} produce. Only {@link #ofPattern(String)} sets it to {@code true}, and only
 * {@code KafkaTopicPerTypeDestinationResolver}'s catch-all uses that factory.
 *
 * @param topic          The topic to publish to, or the topic name or pattern to bind against. See
 *                       {@link #topicIsPattern()} for which one this is.
 * @param key            The message key, or {@code null} for none. Both shipped resolvers, {@code KafkaSharedTopicDestinationResolver}
 *                       and {@code KafkaTopicPerTypeDestinationResolver}, key by the event's stream id and leave
 *                       this {@code null} when the event has none, which is why the component is nullable rather
 *                       than defaulted to some other value.
 * @param headers        Application headers to carry on the message, never {@code null} and empty rather than
 *                       absent when there are none. No key may equal {@value #CONTENT_TYPE_HEADER} or start with
 *                       {@value #HEADER_PREFIX}, the namespace the CloudEvents Kafka binary binding
 *                       ({@code io.cloudevents:cloudevents-kafka}) reserves for the CloudEvent attributes
 *                       themselves, since a colliding key would silently overwrite one of them, {@code streamid}
 *                       included, without anything failing.
 * @param topicIsPattern Whether {@code topic} is a Kafka topic-matching pattern rather than a literal topic name.
 *                       {@code false} for every destination produced by {@link #of(String)} or
 *                       {@link #of(String, String)}. {@code true} only from {@link #ofPattern(String)}.
 *                       {@link #withHeaders(Map)} carries whatever value the destination it copies already had,
 *                       neither factory. A consumer built on this destination subscribes by pattern when this is
 *                       {@code true} and by literal topic name otherwise, never guessing from {@code topic}'s own
 *                       content.
 */
public record KafkaDestination(String topic, @Nullable String key, Map<String, String> headers,
                                boolean topicIsPattern) implements EventDestination {

    /**
     * The header namespace the CloudEvents Kafka binary binding writes every CloudEvent attribute under, except
     * {@code datacontenttype} which becomes {@value #CONTENT_TYPE_HEADER} instead. Reserved here so an application
     * header cannot collide with either and silently overwrite one of them. Unlike {@code RabbitMqCloudEventMapper},
     * this module owns no mapping of its own, since {@code cloudevents-kafka}'s binary message writer already
     * produces these headers, so this constant only mirrors the namespace that writer uses rather than defining it.
     */
    public static final String HEADER_PREFIX = "ce_";

    /**
     * The one CloudEvent attribute the CloudEvents Kafka binary binding writes outside {@value #HEADER_PREFIX}, as
     * an unprefixed header, mirroring {@code datacontenttype}'s own {@code content-type} placement in the AMQP and
     * HTTP bindings.
     */
    public static final String CONTENT_TYPE_HEADER = "content-type";

    public KafkaDestination {
        requireNonNull(topic, "topic cannot be null");
        requireNonNull(headers, "headers cannot be null");
        for (String headerKey : headers.keySet()) {
            if (headerKey.startsWith(HEADER_PREFIX) || CONTENT_TYPE_HEADER.equals(headerKey)) {
                throw new IllegalArgumentException("Header \"" + headerKey + "\" uses the \"" + HEADER_PREFIX +
                        "\" prefix or the \"" + CONTENT_TYPE_HEADER + "\" name, both reserved for the CloudEvent " +
                        "attributes the CloudEvents Kafka binary binding writes");
            }
        }
        headers = Map.copyOf(headers);
    }

    /**
     * Create a destination with no message key and no application headers. {@link #topicIsPattern()} is
     * {@code false}, so {@code topic} is a literal topic name.
     */
    public static KafkaDestination of(String topic) {
        return new KafkaDestination(topic, null, Map.of(), false);
    }

    /**
     * Create a destination with no application headers. {@link #topicIsPattern()} is {@code false}, so {@code topic}
     * is a literal topic name.
     */
    public static KafkaDestination of(String topic, @Nullable String key) {
        return new KafkaDestination(topic, key, Map.of(), false);
    }

    /**
     * Create a destination whose {@code topic} is a Kafka topic-matching pattern rather than a literal topic name,
     * meant for {@code KafkaConsumer.subscribe(java.util.regex.Pattern)} and never for publishing. No message key
     * or application headers, since a pattern-typed destination is never used to publish a message.
     * {@link #topicIsPattern()} is {@code true}.
     */
    public static KafkaDestination ofPattern(String pattern) {
        return new KafkaDestination(pattern, null, Map.of(), true);
    }

    /**
     * A copy of this destination with {@code headers} replacing whatever headers it already had.
     * {@link #topicIsPattern()} carries over unchanged.
     */
    public KafkaDestination withHeaders(Map<String, String> headers) {
        return new KafkaDestination(topic, key, headers, topicIsPattern);
    }
}
