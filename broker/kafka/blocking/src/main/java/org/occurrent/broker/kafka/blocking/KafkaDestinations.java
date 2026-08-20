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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.regex.Pattern;

/**
 * What {@link KafkaTopicPerTypeDestinationResolver} and {@link KafkaSharedTopicDestinationResolver} both need to
 * turn a {@link CloudEvent} into a {@link KafkaDestination}, kept here once rather than in each resolver, the same
 * discipline {@code EventTypeNarrowing} applies to the filter-tree walk both resolvers also share.
 */
final class KafkaDestinations {

    private KafkaDestinations() {
    }

    /**
     * Kafka's own rule for a legal topic name, {@code [a-zA-Z0-9._-]}. Not exposed through the client's public API,
     * so this is stated independently rather than depending on Kafka's {@code internals} package.
     */
    private static final Pattern LEGAL_TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]+");

    /**
     * Kafka's own limit on a topic name's length.
     */
    static final int MAX_TOPIC_NAME_LENGTH = 249;

    /**
     * Whether {@code topic} is a name Kafka itself would accept, checked independently rather than left for a
     * broker round trip to discover. Each caller refuses an illegal name with its own message, since one resolver
     * derives it from a cloud event type and the other takes it directly from a caller, two different things to
     * say about the same failure.
     */
    static boolean isLegalTopicName(String topic) {
        return !topic.isEmpty() && !topic.equals(".") && !topic.equals("..")
                && topic.length() <= MAX_TOPIC_NAME_LENGTH && LEGAL_TOPIC_NAME.matcher(topic).matches();
    }

    /**
     * The event's {@code streamid} extension, or {@code null} when it has none. Read directly rather than through
     * {@code OccurrentExtensionGetter.getStreamId}, which throws when the extension is absent instead of answering
     * {@code null}, and an event published through {@code DomainEventSink.publish(E)} is documented to carry no
     * stream identity at all.
     */
    static @Nullable String streamIdOf(CloudEvent cloudEvent) {
        if (!cloudEvent.getExtensionNames().contains(OccurrentCloudEventExtension.STREAM_ID)) {
            return null;
        }
        Object streamId = cloudEvent.getExtension(OccurrentCloudEventExtension.STREAM_ID);
        return streamId == null ? null : streamId.toString();
    }
}
