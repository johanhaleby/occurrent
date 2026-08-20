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
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.kafka.KafkaMessageFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Rebuilds the {@link CloudEvent} a {@link ConsumerRecord} carries, the read side a consume-side bridge needs.
 * Reuses {@code io.cloudevents:cloudevents-kafka}'s own binary reader ({@link KafkaMessageFactory#createReader(ConsumerRecord)})
 * for the header mapping itself, rather than hand-rolling it the way {@code RabbitMqCloudEventMapper} has to,
 * since {@code cloudevents-kafka} already implements the Kafka binary binding correctly for the general case and
 * this module's own {@link KafkaCloudEventSink} already writes through the matching writer half of the same
 * library. Two defects in what that reader alone produces are corrected here, both verified by decompiling the
 * exact {@code cloudevents-kafka} and {@code cloudevents-core} versions this project pins (4.0.1), not assumed by
 * analogy with the RabbitMQ mapping.
 * <p>
 * <strong>Every context attribute and extension the reader produces is a {@link String}.</strong>
 * {@code BaseGenericBinaryMessageReaderImpl.read(...)}, the shared base class {@code KafkaBinaryMessageReaderImpl}
 * builds on, calls {@code visitor.withContextAttribute(name, toCloudEventsValue(value))} for every header, and
 * {@code toCloudEventsValue} always returns a {@code String}. This is the same defect ADR 133's amendment fixed for
 * {@code RabbitMqCloudEventMapper}. {@code streamversion} and {@code position} are the two extensions
 * {@link OccurrentCloudEventExtension} itself defines as {@code Long}, so this mapper restores that type on them
 * after {@code toEvent()} runs, the same two names and the same reasoning as the RabbitMQ mapping. Every other
 * extension, application-defined ones included, stays a {@code String}, since this mapper has no way to know what
 * type it should be.
 * <p>
 * <strong>The reader also flattens data that is present but empty to no data at all.</strong>
 * {@code KafkaBinaryMessageReaderImpl}'s constructor builds a {@code null} {@code CloudEventData} whenever the
 * record value is {@code null} <em>or</em> has length zero, so a record whose value is a genuine zero-length
 * {@code byte[]}, distinct on the wire from a {@code null} value, still comes back through {@code toEvent()} as
 * absent data. ADR 133's second amendment already establishes that a Kafka record's value natively tells the two
 * states apart, present-but-empty and absent, without needing a marker header the way AMQP does, but that claim
 * only holds if something actually reads {@link ConsumerRecord#value()} to use the distinction, and the library's
 * own {@code toEvent()} does not. This mapper does: when the record's raw value is non-null and empty, and the
 * event {@code toEvent()} built has no data, it overrides the data with an explicit empty {@link BytesCloudEventData}
 * instead of leaving it absent.
 * <p>
 * Public because {@code KafkaDomainEventBridge} lives in a sub-package of this one and needs it too, the same
 * reasoning {@code RabbitMqCloudEventMapper} and {@code RabbitMqTopology} already state for themselves.
 */
public final class KafkaCloudEventMapper {

    /**
     * The extensions {@link #toCloudEvent(ConsumerRecord)} rebuilds as a {@code Long} rather than leaving a
     * {@link String}, since {@link OccurrentCloudEventExtension} defines both as one.
     */
    private static final Set<String> NUMERIC_OCCURRENT_EXTENSIONS = Set.of(
            OccurrentCloudEventExtension.STREAM_VERSION, OccurrentCloudEventExtension.POSITION);

    private KafkaCloudEventMapper() {
    }

    /**
     * Rebuilds the {@link CloudEvent} {@code record} was written from by {@link KafkaCloudEventSink} (or any other
     * writer using {@code cloudevents-kafka}'s binary mode). See this class's own javadoc for the two corrections
     * applied on top of {@code cloudevents-kafka}'s own reader: {@code streamversion} and {@code position} come
     * back {@code Long}, and data that was present but empty comes back that way rather than as no data at all.
     * Every other context attribute and extension is a {@link String}, exactly what {@code cloudevents-kafka}'s
     * reader itself produces.
     *
     * @throws RuntimeException whatever {@code cloudevents-kafka}'s reader throws for a record it cannot parse,
     *                           an unrecognised spec version or a malformed header among them.
     */
    public static CloudEvent toCloudEvent(ConsumerRecord<String, byte[]> record) {
        requireNonNull(record, "record cannot be null");
        CloudEvent event = KafkaMessageFactory.createReader(record).toEvent();

        CloudEventBuilder builder = CloudEventBuilder.from(event);
        boolean changed = false;
        for (String extensionName : NUMERIC_OCCURRENT_EXTENSIONS) {
            Object value = event.getExtension(extensionName);
            if (value != null) {
                builder.withContextAttribute(extensionName, Long.parseLong(value.toString()));
                changed = true;
            }
        }
        byte[] rawValue = record.value();
        if (rawValue != null && rawValue.length == 0 && event.getData() == null) {
            builder.withData(BytesCloudEventData.wrap(rawValue));
            changed = true;
        }
        return changed ? builder.build() : event;
    }
}
