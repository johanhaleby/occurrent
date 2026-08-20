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

/**
 * A {@link KafkaCloudEventSink} publish that did not succeed. {@link KafkaPublishTimeoutException} is the specific
 * case where the publish could not be confirmed within the configured acknowledgement timeout, whether that is
 * because the broker never acknowledged an already-sent record or because the producer could not even enqueue the
 * send in time. This is everything else the Kafka client reported for that one attempt rather than merely timed
 * out on, a record too large, an invalid topic name, an authorization failure the broker rejected the send for,
 * any other failure {@code send()} raised synchronously or the acknowledgement future carried asynchronously, or
 * the sending or waiting thread being interrupted. Kafka has no channel or connection abstraction the way
 * RabbitMQ does, so unlike {@code RabbitMqPublishException} this is not a transport-level failure specifically,
 * only whatever the client itself decided to report the attempt as.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message) {
        super(message);
    }

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
