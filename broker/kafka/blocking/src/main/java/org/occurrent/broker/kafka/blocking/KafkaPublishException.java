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
 * case where the acknowledgement wait itself expired. This is the general case, everything else the Kafka client
 * reported, including its own {@code org.apache.kafka.common.errors.TimeoutException} raised while waiting for
 * topic metadata rather than while waiting for the broker's acknowledgement of a send already under way, which is
 * usually transient and is deliberately not {@link KafkaPublishTimeoutException}, so the default retry strategy
 * still retries it.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message) {
        super(message);
    }

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
