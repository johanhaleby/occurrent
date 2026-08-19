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

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * A {@link KafkaCloudEventSink} publish whose acknowledgement timeout expired before the broker acknowledged it,
 * reported by {@code java.util.concurrent.TimeoutException} from the send future's {@code get}, strictly after the
 * send itself was already under way. The message may or may not have reached the broker, so this reports neither
 * success nor a known failure, only that the caller has to decide, and a caller that republishes may produce a
 * duplicate under the same at-least-once contract every consumer here already works under.
 * <p>
 * Kafka's own {@code org.apache.kafka.common.errors.TimeoutException}, raised while waiting for topic metadata
 * before a send can even be attempted, is not this. It surfaces as a plain {@link KafkaPublishException} instead,
 * since it is usually transient and the default retry strategy is meant to absorb it.
 */
public class KafkaPublishTimeoutException extends KafkaPublishException {

    public KafkaPublishTimeoutException(Duration acknowledgementTimeout, TimeoutException cause) {
        super("No broker acknowledgement was received within the configured acknowledgement timeout of " + acknowledgementTimeout, cause);
    }
}
