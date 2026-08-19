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

/**
 * A {@link KafkaCloudEventSink} publish that could not be confirmed within the configured acknowledgement timeout.
 * Two distinct failures both report this, since both leave the caller with the same decision to make. The broker
 * never acknowledged a record the producer had already sent, {@code java.util.concurrent.TimeoutException} from the
 * send future's {@code get}. Or the producer could not even hand the record to a broker within that same timeout,
 * most often because it needed a fresh view of the cluster and could not get one in time,
 * {@code org.apache.kafka.common.errors.TimeoutException} from {@code send} itself or wrapped in an
 * {@code ExecutionException} from the future. {@link KafkaCloudEventSink.Builder#build()} forces Kafka's own
 * {@code max.block.ms} down to this same acknowledgement timeout precisely so the second failure cannot silently
 * run past it, on a broker that was reachable when it was last used and has since gone away for example, where
 * Kafka's own default lets that wait run to a full minute regardless of what this sink was configured with.
 * <p>
 * Either way, the message may or may not have reached the broker, so this reports neither success nor a known
 * failure, only that the caller has to decide, and a caller that republishes may produce a duplicate under the same
 * at-least-once contract every consumer here already works under.
 */
public class KafkaPublishTimeoutException extends KafkaPublishException {

    public KafkaPublishTimeoutException(Duration acknowledgementTimeout, Exception cause) {
        super("No broker acknowledgement was received within the configured acknowledgement timeout of " + acknowledgementTimeout, cause);
    }
}
