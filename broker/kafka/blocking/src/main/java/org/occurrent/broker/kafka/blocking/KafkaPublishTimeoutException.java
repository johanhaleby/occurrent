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
 * Three distinct failures report this, and they are not equally uncertain about whether the record reached a
 * broker, even though this type reports them all the same way.
 * <p>
 * In the synchronous {@code send()} case, {@code send} itself throws Kafka's own
 * {@code org.apache.kafka.common.errors.TimeoutException} before returning a future, most often because the
 * producer needed a fresh view of the cluster and could not get one in time.
 * {@link KafkaCloudEventSink.Builder#build()} forces {@code max.block.ms} down to this same acknowledgement
 * timeout so this cannot silently run past it, on a broker that was reachable when this producer last used it and
 * has since gone away, for example. The record was never accepted by the producer here, so it never reached any
 * broker, and republishing after this specific case cannot create a duplicate on its own.
 * <p>
 * In the wrapped-future case, that same {@code org.apache.kafka.common.errors.TimeoutException} is caught as
 * the cause of an {@code ExecutionException} from a future {@code send} did return. The record was accepted and
 * its delivery then ran out of time, so it may already be on the broker.
 * <p>
 * In the acknowledgement-wait case, the send future's own {@code java.util.concurrent.TimeoutException} is what
 * expires, when the configured acknowledgement timeout itself runs out on a send still in flight. The record may
 * also already be on the broker here.
 * <p>
 * Only the synchronous {@code send()} case is known to have never reached a broker, and nothing here lets a caller
 * tell it apart from the wrapped-future case. Both carry the identical
 * {@code org.apache.kafka.common.errors.TimeoutException} as their cause, so this exception's cause alone cannot
 * distinguish them. Treat every occurrence of this exception as the uncertain case. The record may already be on
 * the broker, and republishing may produce a duplicate under the same at-least-once contract every consumer here
 * already works under.
 */
public class KafkaPublishTimeoutException extends KafkaPublishException {

    public KafkaPublishTimeoutException(Duration acknowledgementTimeout, Exception cause) {
        super("No broker acknowledgement was received within the configured acknowledgement timeout of " + acknowledgementTimeout, cause);
    }
}
