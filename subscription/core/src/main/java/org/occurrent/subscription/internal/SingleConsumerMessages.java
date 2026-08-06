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

package org.occurrent.subscription.internal;

/**
 * The wording shared verbatim by every push sink that takes a single consumer, kept in one place so the four call
 * sites (the blocking and reactor push subscription models, and the blocking and reactor domain-event feeds) cannot
 * drift. See ADR 90 for why the sinks are single-consumer.
 * <p>
 * This message is the migration path for the change, so it has to carry the whole story. There is no OpenRewrite
 * recipe for a bean topology, and a startup failure that names both consumers is the most useful thing this class can
 * offer instead.
 */
public final class SingleConsumerMessages {

    private SingleConsumerMessages() {
    }

    /**
     * Rejects a second consumer on a sink that already has one.
     *
     * @param sinkType     The sink's simple type name, e.g. {@code "PushSubscriptionModel"}.
     * @param consumerNoun What the sink feeds, e.g. {@code "subscription"} or {@code "projection"}.
     * @param registeredId The consumer already registered, named so the reader knows which two collided.
     * @param attemptedId  The consumer that was refused.
     */
    public static String singleConsumerOnly(String sinkType, String consumerNoun, String registeredId, String attemptedId) {
        return ("This %s already feeds %s '%s', so '%s' was refused: a push sink feeds exactly one consumer. "
                + "Declare one sink per projection or saga, each fed by its own queue. A shared sink carries one "
                + "acknowledgement for several consumers, so one consumer that keeps failing holds up every consumer "
                + "behind it, and they lose the message entirely once the broker gives up on it.")
                .formatted(sinkType, consumerNoun, registeredId, attemptedId);
    }
}
