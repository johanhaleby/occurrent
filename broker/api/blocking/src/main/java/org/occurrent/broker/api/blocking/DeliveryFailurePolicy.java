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

package org.occurrent.broker.api.blocking;

/**
 * What a consume-side bridge does after a failed delivery, shared across transports rather than declared as two
 * enums that happen to line up. The actual promise, regardless of this setting: a bridge never acknowledges a
 * message without that message being durable somewhere else first. Under {@link #PARK}, that somewhere else is the
 * parking destination, so a parked message <strong>is</strong> acknowledged out of the source queue or topic, once
 * the republish to parking is confirmed, even though nothing ever consumed it.
 */
public enum DeliveryFailurePolicy {

    /**
     * Redeliver the message for another attempt. The default, because it is the choice that cannot lose a message
     * on a transient failure. A handler that fails on every attempt stays in a redelivery loop until an operator
     * intervenes, which is the tradeoff for never losing one on a failure that would have cleared on retry.
     */
    REDELIVER,

    /**
     * Route the message to a holding destination nobody consumes from, so an operator can look at what failed
     * without it looping forever. Choosing this requires a parking destination of the transport's own
     * {@link EventDestination} type. A bridge configured with {@code PARK} and no parking destination refuses to
     * start.
     */
    PARK
}
