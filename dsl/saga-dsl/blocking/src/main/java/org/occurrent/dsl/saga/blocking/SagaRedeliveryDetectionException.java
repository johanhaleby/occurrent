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

package org.occurrent.dsl.saga.blocking;

/**
 * Thrown when a saga cannot tell whether an event is a redelivery. That is the case for an event without both a stream
 * id and a stream version when its {@link RedeliveryDetection} is {@link RedeliveryDetection#REQUIRED}, and for an
 * event whose stream id or stream version extension cannot be read, whatever the setting. Without a readable stream key
 * the saga cannot tell a redelivered event from a new one, so reacting to it would run the reaction again and issue its
 * commands again on every redelivery. A global position is not a substitute, because events can reach a saga in a
 * different order than their positions.
 * <p>
 * It propagates to the subscription model rather than being logged, and whether the feed offers the event again is
 * that model's own business. A push feed lets the listener that handed the event over decide, and on a broker
 * source {@code DeliveryFailurePolicy} is where the choice is configured. The instance is not quarantined, because
 * with no key there is nothing to record a failure against.
 * <p>
 * For a missing stream key the fix is to forward the Occurrent CloudEvent extensions from the listener feeding the
 * saga. A feed that genuinely carries none of them, together with reactions that are idempotent, is what
 * {@link RedeliveryDetection#BEST_EFFORT} is for. For an unreadable one the fix is in the feed that writes it.
 */
public class SagaRedeliveryDetectionException extends RuntimeException {
    public SagaRedeliveryDetectionException(String message) {
        super(message);
    }

    public SagaRedeliveryDetectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
