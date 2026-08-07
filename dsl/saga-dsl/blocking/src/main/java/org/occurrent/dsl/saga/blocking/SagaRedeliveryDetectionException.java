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
 * Thrown when a saga receives an event carrying neither a stream id with a stream version nor a position, and its
 * {@link RedeliveryDetection} is {@link RedeliveryDetection#REQUIRED}. Without one of those the saga cannot tell a
 * redelivered event from a new one, so reacting to it would run the reaction again and issue its commands again on
 * every redelivery.
 * <p>
 * It propagates to the subscription model rather than being logged, so the event is not acknowledged and the feed
 * offers it again. The fix is to forward the Occurrent CloudEvent extensions from the listener feeding the saga. A
 * feed that genuinely carries none of them, together with reactions that are idempotent, is what
 * {@link RedeliveryDetection#BEST_EFFORT} is for.
 */
public class SagaRedeliveryDetectionException extends RuntimeException {
    public SagaRedeliveryDetectionException(String message) {
        super(message);
    }
}
