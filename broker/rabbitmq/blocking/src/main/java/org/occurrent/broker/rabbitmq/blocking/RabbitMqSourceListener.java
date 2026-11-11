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

package org.occurrent.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.subscription.RoutingOutcome;

/**
 * Told by {@link RabbitMqCloudEventSource} and {@code RabbitMqDomainEventSource} what happened to each delivery, so
 * an application can count them, for example as metrics. Set it with the {@code listener} method on the source's
 * builder. Every method does nothing by default, so a listener implements only the ones it needs.
 * <p>
 * The source calls the listener from the thread that handles its deliveries, and {@link #redelivered(String)} from
 * the thread that polls the subscription, so two calls can run at once and a listener must be thread-safe. A listener
 * that throws is logged at {@code warn} and ignored, and the source goes on as if it had returned. A listener should
 * still return quickly, since the next delivery waits for it.
 * <p>
 * A delivery the source holds back and hands to the broker again because the subscription is not ready for it, a
 * {@link RoutingOutcome#DEFERRED} or {@link RoutingOutcome#UNAVAILABLE} outcome, is not reported, since nothing
 * failed.
 */
public interface RabbitMqSourceListener {

    /**
     * The source acknowledged a delivery its subscriber took or filtered out. Not called when the acknowledgement
     * itself threw.
     *
     * @param queue The queue the source consumes from.
     */
    default void delivered(String queue) {
    }

    /**
     * A delivery went to the source's {@link DeliveryFailurePolicy}. Called before the policy is applied, so
     * {@link #parked(String)} or {@link #redelivered(String)} follows when that works.
     *
     * @param queue   The queue the source consumes from.
     * @param failure What the inbound mapper threw, or what handling the event threw, or {@code null} for a
     *                {@link RoutingOutcome#NOT_DELIVERABLE} outcome, which the subscription decides without throwing.
     */
    default void failed(String queue, @Nullable Throwable failure) {
    }

    /**
     * The parking destination confirmed a copy of a failed delivery, and the source acknowledged the original.
     *
     * @param queue The queue the source consumes from.
     */
    default void parked(String queue) {
    }

    /**
     * The source handed a failed delivery back to the queue under {@link DeliveryFailurePolicy#REDELIVER}. A
     * delivery that could not be parked goes back to the queue too, but is not reported here, so it shows up only in
     * {@link #failed(String, Throwable)}.
     *
     * @param queue The queue the source consumes from.
     */
    default void redelivered(String queue) {
    }
}
