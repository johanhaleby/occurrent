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
 * The source calls the listener from the thread that handles its deliveries, from the thread that polls the
 * subscription, and from the thread that calls {@code close()}, so two calls can run at once and a listener must be
 * thread-safe. A listener that throws is logged at {@code warn} and ignored, and the source goes on as if it had
 * returned. That covers an {@link Error} too, such as the {@link LinkageError} a listener throws when it cannot link
 * against its metrics library, but not a {@link VirtualMachineError}, such as an {@link OutOfMemoryError}, which the
 * source does not catch. A listener should still return quickly.
 * The next delivery waits for it, and {@link #parked(String)} and {@link #redelivered(String)} are called while the
 * source holds the lock its poll and {@code close()} also take.
 * <p>
 * A delivery the source holds back and hands to the broker again because the subscription is not ready for it, a
 * {@link RoutingOutcome#DEFERRED} or {@link RoutingOutcome#UNAVAILABLE} outcome, is not reported, since nothing
 * failed.
 */
public interface RabbitMqSourceListener {

    /**
     * The source handed the client the acknowledgement of a delivery its subscriber took or filtered out. Not called
     * when that call threw. The client drops the acknowledgement of a delivery from before an automatic connection
     * recovery, so the broker delivers it again and it can be counted twice.
     *
     * @param queue The queue the source consumes from.
     */
    default void delivered(String queue) {
    }

    /**
     * A delivery went to the source's {@link DeliveryFailurePolicy}. Called before the policy is applied, so
     * {@link #parked(String)} or {@link #redelivered(String)} follows when that works. Neither follows for a delivery
     * that fails after {@code close()} stopped waiting for the source's deliveries, which the broker puts back on the
     * queue once the channel closes.
     *
     * @param queue   The queue the source consumes from.
     * @param failure What the inbound mapper threw, or what handling the event threw, or {@code null} for a
     *                {@link RoutingOutcome#NOT_DELIVERABLE} outcome, which the subscription decides without throwing.
     */
    default void failed(String queue, @Nullable Throwable failure) {
    }

    /**
     * The parking destination confirmed a copy of a failed delivery, and the source handed the client the
     * acknowledgement of the original, which the client drops across an automatic connection recovery like the one
     * {@link #delivered(String)} describes.
     *
     * @param queue The queue the source consumes from.
     */
    default void parked(String queue) {
    }

    /**
     * The source handed a failed delivery back to the queue, under {@link DeliveryFailurePolicy#REDELIVER} or after a
     * park that failed.
     *
     * @param queue The queue the source consumes from.
     */
    default void redelivered(String queue) {
    }
}
