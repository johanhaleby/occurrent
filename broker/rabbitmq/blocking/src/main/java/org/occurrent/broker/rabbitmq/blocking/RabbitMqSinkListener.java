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

import java.time.Duration;

/**
 * Told by a {@link RabbitMqCloudEventSink} how each publish attempt ended, so an application can count them, for
 * example as metrics. Set it with {@link RabbitMqCloudEventSink.Builder#listener(RabbitMqSinkListener)}. Every method
 * does nothing by default, so a listener implements only the ones it needs.
 * <p>
 * The sink calls the listener on the thread that publishes, once per attempt, so a publish the sink retries reports
 * each attempt. A listener that throws is logged at {@code warn} and ignored, and the publish goes on as if it had
 * returned. A listener should still return quickly, since the publish waits for it.
 * <p>
 * An attempt that fails before the sink knows where the event goes, because the resolver threw, is not reported.
 */
public interface RabbitMqSinkListener {

    /**
     * The broker confirmed a publish attempt and did not return it.
     *
     * @param destination Where the event was published.
     * @param publishTime From the start of the attempt, before the destination was resolved, to the confirm.
     */
    default void published(RabbitMqDestination destination, Duration publishTime) {
    }

    /**
     * The broker returned a publish attempt because no queue was bound for its routing key. Called under both
     * {@link RabbitMqUnroutablePolicy} values, before the sink fails the publish or logs and goes on.
     *
     * @param destination Where the event was published.
     */
    default void unroutable(RabbitMqDestination destination) {
    }

    /**
     * A publish attempt threw for any other reason, the outbound mapper included. The sink rethrows {@code failure}
     * unchanged after this, so a publish it retries can call this more than once.
     *
     * @param destination Where the event was to be published.
     * @param failure     What the attempt threw.
     */
    default void failed(RabbitMqDestination destination, Throwable failure) {
    }
}
