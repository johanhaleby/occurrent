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

/**
 * What {@link RabbitMqCloudEventSink#publish(io.cloudevents.CloudEvent)} does when the broker returns a message
 * because no queue is bound for its routing key. The sink publishes every message with {@code mandatory} set under
 * both policies, so the broker always tells it when a message went nowhere. The policy only decides whether that
 * fails the publish. Set it with {@link RabbitMqCloudEventSink.Builder#onUnroutable(RabbitMqUnroutablePolicy)}.
 */
public enum RabbitMqUnroutablePolicy {

    /**
     * Fail the publish with {@link RabbitMqUnroutableEventException}. This is the default. Use it when nobody
     * receiving an event is a mistake, for example a typo in a routing key. Under {@link
     * org.occurrent.broker.api.blocking.CloudEventForwarder} the subscription then tries the same event again and
     * forwards nothing after it until a queue is bound for that routing key.
     */
    FAIL,

    /**
     * Log a warning with the exchange, the routing key, and the id and type of the event, and return from
     * {@link RabbitMqCloudEventSink#publish(io.cloudevents.CloudEvent)} as if the message was routed. The broker
     * drops the message. Use it on an exchange where events are published for whoever wants them, so an event type
     * that no queue is bound for yet doesn't stop the sink from forwarding the events after it.
     */
    LOG_AND_CONTINUE
}
