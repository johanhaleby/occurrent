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

import com.rabbitmq.client.AMQP.BasicProperties;
import io.cloudevents.CloudEvent;

/**
 * Turns a delivered RabbitMQ message into the {@link CloudEvent} a {@link RabbitMqCloudEventSource} or a
 * {@code RabbitMqDomainEventSource} hands to its subscribers, so a message without
 * {@code cloudEvents_*} headers can be consumed.
 * <p>
 * An exception the mapper throws, or a {@code null} it returns, is handled as a failed delivery by the source's
 * delivery failure policy.
 * <p>
 * The mapper must not modify {@code properties} or {@code body}. They are the delivery itself, and a parked delivery
 * is published from them. A source calls its mapper from its own worker thread, one delivery at a time, but the
 * Spring Boot starter starts every source builder it hands out with the same mapper bean, so a mapper must be safe to
 * call from several threads.
 */
@FunctionalInterface
public interface RabbitMqInboundMapper {

    /**
     * @param properties The AMQP properties of the delivered message, including its headers
     * @param body       The message body
     * @return The event read from the message, never {@code null}
     */
    CloudEvent toCloudEvent(BasicProperties properties, byte[] body);

    /**
     * The default mapper. Reads the binary content mode {@link RabbitMqCloudEventMapper} defines.
     */
    static RabbitMqInboundMapper cloudEventHeaders() {
        return RabbitMqCloudEventMapper::toCloudEvent;
    }
}
