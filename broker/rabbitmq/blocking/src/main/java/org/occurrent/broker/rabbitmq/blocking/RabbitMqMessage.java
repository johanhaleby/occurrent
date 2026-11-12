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

import static java.util.Objects.requireNonNull;

/**
 * The AMQP properties, headers included, and the body of a RabbitMQ message as it is published.
 * Returned by a {@link RabbitMqOutboundMapper}.
 *
 * @param properties The AMQP properties of the message, never {@code null}
 * @param body       The message body, never {@code null} and empty rather than absent
 */
public record RabbitMqMessage(BasicProperties properties, byte[] body) {
    public RabbitMqMessage {
        requireNonNull(properties, "properties cannot be null");
        requireNonNull(body, "body cannot be null");
    }
}
