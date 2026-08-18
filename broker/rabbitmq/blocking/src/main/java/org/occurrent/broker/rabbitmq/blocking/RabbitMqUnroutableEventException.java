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
 * A {@link RabbitMqCloudEventSink} publish the broker confirmed but could not route, reported through AMQP's
 * {@code basic.return} rather than through the confirm. RabbitMQ confirms a publish to an exchange with no binding
 * matching the routing key and then discards the message, so a sink that only read the confirm would report success
 * for an event nobody will ever receive. Every {@link RabbitMqCloudEventSink} publish sets {@code mandatory} and
 * treats a return this way instead, so a typo in a routing key fails loudly rather than looking like a working
 * deployment.
 */
public class RabbitMqUnroutableEventException extends RabbitMqPublishException {

    public RabbitMqUnroutableEventException(String exchange, String routingKey) {
        super("Message published to exchange \"" + exchange + "\" with routing key \"" + routingKey +
                "\" was confirmed by the broker but returned as unroutable (no matching binding)");
    }
}
