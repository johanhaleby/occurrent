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

import io.cloudevents.CloudEvent;

/**
 * Turns a {@link CloudEvent} into the {@link RabbitMqMessage} a {@link RabbitMqCloudEventSink} publishes, so a
 * message can follow a wire format other than the {@code cloudEvents_*} headers of {@link RabbitMqCloudEventMapper}.
 * <p>
 * The sink adds the header {@code x-occurrent-rabbitmq-confirm-publisher-return-token} to every message it
 * publishes, and sets {@code deliveryMode} to 2 (persistent) when the returned properties leave it {@code null}.
 * An explicit {@code deliveryMode} is kept. The headers of the {@link RabbitMqDestination} reach the message only if
 * the mapper puts them there. The mapper runs once per publish attempt, so it must not have side
 * effects, and it runs outside the sink's publish lock, so it must be safe to call from several threads. A mapper that returns {@code null} fails the publish with an {@link IllegalStateException}, and an
 * exception the mapper throws reaches the caller of {@link RabbitMqCloudEventSink#publish(CloudEvent)} unchanged.
 * Neither is retried by the sink's default retry strategy.
 */
@FunctionalInterface
public interface RabbitMqOutboundMapper {

    /**
     * @param cloudEvent  The event to publish
     * @param destination The destination the sink's resolver chose for the event
     * @return The message to publish, never {@code null}
     */
    RabbitMqMessage toMessage(CloudEvent cloudEvent, RabbitMqDestination destination);

    /**
     * The default mapper. Writes the event in the binary content mode {@link RabbitMqCloudEventMapper} defines,
     * with the destination's headers as application headers.
     */
    static RabbitMqOutboundMapper cloudEventHeaders() {
        return (cloudEvent, destination) -> {
            byte[] body = RabbitMqCloudEventMapper.toBody(cloudEvent);
            return new RabbitMqMessage(RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, destination.headers(), body), body);
        };
    }
}
