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
 * A {@link RabbitMqCloudEventSink} publish that did not succeed. {@link RabbitMqPublishTimeoutException} and
 * {@link RabbitMqUnroutableEventException} are the two specific ways that can happen. This is the general case,
 * a channel or connection failure the RabbitMQ client itself reported.
 */
public class RabbitMqPublishException extends RuntimeException {

    public RabbitMqPublishException(String message) {
        super(message);
    }

    public RabbitMqPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
