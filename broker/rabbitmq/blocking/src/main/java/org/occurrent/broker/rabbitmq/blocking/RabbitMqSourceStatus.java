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
 * Whether a {@link RabbitMqCloudEventSource} or a {@code RabbitMqDomainEventSource} still works, as its
 * {@code status()} method reports it.
 */
public enum RabbitMqSourceStatus {

    /**
     * The source has neither stopped nor been closed. It does not say that a consumer is registered at the broker,
     * since the source has none while what it delivers to is paused, not yet started or not yet ready. Whether the
     * connection is up is a separate question, which {@code isConnected()} answers.
     */
    OPEN,

    /**
     * The source stopped for good on its own, after a failure it cannot recover from, such as a catch-up that failed
     * for good or a channel no recovery reopens. It consumes nothing until the application builds a new one.
     */
    STOPPED,

    /**
     * The application closed the source.
     */
    CLOSED
}
