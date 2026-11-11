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
     * The source has not stopped. It consumes while its subscription runs and is ready, and waits otherwise, so this
     * covers a paused subscription too. Whether the connection is up is a separate question, which
     * {@code isConnected()} answers.
     */
    CONSUMING,

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
