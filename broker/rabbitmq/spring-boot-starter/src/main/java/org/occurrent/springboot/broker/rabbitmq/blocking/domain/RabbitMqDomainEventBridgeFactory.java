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

package org.occurrent.springboot.broker.rabbitmq.blocking.domain;

import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;

/**
 * Builds a {@link RabbitMqDomainEventBridge.Builder} pre-seeded with this application's connection, resolver and
 * {@code occurrent.broker.rabbitmq.bridge.*} defaults, the domain-level twin of
 * {@code RabbitMqCloudEventBridgeFactory}. A bridge is inherently per-consumer, one queue per projection or saga
 * per ADR 90, so this is a factory rather than a single auto-configured bridge bean.
 * <p>
 * <strong>The returned builder's {@link RabbitMqDomainEventBridge#close()} is the caller's to call once the
 * consumer is done with it.</strong> The bridge this builds is not a Spring bean, so nothing in the application
 * context closes it.
 */
public interface RabbitMqDomainEventBridgeFactory {

    /**
     * @param queue The queue this bridge consumes from, and declares unless
     *              {@code occurrent.broker.rabbitmq.bridge.declare-topology} is {@code false}.
     * @param feed  The feed this bridge calls {@code acceptCloudEvent(...)} on.
     */
    <E> RabbitMqDomainEventBridge.Builder<E> forQueue(String queue, DomainEventFeed<E> feed);
}
