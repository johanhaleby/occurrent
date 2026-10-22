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

import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSource;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;

/**
 * Builds a {@link RabbitMqDomainEventSource.Builder} pre-seeded with this application's connection, resolver and
 * {@code occurrent.broker.rabbitmq.source.*} defaults, the domain-level twin of
 * {@code RabbitMqCloudEventSourceFactory}. A source is inherently per-consumer, one queue per projection or saga
 * per ADR 90, so this is a factory rather than a single auto-configured source bean.
 * <p>
 * Call {@code build()} on the returned builder before {@code catchUp}, {@code catchUpAll} or {@code goLive} is called
 * on the feed, since that declares the queue and its bindings. With {@code occurrent.broker.rabbitmq.source.declare-topology} set to
 * {@code false}, whoever owns the topology must create them before then. The Topology paragraph of
 * {@code RabbitMqCloudEventSource} says what is lost otherwise.
 * <p>
 * <strong>The returned builder's {@link RabbitMqDomainEventSource#close()} is the caller's to call once the
 * consumer is done with it.</strong> The source this builds is not a Spring bean, so nothing in the application
 * context closes it.
 */
public interface RabbitMqDomainEventSourceFactory {

    /**
     * @param queue The queue this source consumes from, and declares unless
     *              {@code occurrent.broker.rabbitmq.source.declare-topology} is {@code false}.
     * @param feed  The feed this source calls {@code acceptOrDeferCloudEvent(...)} on.
     */
    <E> RabbitMqDomainEventSource.Builder<E> forQueue(String queue, DomainEventFeed<E> feed);
}
