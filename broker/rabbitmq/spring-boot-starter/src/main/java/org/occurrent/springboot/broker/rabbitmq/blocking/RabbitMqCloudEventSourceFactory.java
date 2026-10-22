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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

/**
 * Builds a {@link RabbitMqCloudEventSource.Builder} pre-seeded with this application's connection, resolver and
 * {@code occurrent.broker.rabbitmq.source.*} defaults, so declaring one consumer's source is one call rather than
 * repeating every shared setting by hand. A source is inherently per-consumer, one queue per projection or saga
 * per ADR 90, so this is a factory rather than a single auto-configured source bean.
 * <p>
 * Every value this factory pre-seeds is still a plain {@link RabbitMqCloudEventSource.Builder} call, so any of
 * them can be overridden before {@link RabbitMqCloudEventSource.Builder#build()}, and every refusal the builder
 * itself makes, a missing resolver with {@code declareTopology} left on, {@code PARK} with no parking destination,
 * still applies unchanged. This factory only fills in defaults and never relaxes what the builder already enforces.
 * <p>
 * Call {@code build()} on the returned builder before the catch-up subscribes, since the queue must exist by then.
 * The Topology paragraph of {@link RabbitMqCloudEventSource} says what is lost otherwise.
 * <p>
 * <strong>The returned builder's {@link RabbitMqCloudEventSource#close()} is the caller's to call once the
 * consumer is done with it,</strong> the same way the hand-wired bootstraps already do. The source this builds is
 * not a Spring bean, so nothing in the application context closes it.
 */
public interface RabbitMqCloudEventSourceFactory {

    /**
     * @param queue The queue this source consumes from, and declares unless
     *              {@code occurrent.broker.rabbitmq.source.declare-topology} is {@code false}.
     * @param model The live model this source feeds.
     */
    RabbitMqCloudEventSource.Builder forQueue(String queue, PushSubscriptionModel model);
}
