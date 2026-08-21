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

import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

/**
 * Builds a {@link RabbitMqCloudEventBridge.Builder} pre-seeded with this application's connection, resolver and
 * {@code occurrent.broker.rabbitmq.bridge.*} defaults, so declaring one consumer's bridge is one call rather than
 * repeating every shared setting by hand. A bridge is inherently per-consumer, one queue per projection or saga
 * per ADR 90, so this is a factory rather than a single auto-configured bridge bean.
 * <p>
 * Every value this factory pre-seeds is still a plain {@link RabbitMqCloudEventBridge.Builder} call, so any of
 * them can be overridden before {@link RabbitMqCloudEventBridge.Builder#build()}, and every refusal the builder
 * itself makes, a missing resolver with {@code declareTopology} left on, {@code PARK} with no parking destination,
 * still applies unchanged. This factory only fills in defaults and never relaxes what the builder already enforces.
 * <p>
 * <strong>The returned builder's {@link RabbitMqCloudEventBridge#close()} is the caller's to call once the
 * consumer is done with it,</strong> the same way the hand-wired bootstraps already do. The bridge this builds is
 * not a Spring bean, so nothing in the application context closes it.
 */
public interface RabbitMqCloudEventBridgeFactory {

    /**
     * @param queue          The queue this bridge consumes from, and declares unless
     *                       {@code occurrent.broker.rabbitmq.bridge.declare-topology} is {@code false}.
     * @param model          The live model this bridge feeds.
     * @param outcomeChannel Shared with {@code model}'s own constructor, see {@link RoutingOutcomeChannel}.
     */
    RabbitMqCloudEventBridge.Builder forQueue(String queue, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel);
}
