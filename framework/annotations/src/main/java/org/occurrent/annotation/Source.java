/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.annotation;

/**
 * Where a {@link Projection} reads its events from.
 */
public enum Source {
    /**
     * The default: the projection is fed by the event store's own subscription, through the framework's asynchronous
     * catch-up and durable subscription models. This is transport-neutral, the underlying mechanism is whatever the
     * configured event store provides.
     */
    EVENT_STORE,
    /**
     * The projection is fed by an external push feed the application owns and feeds (driven by a RabbitMQ, Kafka, or
     * other listener), selected with {@link Projection#subscriptionModel()} or {@link Projection#subscriptionModelName()}.
     * The framework gives the projection a replay-then-push catch-up that backfills it from the event store
     * once before it goes live. The feed bean's type decides how live events are delivered: a {@code PushSubscriptionModel}
     * delivers <strong>CloudEvents</strong>, while a {@code DomainEventFeed} delivers <strong>domain events</strong>
     * directly, with no CloudEvent conversion on the live path (a {@code DomainEventFeed} carries the event-id function
     * used for catch-up de-dup).
     */
    PUSH
}
