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
     * The projection is fed by an external push subscription model (a {@code PushSubscriptionModel} driven by a
     * RabbitMQ, Kafka, or other listener) that delivers <strong>CloudEvents</strong>. The framework wraps that model in a
     * replay-then-push bootstrap catch-up so a new or rebuilt projection is backfilled from the event store before it
     * goes live. Select the push model bean with {@link Projection#subscriptionModel()} or
     * {@link Projection#subscriptionModelName()}.
     */
    PUSH,
    /**
     * The projection is fed by an external source that delivers <strong>domain events</strong> directly (a listener with
     * its own message converter), through a {@code DomainEventFeed} bean the application owns and feeds. The live path
     * folds domain events with no CloudEvent conversion, and the framework gives each projection a bootstrap catch-up
     * that replays the event store once. Select the feed bean with {@link Projection#subscriptionModel()} or
     * {@link Projection#subscriptionModelName()}. The feed carries the event-id function used for catch-up de-dup.
     */
    DOMAIN_PUSH
}
