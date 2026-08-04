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
 * Where a {@link Projection} or a {@link Saga} reads its events from.
 */
public enum Source {
    /**
     * The default: the events come from the event store's own subscription, through the framework's asynchronous
     * catch-up and durable subscription models. This is transport-neutral, the underlying mechanism is whatever the
     * configured event store provides.
     */
    EVENT_STORE,
    /**
     * The events come from an external push feed the application owns and feeds (driven by a RabbitMQ, Kafka, or other
     * listener), selected with the annotation's {@code subscriptionModel} or {@code subscriptionModelName}. The
     * framework puts a replay-then-push catch-up in front of it, which backfills from the event store once before going
     * live. A {@link Saga} can turn that replay off with {@code catchup = }{@link Catchup#NONE}, for a feed whose
     * events the local event store does not hold.
     * <p>
     * A {@link Projection} accepts two kinds of feed bean, and the type decides how live events are delivered: a
     * {@code PushSubscriptionModel} delivers <strong>CloudEvents</strong>, while a {@code DomainEventFeed} delivers
     * <strong>domain events</strong> directly, with no CloudEvent conversion on the live path (a
     * {@code DomainEventFeed} carries the event-id function used for catch-up de-dup). A {@link Saga} accepts only a
     * {@code PushSubscriptionModel}, since a domain-event feed carries none of the stream metadata a saga deduplicates
     * redeliveries with.
     */
    PUSH
}
