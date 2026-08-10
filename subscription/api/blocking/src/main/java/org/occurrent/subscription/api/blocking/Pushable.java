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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.util.function.Consumer;

/**
 * A subscription target that events are <strong>pushed into</strong> from outside, rather than one that reads them from
 * the event store itself. An external source, a RabbitMQ or Kafka listener, a Spring application event, an HTTP
 * endpoint, hands each received {@link CloudEvent} to {@link #accept(CloudEvent)}, which dispatches it to the target's
 * registered handlers on the calling thread.
 * <p>
 * This is the CloudEvent-level capability that {@code PushSubscriptionModel} provides. It is a separate interface so a
 * listener (or wiring) can depend on "a thing I push cloud events into" rather than a concrete model, and so a model
 * may choose to be pushable without every subscription model being one. Extends {@link Consumer} so a
 * {@code Pushable} is usable wherever a {@code Consumer<CloudEvent>} is expected.
 */
@NullMarked
public interface Pushable extends Consumer<CloudEvent>, SubscriptionModelCapability {

    /**
     * Push a single event to the target, dispatching it to every matching registered handler.
     */
    @Override
    void accept(CloudEvent cloudEvent);

    /**
     * Push a batch of events, dispatching each in iteration order.
     */
    default void accept(Iterable<CloudEvent> cloudEvents) {
        cloudEvents.forEach(this::accept);
    }
}
