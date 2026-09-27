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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The reactive counterpart of the blocking {@code Pushable}: a subscription target that events are
 * <strong>pushed into</strong> from outside, rather than one that reads them from the event store itself. There is no
 * reactive in-memory event store, so a listener on the blocking {@code InMemoryEventStore} hands the events of each
 * write to {@link #accept(Iterable)} and waits for the returned {@link Mono}, which does nothing until something
 * subscribes. Write from a thread that may block, as {@code PushSubscriptionModel} describes. A RabbitMQ or Kafka
 * listener calls {@code PushSubscriptionModel.acceptRedeliverable(CloudEvent)} instead, which this interface does not
 * declare, and acknowledges the message only when the outcome it completes with allows it.
 * <p>
 * This is the CloudEvent-level capability that the reactor {@code PushSubscriptionModel} provides, kept separate so a
 * listener can depend on the capability rather than a concrete model.
 */
@NullMarked
public interface Pushable extends SubscriptionModelCapability {

    /**
     * Push a single event to the target. The returned {@link Mono} does not complete before every handler the event
     * reaches has completed.
     */
    Mono<Void> accept(CloudEvent cloudEvent);

    /**
     * Push a batch of events, dispatching each in iteration order, sequentially.
     */
    default Mono<Void> accept(Iterable<CloudEvent> cloudEvents) {
        return Flux.fromIterable(cloudEvents).concatMap(this::accept).then();
    }
}
