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
 * <strong>pushed into</strong> from outside, rather than one that reads them from the event store itself. An external
 * source hands each received {@link CloudEvent} to {@link #accept(CloudEvent)}, whose returned {@link Mono} completes
 * once the target's handlers have processed the event, so the caller can acknowledge after processing.
 * <p>
 * This is the CloudEvent-level capability that the reactor {@code PushSubscriptionModel} provides, kept separate so a
 * listener can depend on the capability rather than a concrete model.
 */
@NullMarked
public interface Pushable extends SubscriptionModelCapability {

    /**
     * Push a single event to the target. The returned {@link Mono} completes once every matching handler has completed.
     */
    Mono<Void> accept(CloudEvent cloudEvent);

    /**
     * Push a batch of events, dispatching each in iteration order, sequentially.
     */
    default Mono<Void> accept(Iterable<CloudEvent> cloudEvents) {
        return Flux.fromIterable(cloudEvents).concatMap(this::accept).then();
    }
}
