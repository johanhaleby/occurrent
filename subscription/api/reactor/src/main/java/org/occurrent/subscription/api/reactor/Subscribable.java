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
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * A named, lifecycle-managed counterpart to the plain reactive {@link SubscriptionModel#subscribe(SubscriptionFilter, StartAt)}
 * primitive. Where that primitive returns a bare {@link Flux} the caller subscribes to and
 * disposes itself, a {@code Subscribable} tracks the subscription by id so it can be paused, resumed, and cancelled
 * through {@link SubscriptionModelLifeCycle}.
 */
@NullMarked
public interface Subscribable {

    /**
     * Start listening to cloud events persisted to the event store using the supplied start position and <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore. The
     *                       next event is not processed until the returned {@link Mono} completes.
     */
    Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action);

    /**
     * Start listening to cloud events persisted to the event store at the supplied start position.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param startAt        The position to start the subscription from
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, null, startAt, action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time with the specified <code>filter</code>.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param filter         The filter to use to limit which events that are of interest from the EventStore.
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), action);
    }

    /**
     * Start listening to cloud events persisted to the event store at this moment in time.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    default Subscription subscribe(String subscriptionId, Function<CloudEvent, Mono<Void>> action) {
        return subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), action);
    }
}
