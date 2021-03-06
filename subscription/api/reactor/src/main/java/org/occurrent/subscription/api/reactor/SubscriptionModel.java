/*
 * Copyright 2020 Johan Haleby
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import reactor.core.publisher.Flux;

/**
 * Common interface for reactor (reactive) subscriptions. The purpose of a subscription is to read events from an event store
 * and react to these events. Typically a subscription will forward the event to another piece of infrastructure such as
 * a message bus or to create views from the events (such as projections, sagas, snapshots etc).
 */
public interface SubscriptionModel {

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link SubscriptionFilter} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events.
     */
    Flux<CloudEvent> subscribe(SubscriptionFilter filter, StartAt startAt);

    /**
     * Stream events from the event store as they arrive but filter only events that matches the <code>filter</code>.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEvent> subscribe(SubscriptionFilter filter) {
        return subscribe(filter, StartAt.subscriptionModelDefault());
    }


    /**
     * Stream events from the event store as they arrive from the given start position ({@code startAt}).
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEvent> subscribe(StartAt startAt) {
        return subscribe(null, startAt);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link SubscriptionPosition} that can be used to resume the stream from the current position.
     */
    default Flux<CloudEvent> subscribe() {
        return subscribe(null, StartAt.subscriptionModelDefault());
    }
}