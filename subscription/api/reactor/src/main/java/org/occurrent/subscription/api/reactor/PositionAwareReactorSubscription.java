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
import org.occurrent.subscription.CloudEventWithSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;
import reactor.core.publisher.Mono;

/**
 * A {@link ReactorSubscription} that produces {@link CloudEventWithSubscriptionPosition} compatible {@link CloudEvent}'s.
 * This is useful for subscribers that want to persist the subscription position for a given subscription if the event store doesn't
 * maintain the position for subscriptions.
 */
public interface PositionAwareReactorSubscription extends ReactorSubscription<CloudEventWithSubscriptionPosition> {

    /**
     * The global subscription position might be e.g. the wall clock time of the server, vector clock, number of events consumed etc.
     * This is useful to get the initial position of a subscription before any message has been consumed by the subscription
     * (and thus no {@link SubscriptionPosition} has been persisted for the subscription). The reason for doing this would be
     * to make sure that a subscription doesn't loose the very first message if there's an error consuming the first event.
     *
     * @return The global subscription position for the database.
     */
    Mono<SubscriptionPosition> globalSubscriptionPosition();
}
