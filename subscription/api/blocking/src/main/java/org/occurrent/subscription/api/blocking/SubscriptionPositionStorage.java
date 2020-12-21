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

package org.occurrent.subscription.api.blocking;

import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionPosition;


/**
 * A {@code ReactorSubscriptionPositionStorage} provides means to read and write the subscription position to storage.
 * This subscriptions can continue where they left off by passing the {@link SubscriptionPosition} provided by {@link #read(String)}
 * to a {@link PositionAwareSubscriptionModel} when the application is restarted etc.
 */
public interface SubscriptionPositionStorage {

    /**
     * Read the subscription position for a given subscription.
     * <p>
     * Note that when starting a new subscription you typically want to create {@link StartAt} from the global subscription position
     * (using {@link PositionAwareSubscriptionModel#globalSubscriptionPosition()}) if no {@code SubscriptionPosition} is found for the given subscription.
     * </p>
     * For example:
     * <pre>
     * SubscriptionPosition subscriptionPosition = storage.read(subscriptionId);
     * if (subscriptionPosition == null) {
     *      subscriptionPosition = positionAwareReactorSubscription.globalSubscriptionPosition();
     *      storage.save(subscriptionId, subscriptionPosition);
     * }
     * StartAt startAt = StartAt.subscriptionPosition(subscriptionPosition);
     * </pre>
     *
     * @param subscriptionId The id of the subscription whose position to find
     * @return A Mono with the {@link SubscriptionPosition} data point for the supplied subscriptionId
     */
    SubscriptionPosition read(String subscriptionId);

    /*
     * Save the subscription position for the supplied subscriptionId to storage and then return it for easier chaining.
     */
    SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition);

    /**
     * Delete the {@link SubscriptionPosition} for the supplied {@code subscriptionId}.
     *
     * @param subscriptionId The id of the subscription to delete the {@link SubscriptionPosition} for.
     */
    void delete(String subscriptionId);

    /**
     * Check if the subscription id has a stored subscription position in this storage.
     *
     * @param subscriptionId The id of the subscription to check.
     * @return <code>true</code> if storage contains a subscription position for the stream id, <code>false</code> otherwise.
     */
    boolean exists(String subscriptionId);
}