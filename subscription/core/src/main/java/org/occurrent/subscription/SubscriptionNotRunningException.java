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

package org.occurrent.subscription;

/**
 * The subscription exists on this model but is not running, so pausing it was refused. It is already paused, or the
 * whole model is stopped, or it was registered and never started.
 * <p>
 * The model knows the id. An id it does not know is {@link UnknownSubscriptionException} instead. Ask the model's
 * {@code isPaused(id)} to tell an already-paused subscription from one that never started.
 */
public final class SubscriptionNotRunningException extends SubscriptionRefusedException {

    private final String subscriptionId;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id of the subscription that is not running
     */
    public SubscriptionNotRunningException(String subscriptionId) {
        this(subscriptionId, "Subscription " + subscriptionId + " is not running.");
    }

    /**
     * Creates an exception with a message of your own, for a model that has something to add beyond the id.
     *
     * @param subscriptionId The id of the subscription that is not running
     * @param message        The message to report
     */
    public SubscriptionNotRunningException(String subscriptionId, String message) {
        super(message);
        this.subscriptionId = subscriptionId;
    }

    /**
     * @return The subscription id that is not running
     */
    public String subscriptionId() {
        return subscriptionId;
    }
}
