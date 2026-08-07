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
 * The subscription exists on this model and is already running, so resuming it was refused. Answering silently would
 * hide a lifecycle bug in the calling code, which is why resuming is a transition rather than a goal. Starting a whole
 * model is the opposite and accepts being called twice, see
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0105-starting-a-model-twice-is-allowed-and-a-subscription-that-has-not-started-says-so.md">ADR 105</a>.
 * <p>
 * The model knows the id. An id it does not know is {@link UnknownSubscriptionException} instead.
 */
public final class SubscriptionAlreadyRunningException extends SubscriptionRefusedException {

    private final String subscriptionId;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id of the subscription that is already running
     */
    public SubscriptionAlreadyRunningException(String subscriptionId) {
        this(subscriptionId, "Subscription " + subscriptionId + " is already running.");
    }

    /**
     * Creates an exception with a message of your own, for a model that has something to add beyond the id.
     *
     * @param subscriptionId The id of the subscription that is already running
     * @param message        The message to report
     */
    public SubscriptionAlreadyRunningException(String subscriptionId, String message) {
        super(message);
        this.subscriptionId = subscriptionId;
    }

    /**
     * @return The subscription id that is already running
     */
    public String subscriptionId() {
        return subscriptionId;
    }
}
