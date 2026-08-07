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
 * This subscription model has no subscription with that id, so pausing or resuming it was refused. The id was never
 * subscribed here, or it was cancelled, or it belongs to a different model instance.
 * <p>
 * This is the refusal that says the id does not exist, as opposed to {@link SubscriptionNotRunningException} and
 * {@link SubscriptionAlreadyRunningException}, which both mean the model knows the id and the call does not apply to
 * the state it is in. A caller holding several models and looking for the one that owns an id wants exactly that
 * difference: this one means keep looking, the other two mean you have found the owner.
 */
public final class UnknownSubscriptionException extends SubscriptionRefusedException {

    private final String subscriptionId;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id the model does not have
     */
    public UnknownSubscriptionException(String subscriptionId) {
        this(subscriptionId, "Subscription " + subscriptionId + " is not known to this subscription model.");
    }

    /**
     * Creates an exception with a message of your own, for a model that has something to add beyond the id.
     *
     * @param subscriptionId The id the model does not have
     * @param message        The message to report
     */
    public UnknownSubscriptionException(String subscriptionId, String message) {
        super(message);
        this.subscriptionId = subscriptionId;
    }

    /**
     * @return The subscription id the model does not have
     */
    public String subscriptionId() {
        return subscriptionId;
    }
}
