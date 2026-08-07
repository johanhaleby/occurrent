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
 * A subscription id is already in use on this subscription model instance, so subscribing again with it was refused.
 * Accepting it would silently replace the handler already behind that id.
 * <p>
 * Uniqueness is scoped to one model instance. Several instances, on several nodes, sharing one subscription id is the
 * competing consumer pattern and is untouched by this. See
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0102-a-subscription-id-is-unique-per-subscription-model-instance.md">ADR 102</a>.
 */
public final class DuplicateSubscriptionIdException extends SubscriptionRefusedException {

    private final String subscriptionId;

    /**
     * Creates an exception with the standard message. This is the message every Occurrent subscription model
     * produces, so prefer this constructor over supplying your own.
     *
     * @param subscriptionId The id that is already in use
     */
    public DuplicateSubscriptionIdException(String subscriptionId) {
        this(subscriptionId, "Subscription " + subscriptionId + " is already defined.");
    }

    /**
     * Creates an exception with a message of your own, for a model that has something to add beyond the id.
     *
     * @param subscriptionId The id that is already in use
     * @param message        The message to report
     */
    public DuplicateSubscriptionIdException(String subscriptionId, String message) {
        super(message);
        this.subscriptionId = subscriptionId;
    }

    /**
     * @return The subscription id that was already in use
     */
    public String subscriptionId() {
        return subscriptionId;
    }
}
