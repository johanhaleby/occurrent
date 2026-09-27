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
 * A subscription model refused to subscribe because it has been shut down. A model that is shut down cannot be started
 * again, so subscribing to it again fails the same way until the application creates a new one.
 * <p>
 * This extends {@link IllegalStateException}, which is what a model that was shut down threw before this type existed,
 * so code catching that still catches it. {@code ReactorMongoSubscriptionModel} throws it from {@code subscribe} once it
 * is shut down, and so does {@code ReactorDurableSubscriptionModel} wrapping a model that is not a
 * {@code SubscriptionModel}. Wrapping a {@code SubscriptionModel}, it passes on whatever that model throws.
 */
public class SubscriptionModelShutdownException extends IllegalStateException {

    /**
     * Creates an exception with the standard message.
     */
    public SubscriptionModelShutdownException() {
        this("Cannot start subscription because the subscription model is shutdown.");
    }

    /**
     * Creates an exception with a message of your own.
     *
     * @param message The message to report
     */
    public SubscriptionModelShutdownException(String message) {
        super(message);
    }
}
