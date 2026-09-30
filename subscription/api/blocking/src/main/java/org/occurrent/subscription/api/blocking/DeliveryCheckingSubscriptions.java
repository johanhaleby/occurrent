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

package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;

import java.util.Optional;

/**
 * A subscription model that lets code running inside a subscription's action check that the model still delivers the
 * event to the subscription. The model checks that before it calls the action, but a model that wraps the action and
 * reads from a database before it calls the subscriber's own action, for example to know which version to write a
 * checkpoint with, can see a pause or a cancel return during that read. It calls
 * {@link #checkStillDelivering()} right before the subscriber's action, so the subscriber's action doesn't start once a
 * pause or a cancel has returned.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface DeliveryCheckingSubscriptions extends SubscriptionModelCapability {

    /**
     * Called from inside a subscription's action, on the thread the model called the action on. Throws when a pause,
     * a cancel or a stop has ended the delivery that called the action. The model then counts the call to the action
     * as not made, so it doesn't try it again, doesn't tell the listeners of its retry strategy and doesn't move the
     * subscription's position past the event. A resume delivers the event again. Returns at once when the delivery
     * goes on, or when it isn't called from inside an action this model called.
     *
     * @throws RuntimeException When the delivery that called the action has ended. Let it propagate to the model.
     */
    void checkStillDelivering();

    /**
     * Finds the {@link DeliveryCheckingSubscriptions} capability behind {@code subscriptionModel}, unwrapping a
     * {@link SubscriptionModelWrapper} chain until one is found.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if nothing in the chain implements it.
     */
    static Optional<DeliveryCheckingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(DeliveryCheckingSubscriptions.class);
    }
}
