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
import org.occurrent.retry.AttemptNotMade;

import java.util.Optional;

/**
 * A subscription model that says whether it delivers an event again after its action threw {@link AttemptNotMade} for
 * it.
 * <p>
 * That matters to a caller that holds an event in the action until it may deliver it, such as
 * {@code CompetingConsumerSubscriptionModel} while it waits for the lease. When the model answers {@code true}, such a
 * caller can withdraw the event by throwing {@link AttemptNotMade} instead of letting it through, and the event is
 * delivered by whichever node may deliver it.
 * <p>
 * A model that answers {@code true} treats an {@link AttemptNotMade} from the action as an attempt that wasn't made.
 * It doesn't move the subscription's position past the event, doesn't retry the event, and doesn't report it as a
 * failure or to the listeners of its retry strategy. It delivers the event again when the subscription is resumed or
 * started. A model that retries the action in a loop of its own rethrows {@link AttemptNotMade} without another
 * attempt.
 * <p>
 * A caller can hold an event in the action until a call it makes into the model has returned. So a model that answers
 * {@code true} never waits for the thread that delivers an event inside a call that only asks about a subscription or
 * starts the model, and waits for it only for a limited time inside a pause, a cancel, a stop or a shutdown.
 * <p>
 * A caller that finds no implementation, which is an empty result from {@link #findIn(SubscriptionModelCapability)},
 * treats the model as not delivering an event again.
 *
 * @see #redeliversAnAttemptNotMade()
 */
@NullMarked
public interface RedeliveringSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether this model delivers an event again after its action threw {@link AttemptNotMade} for it. The answer is
     * the same for the whole life of the model instance, so a caller can read it once.
     *
     * @return {@code true} when this model delivers such an event again once the subscription is resumed or started.
     */
    boolean redeliversAnAttemptNotMade();

    /**
     * The model behind {@code subscriptionModel} that answers whether it delivers an event again after
     * {@link AttemptNotMade}. A {@link SubscriptionModelWrapper} is not unwrapped for this capability, so a wrapper is
     * found only when it implements it itself. Whether the event comes back is a property of the whole delivery path,
     * and a wrapper can break it, for example by storing a position past the event.
     * <p>
     * An empty result means the model does not declare it, and a caller treats that as not delivering the event again.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these.
     * @return The model that answers, or empty when this object doesn't implement it.
     */
    static Optional<RedeliveringSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(RedeliveringSubscriptions.class);
    }
}
