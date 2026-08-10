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

package org.occurrent.subscription.api.reactor;

import org.jspecify.annotations.NullMarked;

/**
 * The capability to cancel an individual subscription by id. Split out from {@link SubscriptionModelLifeCycle} because
 * a register-only {@link Subscribable} such as a push model can cancel but has nothing to start, stop, or pause: its
 * events arrive from the caller rather than from a feed it drives.
 */
@NullMarked
public interface CancellableSubscriptions extends SubscriptionModelCapability {

    /**
     * Cancel a subscription so it receives no further events, and release its id for reuse. Cancelling an id that is
     * unknown or already cancelled is a no-op.
     * <p>
     * A model that stores a checkpoint also discards it here, so the subscription cannot later resume from the
     * position it reached.
     *
     * @param subscriptionId The id of the subscription to cancel.
     */
    void cancelSubscription(String subscriptionId);
}
