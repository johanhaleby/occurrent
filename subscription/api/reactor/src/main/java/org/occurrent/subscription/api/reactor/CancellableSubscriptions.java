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
import reactor.core.publisher.Mono;

/**
 * The capability to cancel an individual subscription by id. Split out from {@link SubscriptionModelLifeCycle} because
 * a register-only {@link Subscribable} such as a push model can cancel but has nothing to start, stop, or pause: its
 * events arrive from the caller rather than from a feed it drives.
 */
@NullMarked
public interface CancellableSubscriptions extends SubscriptionModelCapability {

    /**
     * Cancel a subscription so it receives no further events, and release its id for reuse. Cancelling an id that is
     * unknown or already cancelled stops nothing, and still deletes what a store holds for that id, as described
     * below.
     * <p>
     * The cancel takes effect when this method is called, whether or not anything subscribes to the returned
     * {@code Mono}. A caller that ignores the return value therefore gets the same cancel as before this method
     * returned anything, with the store cleanup running in the background.
     * <p>
     * A model that stores a checkpoint, a catch-up marker, or anything else a later subscribe under the same id would
     * resume from also deletes it here. The returned {@code Mono} completes once every such delete for this id has
     * succeeded, in this model and in every model it wraps, and fails when one of them fails. Once it completes, no
     * store holds such state that the cancelled subscription wrote, so a later subscribe under the same id
     * does not resume from where the cancelled one got to. It is cached, so subscribing to it more than once waits for
     * the same deletes rather than running them again.
     * <p>
     * Each model Occurrent ships that stores such state also logs a failed delete as a warning, whether or not
     * anything subscribes to the {@code Mono}, since a caller that ignores it has no other way to learn that the state
     * is still stored. A caller that handles the error therefore sees the failure twice, once in its handler and once
     * in the log.
     * <p>
     * A failed {@code Mono}, or a process that ended before it completed, can leave that state stored. Calling this
     * method again for the same id deletes it, also in a new process that never subscribed that id.
     *
     * @param subscriptionId The id of the subscription to cancel.
     * @return A {@code Mono} that completes once the store cleanup for this id has succeeded.
     */
    Mono<Void> cancelSubscription(String subscriptionId);
}
