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

package org.occurrent.retry;

/**
 * Thrown by an action to say that the attempt for this event wasn't made, for example because the subscription it
 * would deliver to has been closed, or because the event was withdrawn before the action ran it. It is not a failure.
 * <p>
 * It is meant for subscription models and the models that wrap them, such as {@code CompetingConsumerSubscriptionModel}
 * when it withdraws an event it holds for the lease, and not for an event handler. A model throws it from the action
 * once it has paused, cancelled or stopped the subscription, or begun to shut down. Thrown from the action of a
 * subscription that is still open, it ends that subscription's run on the MongoDB change stream models, which deliver
 * nothing more for the subscription until it is paused and resumed. A blocking catch-up replay is held instead,
 * and runs again once the subscription is resumed or the model is started with {@code start(true)}.
 * <p>
 * A subscription model that declares {@code RedeliveringSubscriptions} and answers {@code true} treats it this way.
 * It doesn't move the subscription's position past the event, doesn't retry the event, and doesn't report it as a
 * failure or to the listeners of its {@link RetryStrategy}. It delivers the event again when the subscription is
 * resumed or started. A model that retries the action in a loop of its own rethrows it without another attempt, as
 * the retry of a {@link RetryStrategy} does.
 * <p>
 * The retry of a {@link RetryStrategy} rethrows it without another attempt, and without calling the retry predicate,
 * the error mapper or the listeners, since the attempt did not fail. When the skipped attempt is a retry, the
 * before-retry listener has already been called, and no after-retry listener follows.
 */
public final class AttemptNotMade extends RuntimeException {

    /**
     * @param message What kept the attempt from being made.
     */
    public AttemptNotMade(String message) {
        super(message, null, false, false);
    }
}
