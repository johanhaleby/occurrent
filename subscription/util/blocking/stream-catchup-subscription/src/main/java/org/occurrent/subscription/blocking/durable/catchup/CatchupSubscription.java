/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * A {@link Subscription} whose start is running asynchronously (the catch-up replay). Public so both the stream and
 * DCB catch-up models can hand back the same kind of handle while the replay runs on a background thread.
 */
@NullMarked
record CatchupSubscription(String id, Future<Subscription> delegatedSubscription) implements Subscription {

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        final long timeStarted = System.currentTimeMillis();
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        final Subscription subscription;
        try {
            subscription = delegatedSubscription.get(safeTimeout.timeout(), safeTimeout.timeUnit());
        } catch (TimeoutException e) {
            return false;
        } catch (CancellationException e) {
            // Same answer as CancelledSubscription. This replay was cancelled, so it never started and nothing will
            // start it, and that is not a failure to report.
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            // Thrown rather than reported as false, so a caller that discards the return value still finds out that
            // its read model was never filled. The push catch-up handle answers the same way.
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            } else if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("The catch-up for subscription '" + id + "' failed", cause);
        }
        // The delegate gets whatever is left of the caller's budget. When the replay used all of it the subscription
        // did not start within the timeout, and handing a negative duration on would rely on the delegate tolerating
        // one, which nothing promises.
        Duration remaining = timeout.minusMillis(System.currentTimeMillis() - timeStarted);
        return !remaining.isNegative() && subscription.waitUntilStarted(remaining);
    }
}
