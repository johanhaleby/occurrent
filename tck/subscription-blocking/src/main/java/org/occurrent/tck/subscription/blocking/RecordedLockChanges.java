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

package org.occurrent.tck.subscription.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Objects.requireNonNull;

/**
 * What one listener was told, in arrival order. This is the listener the competing-consumer suite registers, and the
 * thing it asserts on.
 * <p>
 * It waits by blocking on a queue rather than by polling, the same shape {@link RecordedEvents} uses for delivered
 * events and for the same reason: a lock change is pushed to a listener, so a wait can wake on arrival instead of
 * paying a poll interval. Where the suite has no listener to wait on it polls {@code hasLock} instead, because that is
 * the whole of what a strategy without a listener reports.
 * <p>
 * Nothing here proves that no further change is coming, and no timeout can. Where the suite needs that, it waits for a
 * change that must arrive and then asserts on everything recorded, so the wait is for something that must happen rather
 * than for a period in which nothing must.
 */
@NullMarked
public final class RecordedLockChanges implements CompetingConsumerListener {

    /**
     * Which way a lock went for a subscriber.
     */
    public enum Kind {
        /**
         * {@link CompetingConsumerListener#onConsumeGranted(String, String)}.
         */
        GRANTED,
        /**
         * {@link CompetingConsumerListener#onConsumeProhibited(String, String)}.
         */
        PROHIBITED
    }

    /**
     * One call into the listener.
     */
    public record LockChange(String subscriptionId, String subscriberId, Kind kind) {
        public LockChange {
            requireNonNull(subscriptionId, "subscriptionId cannot be null");
            requireNonNull(subscriberId, "subscriberId cannot be null");
            requireNonNull(kind, "kind cannot be null");
        }
    }

    private final BlockingQueue<LockChange> arrived = new LinkedBlockingQueue<>();

    @Override
    public void onConsumeGranted(String subscriptionId, String subscriberId) {
        arrived.add(new LockChange(subscriptionId, subscriberId, Kind.GRANTED));
    }

    @Override
    public void onConsumeProhibited(String subscriptionId, String subscriberId) {
        arrived.add(new LockChange(subscriptionId, subscriberId, Kind.PROHIBITED));
    }

    /**
     * Waits until {@code count} changes have arrived, or the timeout expires, and returns everything that arrived. A
     * short return is not an error here: the caller asserts on the list, so "expected a grant, got nothing" is a
     * comparison of two lists rather than a bare timeout. Anything else already there comes back too, so a strategy
     * reporting the same change twice is caught by the caller's assertion.
     */
    public List<LockChange> awaitAtLeast(int count, Duration timeout) {
        return Arrivals.awaitAtLeast(arrived, count, timeout, "lock change");
    }

    /**
     * Everything that has arrived so far, without waiting. Use this only after a wait that established the change being
     * asserted, never as the whole of an assertion, since on its own it races the strategy.
     */
    public List<LockChange> soFar() {
        return Arrivals.drain(arrived);
    }
}
