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
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Waiting on things that arrive, for the two recording handlers in this package.
 * <p>
 * {@link RecordedEvents} records delivered events and {@link RecordedLockChanges} records lock changes, and they wait
 * the same way. Both block on a queue until enough has arrived or a deadline passes, then take whatever else is
 * already there so an implementation delivering more than it should is caught by the caller's assertion rather than
 * leaving its extra items in the queue unnoticed. Package-private, so neither class grows a base type an implementor
 * would see.
 */
@NullMarked
final class Arrivals {

    private Arrivals() {
    }

    /**
     * Waits until {@code count} things have arrived, or the timeout expires, and returns everything that arrived. A
     * short return is not an error. The caller asserts on the list, so "expected 3, got 1" is a comparison of two
     * lists rather than a bare timeout.
     *
     * @param what what one item is, for the two messages this can produce
     */
    static <T> List<T> awaitAtLeast(BlockingQueue<T> arrived, int count, Duration timeout, String what) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1, was " + count
                    + ". To assert that no " + what + " arrives, wait for one that must and assert on what came back, "
                    + "since no wait can prove an absence.");
        }
        requireNonNull(timeout, "timeout cannot be null");
        List<T> received = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (received.size() < count) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            T next = poll(arrived, remaining, what);
            if (next == null) {
                break;
            }
            received.add(next);
        }
        arrived.drainTo(received);
        return received;
    }

    /**
     * Waits until everything that has arrived satisfies {@code condition}, or the timeout expires, and returns
     * everything that arrived. For an assertion about arrival ORDER a plain count is the wrong thing to wait on. A
     * model that hands a slow catch-up replay over to a live feed can reach the count while a later-ordered item is
     * still in flight, and the assertion would then read a list that was still growing. Waiting on the condition the
     * caller is about to assert removes that race without changing what is asserted. A model that never satisfies it
     * still comes back at the deadline, and the caller's assertion then fails on the full list.
     */
    static <T> List<T> awaitUntil(BlockingQueue<T> arrived, Predicate<List<T>> condition, Duration timeout, String what) {
        requireNonNull(condition, "condition cannot be null");
        requireNonNull(timeout, "timeout cannot be null");
        List<T> received = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        arrived.drainTo(received);
        while (!condition.test(received)) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            T next = poll(arrived, remaining, what);
            if (next == null) {
                break;
            }
            received.add(next);
            arrived.drainTo(received);
        }
        arrived.drainTo(received);
        return received;
    }

    /**
     * Everything that has arrived so far, without waiting.
     */
    static <T> List<T> drain(BlockingQueue<T> arrived) {
        List<T> received = new ArrayList<>();
        arrived.drainTo(received);
        return received;
    }

    private static <T> @Nullable T poll(BlockingQueue<T> arrived, long remainingNanos, String what) {
        try {
            return arrived.poll(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a " + what + " to arrive", e);
        }
    }
}
