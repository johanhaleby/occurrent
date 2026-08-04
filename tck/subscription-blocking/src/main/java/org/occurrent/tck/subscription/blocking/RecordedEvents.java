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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * What one subscription received, in arrival order. This is the handler the suites subscribe with, and the thing they
 * assert on.
 * <p>
 * It waits by blocking on a queue rather than by polling an assertion, so it wakes when an event arrives instead of
 * paying a poll interval per event, and one deadline covers the whole set rather than each event getting its own budget.
 * <p>
 * Nothing here proves that no further event is coming, and no timeout can. Where a suite needs that, it publishes a
 * marker afterwards and waits for the marker, so the wait is for something that must arrive rather than for a period in
 * which nothing must.
 */
@NullMarked
public final class RecordedEvents implements Consumer<CloudEvent> {

    private final BlockingQueue<CloudEvent> arrived = new LinkedBlockingQueue<>();

    @Override
    public void accept(CloudEvent cloudEvent) {
        arrived.add(requireNonNull(cloudEvent, "cloudEvent cannot be null"));
    }

    /**
     * Waits until {@code count} events have arrived, or the timeout expires, and returns everything that arrived. A
     * short return is not an error here: the caller asserts on the list, so "expected 3, got 1" is a comparison of two
     * lists rather than a bare timeout.
     */
    public List<CloudEvent> awaitAtLeast(int count, Duration timeout) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1, was " + count
                    + ". To assert that nothing arrives, publish a marker afterwards and assert on what came back, "
                    + "since no wait can prove an absence.");
        }
        requireNonNull(timeout, "timeout cannot be null");
        List<CloudEvent> received = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (received.size() < count) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            CloudEvent next = poll(remaining);
            if (next == null) {
                break;
            }
            received.add(next);
        }
        // Whatever else has already arrived, so an over-delivering model is caught by the caller's assertion rather
        // than leaving its extra events sitting in the queue unnoticed.
        arrived.drainTo(received);
        return received;
    }

    /**
     * Everything that has arrived so far, without waiting. Use this only after a wait that established the arrival
     * being asserted, never as the whole of an assertion, since on its own it races delivery.
     */
    public List<CloudEvent> soFar() {
        List<CloudEvent> received = new ArrayList<>();
        arrived.drainTo(received);
        return received;
    }

    private @Nullable CloudEvent poll(long remainingNanos) {
        try {
            return arrived.poll(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for an event to arrive", e);
        }
    }
}
