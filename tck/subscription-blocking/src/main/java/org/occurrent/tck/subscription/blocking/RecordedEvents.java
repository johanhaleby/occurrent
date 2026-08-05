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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
     * lists rather than a bare timeout. Anything else already there comes back too, so an over-delivering model is
     * caught by the caller's assertion.
     */
    public List<CloudEvent> awaitAtLeast(int count, Duration timeout) {
        return Arrivals.awaitAtLeast(arrived, count, timeout, "event");
    }

    /**
     * Everything that has arrived so far, without waiting. Use this only after a wait that established the arrival
     * being asserted, never as the whole of an assertion, since on its own it races delivery.
     */
    public List<CloudEvent> soFar() {
        return Arrivals.drain(arrived);
    }
}
