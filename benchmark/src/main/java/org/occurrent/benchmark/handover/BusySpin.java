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

package org.occurrent.benchmark.handover;

import java.util.concurrent.atomic.LongAdder;

/**
 * Stands in for a real push handler's or fold's per-event work, the same role ADR 108's own benchmark used a busy
 * spin for: "a busy-spin standing in for real per-event work". A fixed nanosecond deadline is used rather than a JMH
 * {@code Blackhole} token count, since a token is only an approximate proxy for a fixed wall-clock duration, and the
 * ADR's workloads are stated in microseconds (1, 50, 200). Shared across the benchmark module's classes rather than
 * reimplemented per benchmark, so every benchmark's simulated work stays the same shape.
 */
public final class BusySpin {

    private BusySpin() {
    }

    /**
     * Spins until {@code micros} has elapsed, folding a counter into {@code sink} on every iteration so the loop is
     * not eligible for dead-code elimination.
     */
    public static void spinMicros(long micros, LongAdder sink) {
        long deadlineNanos = System.nanoTime() + (micros * 1_000L);
        long iterations = 0;
        while (System.nanoTime() < deadlineNanos) {
            iterations++;
        }
        sink.add(iterations);
    }
}
