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

package org.occurrent.benchmark.coalescing;

/**
 * Builds the two key-distribution shapes {@link CoalescingFlushBenchmark} replays through a coalescing view. One is
 * a sparse batch where every event updates a different view instance, the other a dense batch where every event
 * updates one of a small, fixed pool of instances. A flush reads and writes once per distinct key in the batch, so
 * these two shapes bound how much a flush can save by combining several events onto the same key before touching
 * the repository, regardless of how large the batch itself is.
 * <p>
 * Public, and so is {@link KeyDensity}: JMH's generated benchmark harness lives in a {@code jmh_generated}
 * sub-package and references {@code CoalescingFixtures.KeyDensity} as a {@code @Param} field type, so both need to
 * be reachable from outside this package even though {@link CoalescingFlushBenchmark} itself sits in the same
 * package as this class. Everything else here stays package-private.
 */
public final class CoalescingFixtures {

    private CoalescingFixtures() {
    }

    public enum KeyDensity {
        /** Every event in the batch targets its own key, so a flush touches as many keys as it has events. */
        SPARSE,
        /** Every event in the batch targets one of {@link #DENSE_KEY_COUNT} keys, however large the batch is. */
        DENSE
    }

    /** How many distinct keys a dense batch cycles through, whatever its size. */
    static final int DENSE_KEY_COUNT = 10;

    /** How many distinct keys a batch of {@code eventCount} events touches under {@code density}. */
    static int keyPoolSize(KeyDensity density, int eventCount) {
        return density == KeyDensity.DENSE ? Math.min(DENSE_KEY_COUNT, eventCount) : eventCount;
    }

    /** The view-instance key for the {@code index}-th event in a batch of {@code eventCount} events. */
    static String keyFor(KeyDensity density, int eventCount, long index) {
        return "key-" + (index % keyPoolSize(density, eventCount));
    }
}
