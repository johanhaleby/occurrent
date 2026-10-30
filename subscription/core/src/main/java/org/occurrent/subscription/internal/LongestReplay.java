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

package org.occurrent.subscription.internal;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The longest time any replay of one catch-up took from reading its live start to checking that the subscription
 * model can still resume from it, which is how far back the change stream history must reach so the catch-up can go
 * live.
 */
public final class LongestReplay {

    private final AtomicLong longestNanos = new AtomicLong();
    private volatile long liveStartReadAt = System.nanoTime();

    /**
     * Marks the moment a replay read its live start.
     */
    public void liveStartRead() {
        liveStartReadAt = System.nanoTime();
    }

    /**
     * Marks the moment a replay checks its live start, and keeps the time since it was read when that is the longest
     * so far.
     */
    public void liveStartChecked() {
        long took = System.nanoTime() - liveStartReadAt;
        longestNanos.accumulateAndGet(took, Math::max);
    }

    /**
     * The longest time so far.
     */
    public Duration longest() {
        return Duration.ofNanos(longestNanos.get());
    }
}
