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

/**
 * Tuning knobs shared by every catch-up-then-live handover engine ({@code BlockingHandover} and
 * {@code ReactiveHandover}): the de-dup cache retained across the replay-to-live overlap, and the cap on events
 * buffered from the live feed while the catch-up replay runs.
 *
 * @param dedupCacheSize    Recently delivered event ids retained to de-duplicate the replay-to-live overlap.
 * @param maxBufferedEvents Cap on events buffered from the live feed during the catch-up replay before failing loud.
 */
public record HandoverOptions(int dedupCacheSize, int maxBufferedEvents) {

    /** Default de-dup cache size, shared by every catch-up-then-live caller unless overridden. */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    /** Default live-buffer cap, shared by every catch-up-then-live caller unless overridden. */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    public HandoverOptions {
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
    }

    /** The default options: {@link #DEFAULT_DEDUP_CACHE_SIZE} and {@link #DEFAULT_MAX_BUFFERED_EVENTS}. */
    public static HandoverOptions defaults() {
        return new HandoverOptions(DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }
}
