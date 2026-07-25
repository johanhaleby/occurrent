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

package org.occurrent.subscription;

/**
 * Tuning knobs for a catch-up-then-live handover: a bootstrap that replays history from the event store, buffers what
 * the live feed emits meanwhile, then drains the buffer and goes live. Accepted by
 * {@code CatchupThenPushSubscriptionModel} and the projection DSL's {@code CatchupProjectionFeed} on both stacks.
 * <p>
 * {@code dedupCacheSize} bounds how far the replay-to-live overlap can be de-duplicated exactly. Beyond that window the
 * at-least-once contract applies, so an idempotent fold absorbs a duplicate.
 * <p>
 * {@code maxBufferedEvents} is a fail-loud cap, not a throttle. Reaching it means the replay is not keeping up with the
 * live feed at all, so the catch-up throws rather than silently dropping events or growing without bound.
 *
 * @param dedupCacheSize    Recently delivered event ids retained to de-duplicate the replay-to-live overlap.
 * @param maxBufferedEvents Cap on events buffered from the live feed during the catch-up replay before failing loud.
 */
public record CatchupThenLiveOptions(int dedupCacheSize, int maxBufferedEvents) {

    /** Default de-dup cache size, shared by every catch-up-then-live caller unless overridden. */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    /** Default live-buffer cap, shared by every catch-up-then-live caller unless overridden. */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    public CatchupThenLiveOptions {
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
    }

    /** The default options: {@link #DEFAULT_DEDUP_CACHE_SIZE} and {@link #DEFAULT_MAX_BUFFERED_EVENTS}. */
    public static CatchupThenLiveOptions defaults() {
        return new CatchupThenLiveOptions(DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }
}
