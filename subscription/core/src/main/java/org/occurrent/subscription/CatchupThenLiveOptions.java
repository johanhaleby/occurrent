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
 * A handover keeps two de-dup caches, one holding what its replay delivered and one holding what a live delivery
 * delivered, and this one value sizes each of them. So the ids retained reach twice this number, and raising it costs
 * twice what the number alone suggests
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
 * <p>
 * {@code maxBufferedEvents} is a fail-loud cap, not a throttle. On the blocking stack it caps the live events waiting
 * while the handover is not live, before a catch-up has taken it live and while a replay runs. On the reactor stack it
 * caps every live event taken in and not yet delivered, so it also applies once the handover is live, when delivery
 * falls behind the live feed. Reaching it refuses the next live event rather than letting the buffer grow without
 * bound. On the blocking stack the call that reached it throws.
 * <p>
 * Which calls reach the cap differs by stack. On the blocking stack only the calls that wait or buffer reach it,
 * {@code accept(..)} and {@code acceptAndWait(..)}, since {@code acceptOrDefer(..)} returns {@code DEFERRED} while the
 * handover is not live and buffers nothing. On the reactor stack {@code acceptOrDefer(..)} returns {@code DEFERRED}
 * only while the handover is not live or is paused for a replay. Once the handover is live, its events count toward
 * the cap like any other live event. Reaching the cap errors the returned {@code Mono}, except that a push model's
 * {@code acceptOrDefer(..)} completes it with {@code NOT_DELIVERABLE} instead.
 * <p>
 * What reaching it costs depends on who made the call. A broker listener does not acknowledge the message when its
 * call throws or its {@code Mono} errors, and has the broker deliver it again, as
 * {@link RoutingOutcome.Disposition#HOLD} describes. A push model's
 * {@code NOT_DELIVERABLE} goes to the listener's own failure policy instead. A
 * {@code CatchupThenPushSubscriptionModel}'s {@code accept(..)} is supported only from the in-memory event store's
 * write path. There the write call throws although the store has kept the event. The push model's
 * {@code accept(Iterable)} stops at that event, so it routes none of the write's later events either, and the
 * subscription can miss all of them.
 *
 * @param dedupCacheSize    Recently delivered event ids retained to de-duplicate the replay-to-live overlap, per
 *                          cache, of which a handover keeps two.
 * @param maxBufferedEvents Cap on live events waiting to be delivered before failing loud. On the blocking stack it
 *                          applies while the handover is not live, and on the reactor stack also once it is live.
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
