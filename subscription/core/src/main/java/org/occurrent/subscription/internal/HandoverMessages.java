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
 * The wording shared verbatim by every catch-up-then-live caller, kept in one place so the four call sites
 * (the blocking and reactor projection feeds, and the blocking and reactor push subscription models) cannot drift.
 */
public final class HandoverMessages {

    private HandoverMessages() {
    }

    /**
     * The guard message for a {@code PositionOrderedReader} that does not write positions, so the catch-up cannot
     * replay history in position order.
     */
    public static final String POSITIONED_READER_REQUIRED =
            "The reader does not write positions (writesPosition() returns false), so the catch-up cannot replay history in position order. Supply a reader from a positioned event store.";

    /**
     * The live-buffer overflow message, without the emit-result suffix. Used by the blocking handover engine.
     *
     * @param maxBufferedEvents The configured cap, echoed in the message.
     */
    public static String bufferOverflow(int maxBufferedEvents) {
        return "Live event buffer overflowed during catch-up replay (cap "
                + maxBufferedEvents + "). The history is too large to buffer the live feed across a full replay. "
                + "Rebuild offline from the event store instead of catching up over a live feed.";
    }

    /**
     * The live-buffer overflow message with the {@code Emit result: <result>} suffix, describing the
     * {@code Sinks.EmitResult} that failed the {@code tryEmitNext}. Used by the reactive handover engine, which
     * serves both reactor callers.
     *
     * @param maxBufferedEvents The configured cap, echoed in the message.
     * @param emitResult        The {@code Sinks.EmitResult} (or equivalent) to append.
     */
    public static String bufferOverflow(int maxBufferedEvents, Object emitResult) {
        return bufferOverflow(maxBufferedEvents) + " Emit result: " + emitResult;
    }

    /**
     * Rejects a live payload that could not be handed to the reactive engine's sink because another thread held
     * the sink's serialization claim for longer than the engine is willing to keep offering. Not an overflow, and
     * not a catch-up failure, so it says so rather than telling an operator to rebuild a read model offline.
     * <p>
     * Defence rather than a message anything produces today. The reactive engine takes every offer to the sink
     * from one queue, one thread at a time, so it is the sink's only producer and nothing else can hold that
     * claim.
     */
    public static String concurrentEmission() {
        return "Another thread held the live sink's serialization claim for longer than this engine retries, so "
                + "this event was not handed over. Nothing is broken and nothing overflowed. Offer the event "
                + "again.";
    }

    /**
     * The catch-up-failure message a caller shows once a prior catch-up has failed and it can no longer accept live
     * events, with the caller's own noun ("this projection feed" vs. "this subscription") substituted in.
     *
     * @param noun The noun describing what cannot accept live events, e.g. {@code "projection feed"} or
     *             {@code "subscription"}.
     */
    public static String catchUpFailed(String noun) {
        return "Catch-up failed for this " + noun + ", so it refuses live events rather than acknowledging events "
                + "that nothing folded, and the source keeps redelivering them. Fix the cause, then replace it: a "
                + "subscription by cancelling it and subscribing again, a projection feed by building a new one.";
    }

    /**
     * Rejects a null replay-to-live de-dup key. The key function is caller-supplied and declared non-null, but nothing
     * enforces that at runtime, and a null reaches {@code BoundedIdCache} as a null element for its eviction queue,
     * which throws a bare {@link NullPointerException} from inside the cache. On the live path that happens after the
     * fold has already run, so the payload is delivered and the pipeline then fails with no indication of the cause.
     */
    public static String dedupKeyRequired() {
        return "The de-dup key function returned null for a payload. It must return a stable non-null id per event, "
                + "since that id is what suppresses the replay-to-live overlap.";
    }

    /**
     * Rejects a non-default {@code StartAt} passed to a catch-up-then-push subscription model. The model always
     * replays a projection's history from the beginning and then hands over to the live feed, so there is no
     * position for a caller to choose.
     */
    public static final String NON_DEFAULT_START_AT_NOT_SUPPORTED =
            "This subscription model always replays a projection's history from the beginning and then hands over "
                    + "to the live feed, so a caller-supplied startAt has no position to apply to. "
                    + "Use StartAt.subscriptionModelDefault().";
}
