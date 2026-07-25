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
     * The catch-up-failure message a caller shows once a prior catch-up has failed and it can no longer accept live
     * events, with the caller's own noun ("this projection feed" vs. "this subscription") substituted in.
     *
     * @param noun The noun describing what cannot accept live events, e.g. {@code "projection feed"} or
     *             {@code "subscription"}.
     */
    public static String catchUpFailed(String noun) {
        return "Catch-up failed for this " + noun + ", so it cannot accept live events. Rebuild it after fixing the cause.";
    }
}
