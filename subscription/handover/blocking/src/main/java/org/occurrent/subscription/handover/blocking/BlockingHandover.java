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

package org.occurrent.subscription.handover.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.handover.HandoverMessages;
import org.occurrent.subscription.handover.HandoverOptions;
import org.occurrent.subscription.internal.BoundedIdCache;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The shared blocking catch-up-then-live coordination: register the live feed first (buffering), replay a source's
 * history in position order, drain the buffer and go live, then mark the catch-up complete. Extracted from (and
 * mirrors exactly) the blocking projection feed and the blocking push subscription model, which each supply their
 * own delivery, de-dup key, and {@link Source} of history and completion-marker.
 * <p>
 * {@code L} is the live payload type and {@code R} is the replay payload type. They are kept separate rather than
 * folded into one type because a replayed event typically carries decoded metadata (for example {@code EventMetadata})
 * that a live event does not, and the two deliveries can be distinct fold overloads on the caller's read model.
 * <p>
 * Note the semantic contract this engine keeps: {@link #accept(Object)} buffers a live payload and returns
 * <em>before</em> it is folded while the catch-up is running, so a caller that acknowledges after {@code accept}
 * returns may acknowledge before the fold actually runs. That is safe only because {@link Source#markCaughtUp()} is
 * called <em>after</em> the buffer is drained (see {@link #catchUp(Source)}), so a crash mid-catch-up re-runs the
 * whole replay from the source, the backstop for any live payload acknowledged but not yet folded.
 */
@NullMarked
public final class BlockingHandover<L, R> {

    /**
     * The replay side of a handover: whether the catch-up already ran, the position-ordered replay stream, and how to
     * record that the catch-up completed.
     */
    public interface Source<R> {
        /** Whether a prior catch-up already completed, so this one should skip straight to going live. */
        boolean isAlreadyCaughtUp();

        /** The history to replay, in position order, from the beginning. Closed by the engine after use. */
        Stream<R> replay();

        /**
         * Record that the catch-up completed. Called after the replay has been consumed and the live buffer has been
         * drained, so an implementation that reads "the current head" reads it <em>after</em> the replay, not before.
         */
        void markCaughtUp();
    }

    /**
     * Thrown by {@link #accept(Object)} once a prior {@link #catchUp(Source)} has failed, wrapping the original
     * failure. Callers with their own wording for this case (the two current callers phrase it differently) should
     * catch this type and re-throw with their own message, using {@link #getCause()} as the original failure.
     */
    public static final class CatchUpFailedException extends RuntimeException {
        public CatchUpFailedException(Throwable cause) {
            super(cause);
        }
    }

    private final Consumer<L> deliverLive;
    private final Function<L, String> liveDedupId;
    private final Consumer<R> deliverReplayed;
    private final Function<R, String> replayDedupId;
    private final int maxBufferedEvents;

    private final Object lock = new Object();
    private final Queue<L> buffer = new ArrayDeque<>();
    private final BoundedIdCache deliveredIds;
    private boolean live = false;
    private @Nullable Throwable catchUpFailure = null;

    private BlockingHandover(Consumer<L> deliverLive, Function<L, String> liveDedupId,
                              Consumer<R> deliverReplayed, Function<R, String> replayDedupId,
                              HandoverOptions options) {
        this.deliverLive = deliverLive;
        this.liveDedupId = liveDedupId;
        this.deliverReplayed = deliverReplayed;
        this.replayDedupId = replayDedupId;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache(options.dedupCacheSize());
    }

    /**
     * @param deliverLive     Folds a live payload (outside the catch-up window, or the buffered overlap once drained).
     * @param liveDedupId     Extracts the replay-to-live de-dup key from a live payload.
     * @param deliverReplayed Folds a replayed payload during the catch-up.
     * @param replayDedupId   Extracts the replay-to-live de-dup key from a replayed payload.
     * @param options         De-dup cache size and live-buffer cap.
     */
    public static <L, R> BlockingHandover<L, R> create(
            Consumer<L> deliverLive, Function<L, String> liveDedupId,
            Consumer<R> deliverReplayed, Function<R, String> replayDedupId,
            HandoverOptions options) {
        Objects.requireNonNull(deliverLive, "deliverLive cannot be null");
        Objects.requireNonNull(liveDedupId, "liveDedupId cannot be null");
        Objects.requireNonNull(deliverReplayed, "deliverReplayed cannot be null");
        Objects.requireNonNull(replayDedupId, "replayDedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        return new BlockingHandover<>(deliverLive, liveDedupId, deliverReplayed, replayDedupId, options);
    }

    /**
     * Feed a live payload. Buffered while the catch-up replay runs, folded directly afterwards, on the calling thread.
     *
     * @throws CatchUpFailedException if a prior {@link #catchUp(Source)} failed.
     * @throws IllegalStateException  if the live buffer overflows during the catch-up.
     */
    public void accept(L payload) {
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new CatchUpFailedException(catchUpFailure);
            }
            if (live) {
                deliverLive(payload);
                return;
            }
            if (buffer.size() >= maxBufferedEvents) {
                throw new IllegalStateException(HandoverMessages.bufferOverflow(maxBufferedEvents));
            }
            buffer.add(payload);
        }
    }

    /**
     * Run the one-time catch-up: replay the source's history (unless already caught up), then drain the buffered live
     * payloads and go live, then mark the catch-up complete.
     */
    public void catchUp(Source<R> source) {
        try {
            if (source.isAlreadyCaughtUp()) {
                drainBufferAndGoLive();
                return;
            }
            try (Stream<R> history = source.replay()) {
                history.forEach(replayed -> {
                    deliverReplayed.accept(replayed);
                    synchronized (lock) {
                        deliveredIds.add(replayDedupId.apply(replayed));
                    }
                });
            }
            drainBufferAndGoLive();
            source.markCaughtUp();
        } catch (RuntimeException e) {
            // Record the failure so a live payload fed after a failed catch-up fails fast instead of buffering until
            // overflow and hiding the error.
            synchronized (lock) {
                catchUpFailure = e;
            }
            throw e;
        }
    }

    private void drainBufferAndGoLive() {
        synchronized (lock) {
            for (L buffered : buffer) {
                deliverLive(buffered);
            }
            buffer.clear();
            live = true;
        }
    }

    // Must be called holding lock. Folds unless the payload was already folded by the replay or an earlier live copy.
    private void deliverLive(L payload) {
        String key = liveDedupId.apply(payload);
        if (deliveredIds.contains(key)) {
            return;
        }
        deliverLive.accept(payload);
        deliveredIds.add(key);
    }
}
