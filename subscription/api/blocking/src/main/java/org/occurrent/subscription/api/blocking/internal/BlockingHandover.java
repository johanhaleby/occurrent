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

package org.occurrent.subscription.api.blocking.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.CatchupThenLiveOptions;

import java.util.ArrayDeque;
import java.util.Iterator;
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
 * {@code T} is the payload type, one for both phases. The caller decides what a payload carries, so where a replayed
 * payload has metadata a live one may not, that difference lives in the payload rather than in this engine's signature.
 * <p>
 * The supplied {@code deliver} is invoked on <em>both</em> sides of this engine's monitor: outside it for the replay
 * fold, and while holding it for the drain and for a live {@link #accept(Object)}. It must tolerate both.
 * <p>
 * Note the semantic contract this engine keeps: {@link #accept(Object)} buffers a live payload and returns
 * <em>before</em> it is folded while the catch-up is running, so a caller that acknowledges after {@code accept}
 * returns may acknowledge before the fold actually runs. That is safe only because {@link Source#markCaughtUp()} is
 * called <em>after</em> the buffer is drained (see {@link #catchUp(Source)}), so a crash mid-catch-up re-runs the
 * whole replay from the source, the backstop for any live payload acknowledged but not yet folded.
 */
@NullMarked
public final class BlockingHandover<T> {

    /**
     * The replay side of a handover: whether the catch-up already ran, the position-ordered replay stream, and how to
     * record that the catch-up completed.
     */
    public interface Source<T> {
        /** Whether a prior catch-up already completed, so this one should skip straight to going live. */
        boolean isAlreadyCaughtUp();

        /** The history to replay, in position order, from the beginning. Closed by the engine after use. */
        Stream<T> replay();

        /**
         * Record that the catch-up completed. Called after the replay has been consumed and the live buffer has been
         * drained, so an implementation that reads "the current head" reads it <em>after</em> the replay, not before.
         */
        void markCaughtUp();

        /**
         * Whether the replay should keep going, asked once per payload before it is folded. Return {@code false} to
         * stop one already in flight, because the model was stopped or is shutting down.
         * <p>
         * A stop is not a failure. Nothing is drained, the handover does not go live, {@link #markCaughtUp()} is not
         * called, and no failure is recorded, so the next catch-up replays the whole history and the handover stays
         * usable. Live payloads arriving after a stop are dropped rather than buffered, the same dropped-not-deferred
         * contract a stopped subscription model has (ADR 85).
         */
        default boolean keepReplaying() {
            return true;
        }
    }

    private final Consumer<T> deliver;
    private final Function<T, String> dedupId;
    private final int maxBufferedEvents;
    private final String noun;

    private final Object lock = new Object();
    private final Queue<T> buffer = new ArrayDeque<>();
    private final BoundedIdCache deliveredIds;
    private boolean live = false;
    private boolean stopped = false;
    private @Nullable Throwable catchUpFailure = null;

    private BlockingHandover(Consumer<T> deliver, Function<T, String> dedupId, CatchupThenLiveOptions options, String noun) {
        this.deliver = deliver;
        this.dedupId = dedupId;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache(options.dedupCacheSize());
        this.noun = noun;
    }

    /**
     * @param deliver Folds a payload, replayed or live. Called outside this engine's monitor for the replay and while
     *                holding it for the drain and for a live {@link #accept(Object)}, so it must tolerate both.
     * @param dedupId Extracts the replay-to-live de-dup key from a payload.
     * @param options De-dup cache size and live-buffer cap.
     * @param noun    The caller's noun for {@link HandoverMessages#catchUpFailed(String)}, e.g.
     *                {@code "projection feed"} or {@code "subscription"}.
     */
    public static <T> BlockingHandover<T> create(
            Consumer<T> deliver, Function<T, String> dedupId, CatchupThenLiveOptions options, String noun) {
        Objects.requireNonNull(deliver, "deliver cannot be null");
        Objects.requireNonNull(dedupId, "dedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(noun, "noun cannot be null");
        return new BlockingHandover<>(deliver, dedupId, options, noun);
    }

    /**
     * Feed a live payload. Buffered while the catch-up replay runs, folded directly afterwards, on the calling thread.
     *
     * @throws IllegalStateException if a prior {@link #catchUp(Source)} has failed, or if the live buffer overflows
     *                                during the catch-up.
     */
    public void accept(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new IllegalStateException(HandoverMessages.catchUpFailed(noun), catchUpFailure);
            }
            if (live) {
                deliverLive(payload);
                return;
            }
            if (stopped) {
                // Dropped rather than buffered: the replay that would have drained this buffer was stopped, so nothing
                // is coming to fold it and buffering would just fill up and overflow.
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
     *
     * @return {@code true} when the catch-up finished and the handover is live, {@code false} when
     * {@link Source#keepReplaying()} stopped it partway. A failure throws rather than returning either.
     */
    public boolean catchUp(Source<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        synchronized (lock) {
            // A fresh catch-up revives a handover a previous one stopped, so stopping is recoverable by replaying
            // again rather than only by building a new one.
            stopped = false;
        }
        try {
            if (source.isAlreadyCaughtUp()) {
                drainBufferAndGoLive();
                return true;
            }
            boolean stoppedMidReplay = false;
            try (Stream<T> history = source.replay()) {
                Iterator<T> replaying = history.iterator();
                while (replaying.hasNext()) {
                    // Checked before the fold rather than after, so a stop takes effect on the payload it arrived for
                    // rather than one later.
                    if (!source.keepReplaying()) {
                        stoppedMidReplay = true;
                        break;
                    }
                    T replayed = replaying.next();
                    // Outside the monitor on purpose: only the cache write needs it, neither the caller's fold nor its
                    // key function.
                    String key = dedupKey(replayed);
                    deliver.accept(replayed);
                    synchronized (lock) {
                        deliveredIds.add(key);
                    }
                }
            }
            if (stoppedMidReplay) {
                // No drain, no going live, and no marker. Recording completion here is the one thing that would make
                // the next start skip a history it never finished folding.
                synchronized (lock) {
                    stopped = true;
                }
                return false;
            }
            drainBufferAndGoLive();
            source.markCaughtUp();
            return true;
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
            for (T buffered : buffer) {
                deliverLive(buffered);
            }
            buffer.clear();
            live = true;
        }
    }

    @SuppressWarnings("ConstantValue") // The function is declared non-null, but it is caller-supplied and unenforced.
    private String dedupKey(T payload) {
        String key = dedupId.apply(payload);
        if (key == null) {
            throw new IllegalStateException(HandoverMessages.dedupKeyRequired());
        }
        return key;
    }

    // Must be called holding lock. Folds unless the payload was already folded by the replay or an earlier live copy.
    private void deliverLive(T payload) {
        String key = dedupKey(payload);
        if (deliveredIds.contains(key)) {
            return;
        }
        deliver.accept(payload);
        deliveredIds.add(key);
    }
}
