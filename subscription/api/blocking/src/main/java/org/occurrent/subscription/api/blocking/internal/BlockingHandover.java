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
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.HandoverMessages;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
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
 * The supplied {@code deliver} is always invoked outside this engine's monitor: for the replay fold, for the buffer
 * drain, and for a live {@link #accept(Object)}. Only the dedup-key reservation that decides whether a given payload
 * is delivered at all happens under the lock. This means a caller that feeds this engine from more than one thread
 * once it is live (a listener container with concurrency &gt; 1, say) gets genuinely concurrent {@code deliver} calls
 * rather than calls queued behind one global lock, so {@code deliver} must tolerate concurrent invocation and cannot
 * rely on this engine to serialize it (<a href="https://github.com/johanhaleby/occurrent/issues/588">#588</a>).
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
         * Whether the replay should keep going, asked once per payload before it is folded, and once more after the
         * last one, right before {@link #markCaughtUp()} would otherwise run. Return {@code false} to stop one
         * already in flight, because the model was stopped or is shutting down, or because whatever identifies this
         * attempt (a subscription id, say) has since been reassigned to a different one.
         * <p>
         * A stop is not a failure. Nothing is drained, the handover does not go live, {@link #markCaughtUp()} is not
         * called, and no failure is recorded, so the next catch-up replays the whole history and the handover stays
         * usable. Live payloads arriving after a stop are dropped rather than buffered, the same dropped-not-deferred
         * contract a stopped subscription model has (ADR 85). The final, post-loop check exists because the
         * per-payload one only ever runs before a fold, never after the last one: an attempt whose ownership lapses
         * while that last fold is still running would otherwise reach {@link #markCaughtUp()} for a history its
         * current owner never actually folded.
         */
        default boolean keepReplaying() {
            return true;
        }

        /**
         * The replay is about to start delivering events. Called once, before the first {@link #replay()} item is
         * folded, and only when a replay actually runs (not on a restart that skips straight to
         * {@link #isAlreadyCaughtUp()}). The default does nothing, so a source with no replay-aware view pays nothing
         * for this hook (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
         */
        default void replayStarted() {
        }

        /**
         * The replay finished folding every event. Called after the last {@link #replay()} item has been folded and
         * before the live buffer is drained, so anything a replay-aware view buffered is durable before a drained live
         * payload is folded and before {@link #markCaughtUp()} runs.
         */
        default void replayCompleted() {
        }

        /**
         * The replay was stopped before it finished, that is, {@link #keepReplaying()} returned {@code false}. Called
         * instead of {@link #replayCompleted()}, so anything buffered since {@link #replayStarted()} is discarded
         * rather than written, the same discard-on-stop contract {@link #keepReplaying()} documents. Must not throw.
         */
        default void replayAbandoned() {
        }
    }

    /**
     * Thrown by {@link #acceptReportingDelivery(Object)} and {@link #acceptIfLive(Object)} for a refusal decided
     * before any dispatch was attempted, a permanently failed catch-up, a full live buffer with nothing draining
     * it, or a {@code dedupId} function that returned {@code null} for the payload, none of them a delivery.
     * Distinct from any other {@link IllegalStateException} either method can throw, in particular one a delivered
     * payload's own handler throws, so a caller that needs to tell those apart can catch this type specifically
     * instead of classifying every {@link IllegalStateException} alike.
     */
    public static final class PreDispatchRefusalException extends IllegalStateException {
        PreDispatchRefusalException(String message) {
            super(message);
        }

        PreDispatchRefusalException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final Consumer<T> deliver;
    private final Function<T, String> dedupId;
    private final int maxBufferedEvents;
    private final String noun;

    private final Object lock = new Object();
    private final Queue<T> buffer = new ArrayDeque<>();
    private final BoundedIdCache deliveredIds;
    // Dedup keys currently being delivered outside the lock, so a second concurrent delivery of the same key waits
    // for neither: it is dropped rather than raced, and the first attempt's own success or failure is what decides
    // deliveredIds. Without this a key could be marked delivered before deliver.accept(payload) actually succeeds,
    // and a delivery that then throws would leave a broker redelivery of the same payload silently skipped.
    private final Set<String> inFlight = new HashSet<>();
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
     * @param deliver Folds a payload, replayed or live. Always called outside this engine's monitor (see the class
     *                javadoc), so it must tolerate concurrent invocation once the handover is live.
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
     * <p>
     * A payload fed after a failed catch-up is refused rather than accepted, and stays refused: the caller
     * acknowledges once this returns, so returning normally would acknowledge a payload nothing handled. Recovery is
     * the caller's to choose, not this engine's (ADR 104).
     * <p>
     * Once live, {@code deliver} runs outside this engine's monitor (see the class javadoc), so a concurrent caller
     * gets a concurrent {@code deliver} call, not one queued behind another payload's fold.
     *
     * @throws PreDispatchRefusalException if a prior {@link #catchUp(Source)} has failed, or if the live buffer
     *                                overflows during the catch-up.
     */
    public void accept(T payload) {
        acceptReportingDelivery(payload);
    }

    /**
     * As {@link #accept(Object)}, additionally reporting whether the payload was genuinely handled (buffered for the
     * replay to drain, delivered live, or already delivered by an earlier attempt) rather than silently dropped
     * because a replay that would have drained it was stopped. A caller that acknowledges an externally sourced
     * payload (a broker message, say) needs this to tell the two apart, since {@link #accept(Object)} returns
     * normally either way.
     *
     * @return {@code false} when this handover is stopped and the payload was dropped rather than buffered or
     *         delivered, or when a concurrent delivery of the same payload is already running and this call is not
     *         the one deciding whether it succeeds. {@code true} otherwise, including a de-duplicated repeat of a
     *         payload an earlier attempt already delivered.
     * @throws PreDispatchRefusalException for the same reasons {@link #accept(Object)} does.
     */
    public boolean acceptReportingDelivery(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        String deliverKey = null;
        boolean dropped = false;
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new PreDispatchRefusalException(HandoverMessages.catchUpFailed(noun), catchUpFailure);
            }
            if (live) {
                String key = dedupKey(payload);
                if (deliveredIds.contains(key)) {
                    // An earlier attempt already delivered this key, so this call reports it delivered without
                    // redelivering.
                } else if (!inFlight.add(key)) {
                    // A concurrent delivery of the same key is running right now, outside this lock, and its own
                    // success or failure is what decides deliveredIds (see the inFlight field javadoc). This call
                    // cannot wait for that attempt without blocking under the lock, and reporting delivered would
                    // let a caller acknowledge a payload whose actual delivery has not succeeded yet, and never
                    // will if that attempt throws. Reporting it dropped is always safe to retry, since a
                    // redelivery of the same key lands on deliveredIds once the in-flight attempt actually finishes.
                    dropped = true;
                } else {
                    deliverKey = key;
                }
            } else if (stopped) {
                // Dropped rather than buffered: the replay that would have drained this buffer was stopped, so
                // nothing is coming to fold it and buffering would just fill up and overflow.
                dropped = true;
            } else if (buffer.size() >= maxBufferedEvents) {
                throw new PreDispatchRefusalException(HandoverMessages.bufferOverflow(maxBufferedEvents));
            } else {
                buffer.add(payload);
            }
        }
        if (deliverKey != null) {
            deliverOutsideLock(payload, deliverKey);
        }
        return !dropped;
    }

    /**
     * As {@link #acceptReportingDelivery(Object)}, except a payload that would only buffer is refused instead:
     * returned {@code false} without ever being added to the buffer. For a caller that can redeliver the same
     * payload later, a buffered payload is strictly worse than a refused one, since a buffered payload has already
     * been reported handled by the time this returns, while a refused one has not, and can safely be offered again.
     * <p>
     * A payload fed while this handover is stopped is refused the same way, for the same reason: nothing is
     * currently draining a buffer for it to wait in.
     *
     * @return {@code true} once the payload has genuinely landed: delivered live just now, or already delivered by
     *         an earlier attempt. {@code false} whenever this call did not deliver it, whether because this
     *         handover is not live yet, is stopped, or a concurrent delivery of the same payload is already
     *         running elsewhere. Every {@code false} is safe to retry: a redelivery lands on {@code deliveredIds}
     *         once whatever is holding it up resolves.
     * @throws PreDispatchRefusalException for the same reasons {@link #accept(Object)} does. Checked first, before
     *         the live check, so a payload fed after a permanently failed catch-up fails fast rather than reporting
     *         {@code false} forever for a caller to retry a catch-up that is never coming back.
     */
    public boolean acceptIfLive(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        String deliverKey = null;
        boolean landed;
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new PreDispatchRefusalException(HandoverMessages.catchUpFailed(noun), catchUpFailure);
            }
            if (!live) {
                // Refuse without buffering, unlike acceptReportingDelivery. Covers "never started", "still
                // replaying", and "stopped mid-replay" alike, all three are "not live", and a caller here has
                // already promised it can redeliver, so there is nothing to gain by holding the payload instead of
                // asking again later.
                landed = false;
            } else {
                String key = dedupKey(payload);
                if (deliveredIds.contains(key)) {
                    landed = true;
                } else if (!inFlight.add(key)) {
                    landed = false;
                } else {
                    deliverKey = key;
                    landed = true;
                }
            }
        }
        if (deliverKey != null) {
            deliverOutsideLock(payload, deliverKey);
        }
        return landed;
    }

    /**
     * Whether a live payload fed right now would actually be delivered, immediately and synchronously, rather than
     * buffered against a replay's own drain, refused outright, or silently dropped. True only once
     * {@link #catchUp(Source)} has reached live. False before that, whether {@link #catchUp(Source)} has never been
     * called, is still replaying, or was stopped mid-replay by {@link Source#keepReplaying()}, and false forever
     * after a {@link #catchUp(Source)} attempt has thrown, since the failure it records is never cleared, not even
     * by a later {@link #catchUp(Source)} call that itself reaches live.
     * <p>
     * The one fact this deliberately does not answer is whether a currently buffering payload is safe against a
     * crash. It is, while an actual replay is what will drain that buffer, since nothing is recorded complete until
     * after the drain and a crash simply replays the same history again. This method reads {@code false} for that
     * case anyway, the same as it does before anything has started, because this handover keeps no separate record
     * of "a replay is in flight" for it to report, only whether it is live and whether it has permanently failed.
     * A caller that means to distinguish a store-backed buffer from one with nothing behind it needs its own signal
     * for that, this is not it.
     */
    public boolean isReadyForLiveDelivery() {
        synchronized (lock) {
            return live && catchUpFailure == null;
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
        // Tracks whether replayStarted() ran and replayCompleted() has not yet closed it out, so the catch block below
        // knows whether there is a replay lifecycle left open to abandon, rather than calling replayAbandoned() after
        // a clean replayCompleted() has already told the view its batch is durable.
        boolean replayOpen = false;
        try {
            if (source.isAlreadyCaughtUp()) {
                drainBufferAndGoLive();
                return true;
            }
            boolean stoppedMidReplay = false;
            source.replayStarted();
            replayOpen = true;
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
            // Checked again here, once more, even though the loop above already checks it before every fold: that
            // check runs before each payload, never after the last one, so a stop or a cancelled attempt landing
            // while the final fold is still in flight would otherwise reach markCaughtUp() unnoticed, for a
            // history the id's current owner never folded. Reusing keepReplaying() for this, rather than reading
            // it as "only asked before a fold", is deliberate: whatever it means to no longer own the replay, it
            // means the exact same thing whether that is discovered before a fold or right after the last one.
            if (!stoppedMidReplay && !source.keepReplaying()) {
                stoppedMidReplay = true;
            }
            if (stoppedMidReplay) {
                // No drain, no going live, and no marker. Recording completion here is the one thing that would make
                // the next start skip a history it never finished folding.
                abandonReplayWithoutMasking(source);
                synchronized (lock) {
                    stopped = true;
                }
                return false;
            }
            // Ordered before the live buffer drain and before markCaughtUp(), so anything a replay-aware view
            // buffered is durable before either runs (ADR 110).
            source.replayCompleted();
            replayOpen = false;
            drainBufferAndGoLive();
            source.markCaughtUp();
            return true;
        } catch (RuntimeException | Error e) {
            // Record the failure so a live payload fed after a failed catch-up fails fast instead of buffering until
            // overflow and hiding the error.
            //
            // Error is recorded alongside RuntimeException, not only rethrown. Its callers no longer release the
            // registration when a catch-up fails (ADR 104), so a failure this engine does not record leaves a handover
            // that keeps buffering live payloads and returning normally, which acknowledges them into a replay that is
            // never coming back. That is the loss the refusal exists to prevent, and something like a
            // NoClassDefFoundError out of the fold is exactly how it would arrive.
            if (replayOpen) {
                abandonReplayWithoutMasking(source);
            }
            synchronized (lock) {
                catchUpFailure = e;
            }
            throw e;
        }
    }

    // Guarded so that a source's own replayAbandoned() throwing cannot replace the failure (or stop) that made the
    // engine call it in the first place; the contract asks the source not to throw here, but this engine does not
    // trust that.
    private void abandonReplayWithoutMasking(Source<T> source) {
        try {
            source.replayAbandoned();
        } catch (RuntimeException | Error ignored) {
        }
    }

    private void drainBufferAndGoLive() {
        List<T> toDeliver;
        List<String> keysToDeliver;
        synchronized (lock) {
            toDeliver = new ArrayList<>(buffer.size());
            keysToDeliver = new ArrayList<>(buffer.size());
            for (T buffered : buffer) {
                String key = tryReserve(buffered);
                if (key != null) {
                    toDeliver.add(buffered);
                    keysToDeliver.add(key);
                }
            }
            buffer.clear();
            live = true;
        }
        // Outside the monitor, same as a live accept(Object) (#588). Still sequential on this thread, so catchUp's
        // markCaughtUp() call after this method returns is still ordered after every one of these deliveries, and a
        // delivery that throws here still reaches catchUp's own catch block exactly as it did before this method
        // stopped holding the lock for the delivery itself.
        for (int i = 0; i < toDeliver.size(); i++) {
            deliverOutsideLock(toDeliver.get(i), keysToDeliver.get(i));
        }
    }

    @SuppressWarnings("ConstantValue") // The function is declared non-null, but it is caller-supplied and unenforced.
    private String dedupKey(T payload) {
        String key = dedupId.apply(payload);
        if (key == null) {
            throw new PreDispatchRefusalException(HandoverMessages.dedupKeyRequired());
        }
        return key;
    }

    // Must be called holding lock. Returns the key to deliver under, or null when nothing should be delivered:
    // the replay (or an earlier live copy) already delivered this payload, or another thread is delivering it
    // right now. Does not itself record the payload as delivered (#588): a delivery that throws must not poison
    // a later legitimate redelivery of the same payload, so deliveredIds is only updated once deliverOutsideLock
    // knows whether the call actually succeeded.
    private @Nullable String tryReserve(T payload) {
        String key = dedupKey(payload);
        if (deliveredIds.contains(key) || !inFlight.add(key)) {
            return null;
        }
        return key;
    }

    // Runs deliver outside the lock, then reports the outcome back under it: success moves the key from in-flight
    // to delivered, failure only clears the in-flight marker, so a payload whose delivery threw is not recorded and
    // a later redelivery is free to try again.
    private void deliverOutsideLock(T payload, String key) {
        boolean succeeded = false;
        try {
            deliver.accept(payload);
            succeeded = true;
        } finally {
            synchronized (lock) {
                inFlight.remove(key);
                if (succeeded) {
                    deliveredIds.add(key);
                }
            }
        }
    }
}
