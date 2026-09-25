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
 * De-dup is two caches, not one. The replay records what it delivered in one, a live delivery records what it
 * delivered in the other, and every check reads both, so a payload is delivered once either way. What the two
 * caches decide is what happens to the copy that is not delivered. One the replay already delivered reaches
 * {@link Source#alreadyDeliveredByReplay(Object)}, because the replay ran inside the source's history phase and a
 * source that writes something down per delivery has written nothing down for it yet. One an earlier live delivery
 * already handled reaches nothing, because that delivery did all of it
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
 * <p>
 * {@link #accept(Object)} reports a live payload handled only once {@code deliver} has applied it, or had already
 * applied it. One fed while the handover is not live waits in the buffer, and the call waits with it until the drain
 * applies it. A stop, a failed catch-up or an interrupt ends the wait with an exception, so a caller that
 * acknowledges on return never acknowledges a payload that is only held in memory.
 * {@link #acceptReportingDelivery(Object)} does not wait, and reports a buffered payload handled before the drain
 * applies it. It is for the write path of the store the replay reads, which its javadoc covers.
 */
@NullMarked
public final class BlockingHandover<T, K> {

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
         * A stop is not a failure. {@link #markCaughtUp()} is not called and no failure is recorded, so the next
         * catch-up replays the whole history and the handover stays usable.
         * <p>
         * What the stop does with the live payloads depends on where the handover stood when the replay started. One
         * that had not gone live drains nothing and does not go live. Every payload a caller is waiting on in
         * {@link BlockingHandover#accept(Object)} is answered as not applied, so that call throws. Live payloads
         * arriving after the stop are not buffered, so {@link BlockingHandover#accept(Object)} throws for them too
         * and {@link BlockingHandover#acceptReportingDelivery(Object)} returns {@code false}. A payload
         * {@link BlockingHandover#acceptReportingDelivery(Object)} buffered before the stop stays in the buffer for
         * the next catch-up. One that was already live
         * delivers what it buffered while the replay ran and goes on delivering, see
         * {@link BlockingHandover#catchUp}. The final, post-loop check exists because the
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
         * <p>
         * Before this call the engine forgets every de-dup key the replay delivered, so a later live copy of one of
         * those payloads is delivered rather than suppressed. A source that wrote the payload through receives it
         * twice, which at-least-once delivery allows, and one that discarded it receives it again.
         */
        default void replayAbandoned() {
        }

        /**
         * The history this catch-up was going to read has been read, and the live payloads buffered while it ran are
         * about to be delivered. Called immediately before every drain, including the one for a source that was
         * already caught up and replayed nothing, which is what makes it different from {@link #replayCompleted()}.
         * <p>
         * A buffered payload is delivered exactly once and never again, so a source that reports its own catch-up
         * phase has to stop calling this part of the work a replay before the drain rather than after it
         * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
         * decision 6). The default does nothing.
         */
        default void historyDone() {
        }

        /**
         * A live payload arrived whose de-dup key the replay already delivered, so this engine did not deliver it a
         * second time. Called once per such payload, outside this engine's monitor, on whichever thread fed the
         * payload, the drain thread for one that buffered during the replay and the caller's own thread for one
         * that arrived after the handover went live.
         * <p>
         * The replay delivered the payload, and it is delivered once either way, so nothing here should deliver it
         * again. This hook exists for a recording projection, which writes down the append a payload came from once
         * it has applied it. It writes nothing during the replay, which runs inside the history phase
         * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
         * decision 6), and the live copy is never delivered
         * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>),
         * so without this call neither delivery writes the append down. Delivered is not applied, since a projection
         * can skip an event, so a source that records has to check that its replay applied an event of that append.
         * <p>
         * A payload the replay never delivered, one suppressed because an earlier live delivery already handled it,
         * does not come here. That earlier delivery ran everything a delivery runs, so there is nothing left owing.
         * <p>
         * Called again for every further copy of the same payload, since a source that redelivers gets one call per
         * redelivery. The default does nothing.
         *
         * @param payload The live payload that was not delivered.
         */
        default void alreadyDeliveredByReplay(T payload) {
        }
    }

    /**
     * Thrown by {@link #acceptReportingDelivery(Object)} and {@link #acceptIfLive(Object)} for a refusal decided
     * before any dispatch was attempted, a permanently failed catch-up, a full live buffer with nothing draining
     * it, or a {@code dedupId} function that returned {@code null} for the payload, none of them a delivery. Also
     * thrown to a caller waiting for the drain to apply its payload when the catch-up fails, when the caller is
     * interrupted, or when it fed the payload from inside this handover's own replay, and by {@link #accept(Object)}
     * for every payload {@link #acceptReportingDelivery(Object)} would report {@code false} for. None of those
     * payloads was reported handled, so the caller offers it again.
     * Distinct from any other {@link IllegalStateException} either method can throw, in particular one a delivered
     * payload's own handler throws, so a caller that needs to tell those apart can catch this type specifically
     * instead of classifying every {@link IllegalStateException} alike.
     */
    public static final class PreDispatchRefusalException extends IllegalStateException {
        private final BlockingHandover<?, ?> owner;

        PreDispatchRefusalException(BlockingHandover<?, ?> owner, String message) {
            super(message);
            this.owner = owner;
        }

        PreDispatchRefusalException(BlockingHandover<?, ?> owner, String message, Throwable cause) {
            super(message, cause);
            this.owner = owner;
        }

        /**
         * Whether {@code handover} is the engine that threw this. A handler that reenters a second handover lets
         * that one's refusal escape unwrapped through the first, so a caller that means "my own engine refused"
         * has to compare identity rather than catch the type.
         *
         * @param handover The engine to compare against.
         */
        public boolean thrownBy(BlockingHandover<?, ?> handover) {
            return owner == handover;
        }
    }

    private final Consumer<T> deliver;
    private final Function<T, K> dedupId;
    private final int maxBufferedEvents;
    private final String noun;

    private final Object lock = new Object();
    private final Queue<Held<T>> buffer = new ArrayDeque<>();
    // Two caches rather than one, because the two suppressions they cause are not the same event. A key in
    // deliveredIds was delivered live, so suppressing its repeat is a plain no-op. A key in replayedIds was delivered
    // by the replay, inside the history phase, so suppressing the live copy owes the source a call to
    // Source.alreadyDeliveredByReplay(..) (ADR 137). One cache cannot tell those apart, and the replay's own volume
    // evicting the live keys is what made the live-redelivery de-dup empty exactly when the handover went live.
    private final BoundedIdCache<K> deliveredIds;
    private final BoundedIdCache<K> replayedIds;
    // Dedup keys currently being delivered outside the lock, so a second concurrent delivery of the same key waits
    // for neither: it is dropped rather than raced, and the first attempt's own success or failure is what decides
    // deliveredIds. Without this a key could be marked delivered before deliver.accept(payload) actually succeeds,
    // and a delivery that then throws would leave a broker redelivery of the same payload silently skipped.
    private final Set<K> inFlight = new HashSet<>();
    private boolean live = false;
    private boolean stopped = false;
    // While a replay runs, live payloads buffer even on a handover that was already live, because a view that buffers
    // during a replay throws that buffer away if the replay is stopped.
    private boolean replayRunning = false;
    // Whether a stop of the running replay goes live rather than leaving the handover stopped. True when the handover
    // was live before that replay started, or when a catch-up with nothing to replay arrived while it ran.
    private boolean liveWhenReplayStops = false;
    // Held by the catch-up whose replay is running, from the moment the replay starts until that catch-up returns or
    // throws, so its drain, its marker and its failure handling all finish before another replay starts. Separate from
    // replayRunning, which ends when the drain starts, since the drain, the marker and the catch block read and write
    // the replay state this attempt set up.
    private boolean replayTurnHeld = false;
    // The thread of the catch-up holding the replay turn. A payload it feeds while the handover is not live would wait
    // for a drain only it can run, so it is refused instead.
    private @Nullable Thread replayTurnThread = null;
    // How many catchUp(Source) calls are running, counted from the first thing each one does. An interrupted call
    // marks the handover stopped only when it is the only one, since every other state where another call owns this
    // handover (a replay running, a replay waiting for its turn, a catch-up with nothing to replay part way through
    // going live) is a count above one, and asking the count cannot miss one of them the way a flag per state did.
    private int catchUpsInProgress = 0;
    // Source.alreadyDeliveredByReplay(..) calls running outside the lock, waited for before a replay starts.
    private int replayCallbacksRunning = 0;
    // How many catch-ups with nothing to replay are taking this handover live right now. A replay waits for all of
    // them, so the two never write to the view at the same time. A count rather than a flag, since two such calls can
    // overlap and the first to finish would otherwise release a replay while the second is still delivering.
    private int liveTransitionsRunning = 0;
    private @Nullable Throwable catchUpFailure = null;
    // The source whose replay filled replayedIds, so a live payload that replay already delivered can reach it, since
    // accept(..) is handed no source of its own. Written when a replay starts rather than by every catchUp(Source), so
    // a catch-up that replays nothing leaves it in place, and cleared with replayedIds when a replay is abandoned.
    private @Nullable Source<T> source = null;

    private BlockingHandover(Consumer<T> deliver, Function<T, K> dedupId, CatchupThenLiveOptions options, String noun) {
        this.deliver = deliver;
        this.dedupId = dedupId;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache<>(options.dedupCacheSize());
        this.replayedIds = new BoundedIdCache<>(options.dedupCacheSize());
        this.noun = noun;
    }

    /**
     * @param deliver Folds a payload, replayed or live. Always called outside this engine's monitor (see the class
     *                javadoc), so it must tolerate concurrent invocation once the handover is live.
     * @param dedupId Extracts the replay-to-live de-dup key from a payload. Two payloads count as one only when their
     *                keys are equal, so the key has to hold everything that identifies a payload.
     * @param options De-dup cache size and live-buffer cap. The cache size sizes each of the two de-dup caches, one
     *                for what the replay delivered and one for what a live delivery did (ADR 137).
     * @param noun    The caller's noun for {@link HandoverMessages#catchUpFailed(String)}, e.g.
     *                {@code "projection feed"} or {@code "subscription"}.
     */
    public static <T, K> BlockingHandover<T, K> create(
            Consumer<T> deliver, Function<T, K> dedupId, CatchupThenLiveOptions options, String noun) {
        Objects.requireNonNull(deliver, "deliver cannot be null");
        Objects.requireNonNull(dedupId, "dedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(noun, "noun cannot be null");
        return new BlockingHandover<>(deliver, dedupId, options, noun);
    }

    /**
     * Feed a live payload and return once {@code deliver} has applied it. Folded directly on the calling thread once
     * the handover is live. Before that, while a catch-up replay runs or before one has started, the payload waits in
     * the buffer and this call waits with it until the drain applies it, so a caller that acknowledges once this
     * returns never acknowledges a payload that is only held in memory.
     * <p>
     * Throws rather than returning when the payload was not applied, because the replay was stopped before the
     * handover went live, the catch-up failed, the calling thread was interrupted, or this handover is stopped. Recovery is the
     * caller's to choose, not this engine's (ADR 104), and for a broker listener it means not acknowledging, so the
     * broker delivers the payload again. A payload fed after a failed catch-up is refused the same way, and stays
     * refused.
     * <p>
     * A long replay can keep this call waiting for minutes. A Kafka consumer waiting past its
     * {@code max.poll.interval.ms}, five minutes by default, is taken out of its group and the record is delivered
     * again, which costs a redelivery rather than the event. Never call this from the thread that runs
     * {@link #catchUp(Source)} before that call, since nothing else would drain the buffer.
     * <p>
     * Once live, {@code deliver} runs outside this engine's monitor (see the class javadoc), so a concurrent caller
     * gets a concurrent {@code deliver} call, not one queued behind another payload's fold.
     *
     * @throws PreDispatchRefusalException if the payload was not applied for any of the reasons above, if the live
     *                                     buffer overflows during the catch-up, or if a delivery of the same payload
     *                                     is already running on another thread.
     */
    public void accept(T payload) {
        if (!offer(payload, true)) {
            throw new PreDispatchRefusalException(this, HandoverMessages.notApplied(noun));
        }
    }

    /**
     * Feed a live payload without waiting for the drain, reporting whether it was delivered live, buffered for the
     * drain after the replay, or already delivered by an earlier attempt, rather than dropped because this handover
     * is stopped. Unlike {@link #accept(Object)}, a buffered payload is reported {@code true} before {@code deliver}
     * has applied it. That is only for the write path of the store the replay reads, where the payload is already
     * stored. A crash during a replay leaves the catch-up marker unwritten, since {@link Source#markCaughtUp()} runs
     * only after the drain, and the next start replays the payload from the store. A payload from anywhere else goes
     * through {@link #accept(Object)}, which waits until it is applied.
     *
     * @return {@code false} when this handover is stopped and the payload was dropped rather than buffered or
     *         delivered, or when a concurrent delivery of the same payload is already running and this call is not
     *         the one deciding whether it succeeds. {@code true} otherwise, including a de-duplicated repeat of a
     *         payload an earlier attempt already delivered.
     * @throws PreDispatchRefusalException if a prior {@link #catchUp(Source)} has failed, or if the live buffer
     *                                     overflows during the catch-up.
     */
    public boolean acceptReportingDelivery(T payload) {
        return offer(payload, false);
    }

    private boolean offer(T payload, boolean waitUntilApplied) {
        Objects.requireNonNull(payload, "payload cannot be null");
        K deliverKey = null;
        Source<T> replayedBy = null;
        boolean dropped = false;
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), catchUpFailure);
            }
            if (live) {
                K key = dedupKey(payload);
                if (deliveredIds.contains(key)) {
                    // An earlier attempt already delivered this key, so this call reports it delivered without
                    // redelivering.
                } else if (replayedIds.contains(key)) {
                    // The replay delivered this key, so the payload is applied and must not be applied again. The
                    // source is still owed the call below, outside the lock.
                    replayedBy = source;
                    if (replayedBy != null) {
                        replayCallbacksRunning++;
                    }
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
                throw new PreDispatchRefusalException(this, HandoverMessages.bufferOverflow(maxBufferedEvents));
            } else if (!waitUntilApplied) {
                buffer.add(new Held<>(payload, false));
            } else if (Thread.currentThread() == replayTurnThread) {
                throw new PreDispatchRefusalException(this, HandoverMessages.acceptedFromOwnReplay(noun));
            } else {
                Held<T> held = new Held<>(payload, true);
                buffer.add(held);
                return awaitAnswerUnderLock(held);
            }
        }
        if (deliverKey != null) {
            deliverOutsideLock(payload, deliverKey);
        } else if (replayedBy != null) {
            reportAlreadyDeliveredByReplay(replayedBy, payload);
        }
        return !dropped;
    }

    // Assumes lock is held. Releases it while it waits, since the drain, a stop and a failure all answer under it.
    private boolean awaitAnswerUnderLock(Held<T> held) {
        while (held.answer == Answer.PENDING) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (held.answer == Answer.PENDING) {
                    // Taken out of the buffer so a caller that gives up does not leave its place taken. One the drain
                    // already took may still be applied, and the redelivery that follows is de-duplicated.
                    buffer.remove(held);
                    throw new PreDispatchRefusalException(this, HandoverMessages.interruptedBeforeApplied(noun), e);
                }
            }
        }
        return switch (held.answer) {
            case APPLIED -> true;
            case NOT_APPLIED -> false;
            case REFUSED -> throw new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), held.failure);
            case PENDING -> throw new AssertionError("The wait above ends only once the payload is answered.");
        };
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
     * @throws PreDispatchRefusalException if a prior {@link #catchUp(Source)} has failed, or if the {@code dedupId}
     *         function returns {@code null}. The failure is checked first, before
     *         the live check, so a payload fed after a permanently failed catch-up fails fast rather than reporting
     *         {@code false} forever for a caller to retry a catch-up that is never coming back.
     */
    public boolean acceptIfLive(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        K deliverKey = null;
        Source<T> replayedBy = null;
        boolean landed;
        synchronized (lock) {
            if (catchUpFailure != null) {
                throw new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), catchUpFailure);
            }
            if (!live) {
                // Refuse without buffering, unlike acceptReportingDelivery. Covers "never started", "still
                // replaying", and "stopped mid-replay" alike, all three are "not live", and a caller here has
                // already promised it can redeliver, so there is nothing to gain by holding the payload instead of
                // asking again later.
                landed = false;
            } else {
                K key = dedupKey(payload);
                if (deliveredIds.contains(key)) {
                    landed = true;
                } else if (replayedIds.contains(key)) {
                    replayedBy = source;
                    if (replayedBy != null) {
                        replayCallbacksRunning++;
                    }
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
        } else if (replayedBy != null) {
            reportAlreadyDeliveredByReplay(replayedBy, payload);
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
     * Whether this engine refuses every live payload from now on and will go on refusing. True once a
     * {@link #catchUp(Source)} attempt has thrown, and never false again after that, since the failure it records
     * is never cleared. False while replaying, while buffering, and once live.
     * <p>
     * Distinct from {@link #isReadyForLiveDelivery()}, which is also false during a replay that is going to
     * succeed. A caller deciding whether to stop for good needs to tell those two apart, and reading this after
     * the fact is safe precisely because it only ever goes from false to true.
     */
    public boolean refusesPermanently() {
        synchronized (lock) {
            return catchUpFailure != null;
        }
    }

    /**
     * Stop a handover that has not gone live and has no {@link #catchUp(Source)} running, the same way a stopped
     * replay leaves one. Every payload a caller is waiting on is answered as not applied, and later payloads are not
     * buffered, until the next {@link #catchUp(Source)}. Without this, a caller that fed a payload before any catch-up
     * started would wait for one that a shutting-down application never runs.
     * <p>
     * Does nothing to a live handover or to one with a catch-up running. A running replay is stopped through
     * {@link Source#keepReplaying()} instead, and it answers the waiting callers itself.
     */
    public void stopIfNotCatchingUp() {
        synchronized (lock) {
            if (!live && catchUpsInProgress == 0) {
                stopUnderLock();
            }
        }
    }

    /**
     * Run the one-time catch-up: replay the source's history (unless already caught up), then drain the buffered live
     * payloads and go live, then mark the catch-up complete.
     * <p>
     * A replay on a handover that is already live, a feed's {@code catchUp()} after its {@code goLive()}, waits for any
     * live delivery still running and then buffers live payloads until it ends, the same as before a first catch-up.
     * They are delivered when it ends, whether it completes or is stopped, since a view that buffers during a replay
     * throws that buffer away on a stop. A catch-up with nothing to replay that arrives while a replay runs does not
     * drain the buffer itself. The running replay drains it, and goes live even if it is stopped.
     * <p>
     * A replay also waits for a catch-up that is already replaying, until that catch-up returns or throws, so two
     * replays never fold into the view at once and each drain and marker belongs to the replay before it. Calling this
     * from inside a fold, a drain or a {@link Source} callback of a catch-up replaying on this handover deadlocks for
     * that reason.
     *
     * @return {@code true} when the catch-up finished and the handover is live, {@code false} when
     * {@link Source#keepReplaying()} stopped it partway, or when the calling thread was interrupted while waiting for
     * the deliveries already running to end, which leaves the handover as it was and the interrupt on the thread. A
     * failure throws rather than returning either.
     */
    public boolean catchUp(Source<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        synchronized (lock) {
            // A fresh catch-up revives a handover a previous one stopped, so stopping is recoverable by replaying
            // again rather than only by building a new one.
            stopped = false;
            catchUpsInProgress++;
        }
        // Tracks whether replayStarted() ran and replayCompleted() has not yet closed it out, so the catch block below
        // knows whether there is a replay lifecycle left open to abandon, rather than calling replayAbandoned() after
        // a clean replayCompleted() has already told the view its batch is durable.
        boolean replayOpen = false;
        // Whether this call took the replay turn, so only the call that took it gives it back.
        boolean holdsReplayTurn = false;
        try {
            if (source.isAlreadyCaughtUp()) {
                synchronized (lock) {
                    if (replayRunning) {
                        // The running replay delivers the buffer when it ends, and goes live even if it is stopped.
                        liveWhenReplayStops = true;
                        return true;
                    }
                    // Claimed under the same lock that read replayRunning, so a replay cannot start between the two
                    // and find this call draining into a view it is about to replay into.
                    liveTransitionsRunning++;
                }
                try {
                    drainBufferAndGoLive(source);
                } finally {
                    synchronized (lock) {
                        liveTransitionsRunning--;
                        lock.notifyAll();
                    }
                }
                return true;
            }
            boolean stoppedMidReplay = false;
            boolean interrupted = false;
            boolean drainAfterInterrupt = false;
            Throwable alreadyFailed = null;
            synchronized (lock) {
                // Read before the wait, so the check after it asks whether a catch-up failed while this call waited
                // rather than whether the handover had already failed when the caller asked for this one.
                Throwable failureBeforeWaiting = catchUpFailure;
                boolean wasLive = live;
                // Stops a further live delivery from starting, which is what the wait below waits out.
                live = false;
                if (!awaitLiveDeliveriesUnderLock()) {
                    // The wait was interrupted, so this call gives up rather than replaying next to a delivery it
                    // never waited out. Live is read again for the same reason it is below: a catch-up that went live
                    // during the wait did so for its own caller, and writing the value read before the wait would
                    // take that back.
                    interrupted = true;
                    if (wasLive || live) {
                        // Claimed under this lock, so the drain below runs the way a catch-up with nothing to replay
                        // drains, with a replay waiting rather than starting next to it.
                        liveTransitionsRunning++;
                        drainAfterInterrupt = true;
                    } else if (catchUpsInProgress == 1) {
                        // Only when no other catch-up is running. Stopping is this call's answer for its own caller,
                        // and while another one is going live or waiting for its turn the handover is that call's,
                        // with the payloads after this belonging in its buffer rather than dropped.
                        stopUnderLock();
                    }
                } else if (catchUpFailure != null && catchUpFailure != failureBeforeWaiting) {
                    // The catch-up this call waited for failed, which leaves the handover refusing everything and its
                    // caller told to replace it. So this replay does not start and fold a history into a view its
                    // caller was told to stop using. A caller that asks for a catch-up on a handover that had already
                    // failed still gets one, which is what it asked for.
                    alreadyFailed = catchUpFailure;
                } else {
                    // Written after the wait rather than before it, because wait() releases this lock, and a
                    // catch-up going live in that window sets live back to true, which would put the replay next to
                    // a live handover. Reading live again here is what makes the two one step.
                    liveWhenReplayStops = wasLive || live;
                    live = false;
                    // Every key belongs to the source a suppression reports to, so a new replay starts from none.
                    replayedIds.clear();
                    this.source = source;
                    replayRunning = true;
                    replayTurnHeld = true;
                    replayTurnThread = Thread.currentThread();
                    holdsReplayTurn = true;
                    // Cleared again here, not only when this call was entered, because the catch-up it waited for can
                    // have stopped in between. The payloads arriving during this replay belong in its buffer, and a
                    // handover left stopped would drop them.
                    stopped = false;
                }
            }
            if (alreadyFailed != null) {
                throw new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), alreadyFailed);
            }
            if (interrupted) {
                if (drainAfterInterrupt) {
                    try {
                        // Payloads taken into the buffer while live was false were reported handled or are waited
                        // on, so they are delivered here rather than being left behind a handover that is live again.
                        deliverBufferAndGoLive();
                    } finally {
                        synchronized (lock) {
                            liveTransitionsRunning--;
                            lock.notifyAll();
                        }
                    }
                }
                return false;
            }
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
                    K key = dedupKey(replayed);
                    deliver.accept(replayed);
                    synchronized (lock) {
                        replayedIds.add(key);
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
                boolean goLive;
                synchronized (lock) {
                    replayRunning = false;
                    goLive = liveWhenReplayStops;
                    if (!goLive) {
                        stopUnderLock();
                    }
                }
                if (goLive) {
                    // What buffered while the replay ran reaches the view now that it has thrown its replay batch away.
                    deliverBufferAndGoLive();
                }
                return false;
            }
            // Ordered before the live buffer drain and before markCaughtUp(), so anything a replay-aware view
            // buffered is durable before either runs (ADR 110).
            source.replayCompleted();
            replayOpen = false;
            drainBufferAndGoLive(source);
            source.markCaughtUp();
            return true;
        } catch (Throwable e) {
            // Record the failure so a live payload fed after a failed catch-up fails fast instead of buffering until
            // overflow and hiding the error.
            //
            // Every Throwable is recorded, not only a RuntimeException. Its callers no longer release the registration
            // when a catch-up fails (ADR 104), so a failure this engine does not record leaves a handover that keeps
            // buffering live payloads and returning normally, which acknowledges them into a replay that is never
            // coming back. That is the loss the refusal exists to prevent. A NoClassDefFoundError out of the fold is
            // one way it arrives, and a checked exception from a fold written in Kotlin, which declares nothing, is
            // another. An OutOfMemoryError is recorded too, since a replay it cut short is just as incomplete.
            if (replayOpen) {
                abandonReplayWithoutMasking(source);
            }
            boolean deliverBuffer;
            synchronized (lock) {
                // Only the call holding the replay turn owns the replay state. One that failed before taking it, its
                // marker lookup say, would otherwise drain or clear the state of a replay another call is running.
                deliverBuffer = holdsReplayTurn && replayRunning && liveWhenReplayStops;
                if (holdsReplayTurn && !deliverBuffer) {
                    // Cleared under the lock that read liveWhenReplayStops, so a catch-up with nothing to replay
                    // arriving now drains the buffer itself rather than leaving it to a replay that no longer will.
                    // With deliverBuffer true the drain below clears it under its own lock.
                    replayRunning = false;
                }
            }
            if (deliverBuffer) {
                // A payload taken into the buffer during a replay on a live handover was reported handled or is
                // waited on, so it is delivered before the failure makes this handover refuse everything after it.
                try {
                    deliverBufferAndGoLive();
                } catch (Throwable deliveryFailure) {
                    // Skip the instance itself. A view can throw one shared exception object from both the replay and
                    // the drain, and addSuppressed refuses to suppress an exception under itself. The
                    // IllegalArgumentException it throws would escape before the failure below is recorded, and the
                    // drain has already gone live, so later live events would be applied and acknowledged.
                    if (deliveryFailure != e) {
                        e.addSuppressed(deliveryFailure);
                    }
                }
            }
            synchronized (lock) {
                // The first failure is the one that matters, so a later call refusing because of it does not take its
                // place and hide the cause.
                if (catchUpFailure == null) {
                    catchUpFailure = e;
                }
                if (holdsReplayTurn) {
                    replayRunning = false;
                }
                // A failed handover refuses every payload, so one a caller is still waiting on is refused too.
                answerAwaitedInBuffer(Answer.REFUSED, catchUpFailure);
            }
            throw e;
        } finally {
            synchronized (lock) {
                if (holdsReplayTurn) {
                    replayTurnHeld = false;
                    replayTurnThread = null;
                }
                catchUpsInProgress--;
                lock.notifyAll();
            }
        }
    }

    // Guarded so that a source's own replayAbandoned() throwing cannot replace the failure (or stop) that made the
    // engine call it in the first place; the contract asks the source not to throw here, but this engine does not
    // trust that.
    private void abandonReplayWithoutMasking(Source<T> source) {
        // A view that buffers during a replay discards that buffer here, so a key the replay left behind would
        // suppress the only copy of an event the read model never got. Forgetting it costs a view that wrote through
        // a second delivery, which at-least-once delivery allows.
        synchronized (lock) {
            replayedIds.clear();
            this.source = null;
        }
        try {
            source.replayAbandoned();
        } catch (Throwable ignored) {
        }
    }

    // Assumes lock is held. Releases it while it waits for every live delivery, replay callback and replay already
    // running, so the replay about to start is the only thing writing to the view. None starts meanwhile, since live
    // is false. A catch-up that is replaying is waited for like the rest, through its drain and its marker: one that
    // finishes takes the handover live, and a second replay running past that point would have live payloads folded
    // next to it and thrown away if it stops.
    // Answers false when the wait was interrupted, so a caller shutting this down is not held by a fold that never
    // returns. The interrupt stays on the thread for whoever asked for it.
    private boolean awaitLiveDeliveriesUnderLock() {
        while (!inFlight.isEmpty() || replayCallbacksRunning > 0 || liveTransitionsRunning > 0 || replayTurnHeld) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    private void reportAlreadyDeliveredByReplay(Source<T> replayedBy, T payload) {
        try {
            replayedBy.alreadyDeliveredByReplay(payload);
        } finally {
            synchronized (lock) {
                replayCallbacksRunning--;
                lock.notifyAll();
            }
        }
    }

    private void drainBufferAndGoLive(Source<T> source) {
        // Every drain runs through here, including the one for a source that was already caught up and ran no replay
        // at all, which is why the signal sits here rather than beside replayCompleted().
        source.historyDone();
        deliverBufferAndGoLive();
    }

    private void deliverBufferAndGoLive() {
        List<Held<T>> toDeliver;
        List<K> keysToDeliver;
        List<Held<T>> alreadyReplayed;
        // Payloads whose key an earlier one in this drain, or a live delivery still running, has reserved. Answered by
        // whether that delivery applied the key.
        List<Held<T>> repeats;
        List<K> repeatKeys;
        Source<T> replayedBy;
        synchronized (lock) {
            replayedBy = this.source;
            toDeliver = new ArrayList<>(buffer.size());
            keysToDeliver = new ArrayList<>(buffer.size());
            alreadyReplayed = new ArrayList<>();
            repeats = new ArrayList<>();
            repeatKeys = new ArrayList<>();
            for (Held<T> buffered : buffer) {
                K key = dedupKey(buffered.payload);
                if (deliveredIds.contains(key)) {
                    buffered.answer(Answer.APPLIED, null);
                    continue;
                }
                if (replayedIds.contains(key)) {
                    // Applied by the replay already, so delivering it again would apply it twice. Collected instead,
                    // for the call the source is owed once this lock is released (ADR 137).
                    alreadyReplayed.add(buffered);
                    continue;
                }
                // Reserved here, before anything is delivered, but not recorded delivered here (#588): a delivery that
                // throws must not poison a later legitimate redelivery of the same payload, so deliveredIds is only
                // written once deliverOutsideLock knows the call actually succeeded.
                if (inFlight.add(key)) {
                    toDeliver.add(buffered);
                    keysToDeliver.add(key);
                } else {
                    repeats.add(buffered);
                    repeatKeys.add(key);
                }
            }
            // Counted before this handover goes live, so a catch-up that starts right after waits for these calls
            // rather than replaying while the previous replay's source is still being told about them.
            replayCallbacksRunning += alreadyReplayed.size();
            buffer.clear();
            replayRunning = false;
            live = true;
            lock.notifyAll();
        }
        try {
            deliverTaken(replayedBy, toDeliver, keysToDeliver, alreadyReplayed);
        } catch (Throwable e) {
            // The catch-up records this failure once it reaches catchUp(..), and refuses every payload from then on.
            // A payload this drain took but did not apply is answered the same way now, since nothing delivers it later.
            synchronized (lock) {
                answerAll(toDeliver, Answer.REFUSED, e);
                answerAll(alreadyReplayed, Answer.REFUSED, e);
                answerAll(repeats, Answer.REFUSED, e);
            }
            throw e;
        }
        synchronized (lock) {
            for (int i = 0; i < repeats.size(); i++) {
                repeats.get(i).answer(deliveredIds.contains(repeatKeys.get(i)) ? Answer.APPLIED : Answer.NOT_APPLIED, null);
            }
            lock.notifyAll();
        }
    }

    private void deliverTaken(Source<T> replayedBy, List<Held<T>> toDeliver, List<K> keysToDeliver, List<Held<T>> alreadyReplayed) {
        // Ahead of the drained deliveries, so a payload the replay already applied is reported before any payload
        // that comes after it.
        boolean reported = false;
        try {
            for (Held<T> replayedPayload : alreadyReplayed) {
                replayedBy.alreadyDeliveredByReplay(replayedPayload.payload);
                answer(replayedPayload, Answer.APPLIED);
            }
            reported = true;
        } finally {
            if (!reported) {
                // Nothing has been delivered yet, so every key reserved above is still reserved and would be skipped
                // by a later redelivery, and waited for by a later catch-up. Released for the same reason the
                // delivery loop below releases the rest of them, whatever the source threw.
                releaseReservations(keysToDeliver);
            }
            // Counted from under the lock that made this handover live, so a catch-up starting right now waits for
            // these calls rather than replaying while the previous replay's source is still being told about them.
            synchronized (lock) {
                replayCallbacksRunning -= alreadyReplayed.size();
                lock.notifyAll();
            }
        }
        // Outside the monitor, same as a live accept(Object) (#588). Still sequential on this thread, so catchUp's
        // markCaughtUp() call after this method returns is still ordered after every one of these deliveries, and a
        // delivery that throws here still reaches catchUp's own catch block exactly as it did before this method
        // stopped holding the lock for the delivery itself.
        int delivered = 0;
        try {
            for (; delivered < toDeliver.size(); delivered++) {
                Held<T> buffered = toDeliver.get(delivered);
                deliverOutsideLock(buffered.payload, keysToDeliver.get(delivered));
                // Answered as each one is applied rather than after the whole drain, so its caller stops waiting as soon
                // as its own payload is applied.
                answer(buffered, Answer.APPLIED);
            }
        } finally {
            // Every key was reserved above, before any of them was delivered. A delivery that throws leaves the
            // rest of them reserved and never delivered, and a reserved key is skipped by every later attempt, so
            // a redelivery of one of those payloads would be dropped without ever being handled. Releasing them
            // here is what keeps the failure recoverable: the catch-up records the failure, the caller sees it,
            // and the payloads are still eligible when the source offers them again.
            releaseReservations(keysToDeliver.subList(Math.min(delivered + 1, keysToDeliver.size()), keysToDeliver.size()));
        }
    }

    private void answer(Held<T> held, Answer answer) {
        synchronized (lock) {
            held.answer(answer, null);
            lock.notifyAll();
        }
    }

    // Assumes lock is held.
    private void answerAll(List<Held<T>> held, Answer answer, @Nullable Throwable failure) {
        for (Held<T> one : held) {
            one.answer(answer, failure);
        }
        lock.notifyAll();
    }

    // Assumes lock is held. Nothing drains a stopped handover's buffer until the next catch-up, so a caller waiting
    // there is answered now rather than held until then. A payload fed through acceptReportingDelivery(..) stays
    // for that catch-up, since nobody waits on it and the store has it anyway.
    private void stopUnderLock() {
        stopped = true;
        answerAwaitedInBuffer(Answer.NOT_APPLIED, null);
    }

    // Assumes lock is held.
    private void answerAwaitedInBuffer(Answer answer, @Nullable Throwable failure) {
        Iterator<Held<T>> held = buffer.iterator();
        while (held.hasNext()) {
            Held<T> one = held.next();
            if (one.awaited) {
                held.remove();
                one.answer(answer, failure);
            }
        }
        lock.notifyAll();
    }

    private void releaseReservations(List<K> keys) {
        if (keys.isEmpty()) {
            return;
        }
        synchronized (lock) {
            inFlight.removeAll(keys);
            lock.notifyAll();
        }
    }

    @SuppressWarnings("ConstantValue") // The function is declared non-null, but it is caller-supplied and unenforced.
    private K dedupKey(T payload) {
        K key = dedupId.apply(payload);
        if (key == null) {
            throw new PreDispatchRefusalException(this, HandoverMessages.dedupKeyRequired());
        }
        return key;
    }

    // Runs deliver outside the lock, then reports the outcome back under it: success moves the key from in-flight
    // to delivered, failure only clears the in-flight marker, so a payload whose delivery threw is not recorded and
    // a later redelivery is free to try again. Only live deliveries reach here, so only they write deliveredIds. The
    // replay writes replayedIds instead.
    private void deliverOutsideLock(T payload, K key) {
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
                lock.notifyAll();
            }
        }
    }

    private enum Answer {
        PENDING, APPLIED, NOT_APPLIED, REFUSED
    }

    // A live payload in the buffer, and what the caller waiting on it is told. The answer is written once, under the
    // lock, and only for a payload a caller waits on.
    private static final class Held<T> {
        private final T payload;
        private final boolean awaited;
        private Answer answer = Answer.PENDING;
        private @Nullable Throwable failure;

        private Held(T payload, boolean awaited) {
            this.payload = payload;
            this.awaited = awaited;
        }

        private void answer(Answer answer, @Nullable Throwable failure) {
            if (awaited && this.answer == Answer.PENDING) {
                this.answer = answer;
                this.failure = failure;
            }
        }
    }
}
