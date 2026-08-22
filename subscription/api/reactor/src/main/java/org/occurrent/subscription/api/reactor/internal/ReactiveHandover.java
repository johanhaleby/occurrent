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

package org.occurrent.subscription.api.reactor.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.HandoverMessages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The shared reactive catch-up-then-live coordination: the replay is folded to completion, then the catch-up-complete
 * marker is recorded, then the live feed is delivered. Live payloads arriving during the replay are buffered in a
 * bounded unicast sink, and the replay-to-live overlap is de-duplicated by an id extracted from the payload. Each phase
 * is serialized by its own {@code concatMap} and the phases run one after another, so the de-dup cache is only ever
 * touched by one thread at a time. That is ordering, not visibility: an asynchronous fold completes on whichever
 * thread ran it, so {@code BoundedIdCache} is synchronized and must stay that way. Extracted from (and mirrors exactly) the reactor projection
 * feed and the reactor push subscription model.
 * <p>
 * {@code T} is the payload type, one for both phases. The caller decides what a payload carries, so where a replayed
 * payload has metadata a live one may not, that difference lives in the payload rather than in this engine's signature.
 * The live-versus-replay distinction that this engine does care about is {@link Item#ack()}, decided per payload at
 * runtime, not per type.
 * <p>
 * <strong>This engine's ordering differs from the blocking one on purpose</strong>: here, the catch-up-complete
 * {@link Mono} returned by {@link #catchUp(Source)} completes, and the marker is persisted, <em>before</em> the
 * buffered live payloads are folded, because the returned {@code Mono} completes once the marker phase is done rather
 * than at the end of the live stream. It does <em>not</em> complete before the replayed payloads are folded: the marker
 * phase starts only after the replay phase has finished folding. The blocking engine's
 * {@code BlockingHandover.catchUp} returns only <em>after</em> the buffered live
 * payloads are drained. Both are internally consistent: a blocking {@code accept} call returns before its payload is
 * folded during the catch-up window, whereas here a live payload's {@code accept} {@link Mono} completes only once
 * its fold has actually run (including payloads buffered during the replay), even though the catch-up-done signal
 * itself already fired. Neither ordering is "fixed" by this extraction. Both are preserved as-is.
 * <p>
 * <strong>The replay runs on {@code boundedElastic}, not on the thread that called {@link #catchUp(Source)}.</strong>
 * This engine subscribes its own pipeline, so a caller that never touches the returned {@link Mono} still gets a
 * replay, and it gets one off its own thread. Join it through the returned {@code Mono} when the caller does want to
 * wait.
 */
@NullMarked
public final class ReactiveHandover<T> {

    /**
     * The replay side of a handover: whether the catch-up already ran, the position-ordered replay flux, and how to
     * record that the catch-up completed.
     */
    public interface Source<T> {
        /** Whether a prior catch-up already completed, so this one should skip straight to going live. */
        Mono<Boolean> isAlreadyCaughtUp();

        /** The history to replay, in position order, from the beginning. */
        Flux<T> replay();

        /** Record that the catch-up completed. */
        Mono<Void> markCaughtUp();

        /**
         * Whether the replay should keep going, asked once per payload before it is folded. Return {@code false} to
         * stop one already in flight, because the model was stopped or is shutting down.
         * <p>
         * A stop is not a failure. Nothing is drained, the handover does not go live, {@link #markCaughtUp()} is not
         * called, and no terminal error is recorded, so the next catch-up replays the whole history and the handover
         * stays usable. Live payloads arriving after a stop are dropped and their acks complete rather than hang, the
         * same dropped-not-deferred contract a stopped subscription model has (ADR 85).
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
         * The replay finished folding every event. The returned {@link Mono} is awaited before the catch-up marker is
         * recorded and before the buffered live payloads are drained, so anything a replay-aware view buffered is
         * durable first. {@code Mono<Void>} rather than a synchronous signal, so the write it triggers can be
         * asynchronous. The default completes immediately.
         */
        default Mono<Void> replayCompleted() {
            return Mono.empty();
        }

        /**
         * The replay was stopped before it finished, that is, {@link #keepReplaying()} returned {@code false}. Called
         * instead of {@link #replayCompleted()}, so anything buffered since {@link #replayStarted()} is discarded
         * rather than written, the same discard-on-stop contract {@link #keepReplaying()} documents. Must not throw.
         */
        default void replayAbandoned() {
        }

        /**
         * The history this catch-up was going to read has been read, and the live payloads buffered while it ran are
         * about to be delivered. Called immediately before every drain, including the one for a source that was
         * already caught up and replayed nothing, which is what makes it different from {@link #replayCompleted()}.
         * <p>
         * A buffered payload is delivered exactly once and never again, since whoever fed it here has already been
         * told it was handled, so a source that reports its own catch-up phase has to stop calling this part of the
         * work a replay before the drain rather than after it
         * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
         * decision 6). The default does nothing.
         */
        default void historyDone() {
        }

        /**
         * Every payload buffered while the history was being read has now been delivered, so what follows is a live
         * payload that arrived afterwards. Called once per catch-up, immediately after the last buffered payload, or
         * immediately after {@link #historyDone()} when none were buffered.
         * <p>
         * The pair with {@link #historyDone()} is what lets a source report the drain as its own part of the
         * catch-up rather than as live delivery, which matters because a buffered payload is delivered exactly once
         * and never again. The default does nothing.
         */
        default void liveDrained() {
        }
    }

    /**
     * Thrown by {@link #acceptReportingDelivery(Object)} and {@link #acceptIfLive(Object)} for a refusal decided
     * before any dispatch was attempted, a permanently failed catch-up, a full live buffer with nothing draining
     * it, or a {@code dedupId} function that returned {@code null} for the payload, none of them a delivery.
     * Distinct from any other {@link IllegalStateException} either method can error with, in particular one a
     * delivered payload's own handler errors with, so a caller that needs to tell those apart can catch this type
     * specifically instead of classifying every {@link IllegalStateException} alike. Mirrors
     * {@code BlockingHandover.PreDispatchRefusalException}.
     */
    public static final class PreDispatchRefusalException extends IllegalStateException {
        private final ReactiveHandover<?> owner;

        PreDispatchRefusalException(ReactiveHandover<?> owner, String message) {
            super(message);
            this.owner = owner;
        }

        PreDispatchRefusalException(ReactiveHandover<?> owner, String message, Throwable cause) {
            super(message, cause);
            this.owner = owner;
        }

        /**
         * Whether {@code handover} is the engine that raised this. A handler that reenters a second handover lets
         * that one's refusal escape unwrapped through the first, so a caller that means "my own engine refused"
         * has to compare identity rather than match the type.
         *
         * @param handover The engine to compare against.
         */
        public boolean thrownBy(ReactiveHandover<?> handover) {
            return owner == handover;
        }
    }

    private final Function<T, Mono<Void>> deliver;
    private final Function<T, String> dedupId;
    private final String noun;
    private final int maxBufferedEvents;
    private final BoundedIdCache deliveredIds;
    private final Sinks.Many<Item> liveSink;
    // The sink's own queue, held so the drain has a boundary. Everything in it when the history read finishes is what
    // was buffered while that read ran, and counting those down is the only way to know when the drain is over: the
    // live feed never completes, so nothing else marks the end of it.
    private final LinkedBlockingQueue<Item> liveBuffer;
    // How many buffered payloads are still to be delivered, -1 before the count is taken.
    private final java.util.concurrent.atomic.AtomicLong remainingInDrain = new java.util.concurrent.atomic.AtomicLong(-1);
    // The source of the catch-up currently going live, so the drain can tell it when the buffered set is exhausted.
    private final AtomicReference<Source<T>> drainedSource = new AtomicReference<>();
    // Acks of live payloads buffered but not yet folded, so a catch-up failure fails them rather than leaving the
    // caller's accept Monos hanging forever. The Boolean each carries is whether the payload was genuinely
    // delivered, not just whether the ack completed without error, see acceptReportingDelivery(..).
    private final Set<MonoSink<Boolean>> pendingLiveAcks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<@Nullable Throwable> terminalError = new AtomicReference<>();
    private static final Logger log = LoggerFactory.getLogger(ReactiveHandover.class);
    // Long enough that a producer holding the serialization claim finishes its own offer and releases it, short
    // enough that a caller's accept does not wait on it for long. Waiting happens on a scheduler, not on the
    // offering thread, so the window costs a timer rather than a thread.
    private static final java.time.Duration CONCURRENT_EMISSION_RETRY_WINDOW = java.time.Duration.ofMillis(100);
    // How long to wait before offering again. The claim is released by one queue write, so this only has to be
    // long enough not to retry into the same instant.
    private static final java.time.Duration CONCURRENT_EMISSION_RETRY_DELAY = java.time.Duration.ofMillis(1);
    // Offers waiting their turn at the sink, oldest first, so the order they were made is the order they reach it.
    private final java.util.Queue<PendingOffer> pendingOffers = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final AtomicBoolean offerDrainRunning = new AtomicBoolean();
    private volatile boolean stopped = false;
    // Set once, right before the buffered live payloads are drained on a successful catch-up, and never cleared
    // afterwards, mirroring BlockingHandover's live field. acceptIfLive(..) reads this to refuse a payload outright,
    // without ever touching liveSink, rather than buffering it the way acceptReportingDelivery(..) does.
    private volatile boolean live = false;

    private ReactiveHandover(Function<T, Mono<Void>> deliver, Function<T, String> dedupId, CatchupThenLiveOptions options, String noun) {
        this.deliver = deliver;
        this.dedupId = dedupId;
        this.noun = noun;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache(options.dedupCacheSize());
        // LinkedBlockingQueue(capacity), not ArrayBlockingQueue(capacity): both cap at maxBufferedEvents (up to 100k by
        // default) and reject past it the same way, but ArrayBlockingQueue pre-allocates its full backing array at
        // construction, roughly 800 KB held for the handover's whole lifetime whether or not the live feed ever
        // buffers anything. LinkedBlockingQueue allocates one node per buffered item, so memory tracks actual use.
        this.liveBuffer = new LinkedBlockingQueue<>(maxBufferedEvents);
        this.liveSink = Sinks.many().unicast().onBackpressureBuffer(liveBuffer);
    }

    /**
     * @param deliver Folds a payload, replayed during the catch-up or live once going live.
     * @param dedupId Extracts the replay-to-live de-dup key from a payload.
     * @param options De-dup cache size and live-buffer cap.
     * @param noun    The caller's noun for {@link HandoverMessages#catchUpFailed(String)}, e.g.
     *                {@code "projection feed"} or {@code "subscription"}, the same as the blocking engine takes.
     */
    public static <T> ReactiveHandover<T> create(
            Function<T, Mono<Void>> deliver, Function<T, String> dedupId, CatchupThenLiveOptions options, String noun) {
        Objects.requireNonNull(deliver, "deliver cannot be null");
        Objects.requireNonNull(dedupId, "dedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(noun, "noun cannot be null");
        return new ReactiveHandover<>(deliver, dedupId, options, noun);
    }

    /**
     * Feed a live payload. The returned {@link Mono} completes once the payload has been folded (or immediately if it
     * is a de-duplicated overlap). Payloads fed before or during the catch-up are buffered and delivered after the
     * replay.
     * <p>
     * A payload fed after a failed catch-up is refused rather than completed, and stays refused: the caller
     * acknowledges on completion, so completing would acknowledge a payload nothing handled. Recovery is the caller's
     * to choose, not this engine's (ADR 104).
     */
    public Mono<Void> accept(T payload) {
        return acceptReportingDelivery(payload).then();
    }

    /**
     * As {@link #accept(Object)}, additionally emitting whether the payload was genuinely handled (buffered for the
     * replay to drain, delivered live, or already delivered by an earlier attempt) rather than silently dropped
     * because a replay that would have drained it was stopped. A caller that acknowledges an externally sourced
     * payload (a broker message, say) needs this to tell the two apart, since {@link #accept(Object)} completes
     * normally either way.
     *
     * @return A {@link Mono} that completes with {@code false} only when this handover is stopped and the payload
     *         was dropped rather than buffered or delivered, and {@code true} otherwise, including a de-duplicated
     *         repeat of an already-delivered payload. Errors for the same reasons {@link #accept(Object)} does.
     */
    public Mono<Boolean> acceptReportingDelivery(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.create(ackSink -> bufferOrDeliverLive(payload, ackSink));
    }

    /**
     * As {@link #acceptReportingDelivery(Object)}, except a payload that would only buffer is refused instead:
     * completed {@code false} without ever reaching {@link #liveSink}. For a caller that can redeliver the same
     * payload later, a buffered payload is strictly worse than a refused one, since a buffered payload has already
     * been reported handled by the time this completes, while a refused one has not, and can safely be offered
     * again.
     * <p>
     * A payload fed while this handover is stopped is refused the same way, for the same reason. Nothing is
     * currently draining a buffer for it to wait in. Mirrors {@code BlockingHandover.acceptIfLive(Object)}'s
     * not-live and stopped refusals, but not its concurrent-delivery one: {@code BlockingHandover} delivers
     * outside its lock and can have two threads folding the same key at once, so it reports {@code false} for
     * whichever one loses that race. This engine has no such race to report, because {@link #liveSink} and its
     * single {@code concatMap} subscriber (see the class javadoc) serialize every live delivery onto one thread,
     * so a payload offered while an earlier delivery of the same key is still queued or folding simply waits its
     * turn behind it rather than racing it, and completes {@code true} once that earlier delivery lands.
     *
     * @return A {@link Mono} that completes with {@code true} once the payload has genuinely landed, delivered live
     *         just now, or already delivered by an earlier attempt, including one still queued or folding ahead of
     *         it on the sink. {@code false} only when this handover is not live yet or is stopped, so this call
     *         refused the payload outright rather than queuing it. Every {@code false} is safe to retry. Errors with
     *         {@link PreDispatchRefusalException} for the same reasons {@link #acceptReportingDelivery(Object)}
     *         does, checked first, before the live check, so a payload fed after a permanently failed catch-up
     *         fails fast rather than completing {@code false} forever for a caller to retry a catch-up that is
     *         never coming back.
     */
    public Mono<Boolean> acceptIfLive(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.create(ackSink -> {
            Throwable failure = terminalError.get();
            if (failure != null) {
                ackSink.error(catchUpFailed(failure));
                return;
            }
            if (!live) {
                // Refuse without buffering, unlike acceptReportingDelivery. Covers "never started", "still
                // replaying", and "stopped mid-replay" alike, all three are "not live", and a caller here has
                // already promised it can redeliver, so there is nothing to gain by holding the payload instead of
                // asking again later.
                ackSink.success(false);
                return;
            }
            bufferOrDeliverLive(payload, ackSink);
        });
    }

    // Shared by acceptReportingDelivery(..) and, once live, by acceptIfLive(..): registers the pending ack, re-checks
    // the terminal failure and stopped flag under the same race window acceptReportingDelivery has always had to
    // guard, then reserves the dedup key and hands the item to liveSink for the concatMap pipeline to drain.
    private void bufferOrDeliverLive(T payload, MonoSink<Boolean> ackSink) {
        ackSink.onDispose(() -> pendingLiveAcks.remove(ackSink));
        Throwable failure = terminalError.get();
        if (failure != null) {
            ackSink.error(catchUpFailed(failure));
            return;
        }
        if (stopped) {
            // Dropped rather than buffered, and the ack completes rather than failing. The replay that would have
            // drained this buffer was stopped, so nothing is coming to fold it. Dropped, not deferred (ADR 85).
            ackSink.success(false);
            return;
        }
        pendingLiveAcks.add(ackSink);
        // Re-check both after registering. A stop or a failure landing between the checks above and this add
        // would otherwise leave the ack unresolved, because the handler that resolves the pending acks has
        // already run, and the caller's Mono would never complete.
        failure = terminalError.get();
        if (failure != null) {
            ackSink.error(catchUpFailed(failure));
            return;
        }
        if (stopped) {
            ackSink.success(false);
            return;
        }
        String key;
        try {
            key = dedupKey(payload);
        } catch (RuntimeException keyFailure) {
            ackSink.error(keyFailure);
            return;
        }
        Item item = new Item(() -> deliver.apply(payload), key, ackSink);
        offerToLiveSink(item, ackSink);
    }

    // The unicast sink comes from the safe spec, so it rejects a second concurrent producer with
    // FAIL_NON_SERIALIZED instead of corrupting its queue. That rejection clears as soon as the producer holding
    // the claim finishes its own offer, so offering again is the whole fix.
    //
    // Every offer goes through one queue, in the order the offers were made, and one thread at a time takes that
    // queue to the sink. Two reasons for the queue rather than each offer retrying for itself. Retries that run
    // independently can reach the sink in a different order than the offers were made, which for one caller
    // offering two events in order means the second can be delivered first. And tryEmitNext delivers inline when
    // it wins, so a caller that waited for its own turn would be held for as long as somebody else's handler
    // takes to run, on a carrier or event-loop thread that has other work.
    //
    // One drain at a time also means this engine is the sink's only producer, so FAIL_NON_SERIALIZED cannot happen
    // any more. The handling below stays as defence, not as a path anything reaches today, which is why no test
    // drives it.
    private void offerToLiveSink(Item item, MonoSink<Boolean> ackSink) {
        pendingOffers.add(new PendingOffer(item, ackSink, System.nanoTime() + CONCURRENT_EMISSION_RETRY_WINDOW.toNanos()));
        drainPendingOffers();
    }

    private void drainPendingOffers() {
        // One drain at a time. A caller that finds one already running has left its own offer on the queue, and
        // that drain picks it up, so this returns rather than waiting.
        if (!offerDrainRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            while (true) {
                PendingOffer pending = pendingOffers.peek();
                if (pending == null) {
                    return;
                }
                Sinks.EmitResult result = liveSink.tryEmitNext(pending.item());
                if (!result.isFailure()) {
                    pendingOffers.poll();
                    continue;
                }
                switch (result) {
                    case FAIL_NON_SERIALIZED -> {
                        if (System.nanoTime() >= pending.deadline()) {
                            pendingOffers.poll();
                            pending.ack().error(new PreDispatchRefusalException(this, HandoverMessages.concurrentEmission()));
                            continue;
                        }
                        // Left at the head, so whatever runs next starts with it and the order holds.
                        Schedulers.parallel().schedule(this::drainPendingOffers,
                                CONCURRENT_EMISSION_RETRY_DELAY.toNanos(), TimeUnit.NANOSECONDS);
                        return;
                    }
                    // The pipeline is gone, so nothing is coming to deliver this payload. Dropped rather than
                    // refused, the same answer the stopped check above gives. Also defence rather than a reachable
                    // path, since that check runs first and catches every way the pipeline ends today.
                    case FAIL_TERMINATED, FAIL_CANCELLED -> {
                        pendingOffers.poll();
                        pending.ack().success(false);
                    }
                    default -> {
                        pendingOffers.poll();
                        pending.ack().error(new PreDispatchRefusalException(this, HandoverMessages.bufferOverflow(maxBufferedEvents, result)));
                    }
                }
            }
        } finally {
            offerDrainRunning.set(false);
        }
    }

    // An offer waiting its turn at the sink, with the point in time after which this engine stops offering it and
    // reports contention instead.
    private record PendingOffer(Item item, MonoSink<Boolean> ack, long deadline) {
    }

    /**
     * Whether this engine refuses every live payload from now on and will go on refusing. True once a
     * {@link #catchUp(Source)} attempt has errored, and never false again after that. False while replaying, while
     * buffering, and once live.
     * <p>
     * Distinct from a replay that is still running, which also cannot deliver but is going to succeed. A caller
     * deciding whether to stop for good needs to tell those two apart, and reading this after the fact is safe
     * precisely because it only ever goes from false to true.
     */
    public boolean refusesPermanently() {
        return terminalError.get() != null;
    }

    /**
     * Run the one-time catch-up: replay the source's history, record the completion marker, then start delivering the
     * live feed. The returned {@link Mono} completes when the replay and marker are done (see the class javadoc for
     * how that relates to the buffered live payloads), emitting {@code true} when the catch-up finished and
     * {@code false} when {@link Source#keepReplaying()} stopped it partway. A failure errors it instead.
     */
    public Mono<Boolean> catchUp(Source<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        // A fresh catch-up revives a handover a previous one stopped, so stopping is recoverable by replaying again
        // rather than only by building a new one.
        stopped = false;
        Sinks.One<Boolean> catchupDone = Sinks.one();

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = source.isAlreadyCaughtUp().cache();
        // Tracks whether replayStarted() ran and replayCompleted() has not yet closed it out, so the error handler
        // below knows whether there is a replay lifecycle left open to abandon, rather than calling replayAbandoned()
        // after a clean replayCompleted() has already told the view its batch is durable (ADR 110).
        AtomicBoolean replayOpen = new AtomicBoolean(false);
        // Three sequential phases, not stages of one Flux.concat. The marker must not be written until every replayed
        // payload has actually been folded, and a concat sibling cannot express that: concatMap's prefetch drains the
        // replay into its queue, so the replay Flux completes as soon as its items are emitted and concat moves on to
        // the next sibling while the folds are still running. That wrote the marker mid-replay for an asynchronous
        // fold, and since the marker makes a restart skip the replay, the unfolded events were lost with no error.
        Mono<Void> replayFolded = alreadyDone.flatMap(done -> {
            if (done) {
                return Mono.empty();
            }
            source.replayStarted();
            replayOpen.set(true);
            return source.replay().map(this::replayedItem)
                    // Checked inside the concatMap function rather than upstream of it. An upstream takeWhile would run
                    // at emission, and concatMap prefetches, so it could race far ahead of the folds. This is
                    // serialized per payload with the fold itself, which is the same reason the phases here are
                    // sequential.
                    .concatMap(item -> source.keepReplaying() ? deliver(item) : Mono.error(CatchupStopped.INSTANCE))
                    .then()
                    // Ordered before the marker and before the live buffer drain, so anything a replay-aware view
                    // buffered is durable before either runs.
                    .then(Mono.defer(source::replayCompleted))
                    .doOnSuccess(ignored -> replayOpen.set(false));
        });
        Mono<Void> recordMarker = alreadyDone.flatMap(done -> done ? Mono.<Void>empty() : source.markCaughtUp());

        replayFolded
                // Before the marker, before the catch-up signal and before the drain, on every path into it,
                // including the already-caught-up one that skipped the replay entirely. Ahead of the signal
                // specifically because a source's own subscriber to it runs inline and may forget the id, and this
                // running after that would leave state behind that nothing removes.
                .then(Mono.fromRunnable(() -> {
                    drainedSource.set(source);
                    source.historyDone();
                    // Taken after historyDone, so nothing that arrives from here on is counted as part of the drain.
                    remainingInDrain.compareAndSet(-1L, liveBuffer.size());
                }))
                .then(recordMarker)
                .doOnSuccess(ignored -> {
                    // Set before the live sink starts draining, the same point BlockingHandover.drainBufferAndGoLive
                    // flips its own live field. A payload acceptIfLive(..) sees after this point is treated as live
                    // even while whatever buffered ahead of it during the replay is still being delivered.
                    live = true;
                    // An empty buffer has nothing to deliver, so its drain is over the moment the handover is.
                    // Signalled here rather than beside historyDone, so a listener that frees the id on this cannot
                    // do it while the marker is still unwritten. A buffer with anything in it reaches liveDrained
                    // from the last delivery instead, which also runs after this point.
                    if (remainingInDrain.get() == 0L) {
                        source.liveDrained();
                    }
                    catchupDone.tryEmitValue(true);
                })
                .thenMany(liveSink.asFlux().concatMap(this::deliver))
                // This engine subscribes its own pipeline rather than handing it back, so without a scheduler the
                // replay would run on whoever called catchUp, which is the Spring refresh thread for an annotated
                // projection. boundedElastic because the replay folds through blocking bridges.
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(ignored -> {
                }, error -> {
                    if (error == CatchupStopped.INSTANCE) {
                        // Stopped, not failed. No marker, no drain, and no terminal error, so the handover stays
                        // usable. The buffered acks still have to be resolved or their callers hang forever; they
                        // complete rather than fail, because the payload was dropped rather than rejected.
                        stopped = true;
                        abandonReplayWithoutMasking(source, replayOpen);
                        catchupDone.tryEmitValue(false);
                        pendingLiveAcks.forEach(sink -> sink.success(false));
                        return;
                    }
                    // A catch-up-phase failure terminates the pipeline before the buffered live payloads are drained.
                    // Fail their acks and reject later ones, so the caller sees the error instead of hanging.
                    abandonReplayWithoutMasking(source, replayOpen);
                    terminalError.set(error);
                    // Logged only when the signal cannot carry the failure, which is the live phase, where
                    // catchupDone has already emitted and nothing else tells anyone. Logging unconditionally would
                    // repeat what the caller this signal reaches already logs for itself.
                    if (catchupDone.tryEmitError(error).isFailure()) {
                        log.error("The catch-up-then-live handover for this {} failed after it had already reported "
                                + "its catch-up done, so nothing was waiting to be told. It now refuses every live "
                                + "event. Fix the cause, then replace it, a subscription by cancelling it and "
                                + "subscribing again, a projection feed by building a new one.", noun, error);
                    }
                    // Wrapped like the later refusals in accept(..): the caller sees the same "this is terminal, and
                    // here is what to do" message whichever side of the failure its payload arrived on. The catch-up
                    // signal above still carries the raw cause, since that caller asked about the catch-up itself.
                    pendingLiveAcks.forEach(sink -> sink.error(catchUpFailed(error)));
                });

        return catchupDone.asMono();
    }

    // Guarded so that a source's own replayAbandoned() throwing cannot replace the failure (or stop) that made the
    // engine call it in the first place; the contract asks the source not to throw here, but this engine does not
    // trust that. compareAndSet so a clean replayCompleted() (which already cleared replayOpen) is never followed by
    // an abandon call for a lifecycle that already closed successfully.
    private void abandonReplayWithoutMasking(Source<T> source, AtomicBoolean replayOpen) {
        if (replayOpen.compareAndSet(true, false)) {
            try {
                source.replayAbandoned();
            } catch (RuntimeException | Error ignored) {
            }
        }
    }

    /**
     * Unwinds the replay pipeline on a deliberate stop. A singleton with no stack trace: it is a control signal handled
     * entirely inside this engine, never surfaced to a caller, and compared by identity so a fold that throws
     * something similar cannot be mistaken for it.
     */
    private static final class CatchupStopped extends RuntimeException {
        private static final CatchupStopped INSTANCE = new CatchupStopped();

        private CatchupStopped() {
            super("The catch-up was stopped before it finished", null, false, false);
        }
    }

    // Serialized by concatMap within a phase, and the phases run sequentially, so the de-dup cache is touched by one
    // thread at a time. Those calls still land on different threads, so the cache does its own synchronization.
    // Counts one delivered payload against the buffered set, and tells the source once that set is exhausted. Only
    // ever counts down from a taken count, so a delivery before the history read finished, or after the drain is
    // over, changes nothing.
    private void countTowardsDrain() {
        long remaining = remainingInDrain.get();
        if (remaining <= 0L) {
            return;
        }
        if (remainingInDrain.decrementAndGet() == 0L) {
            drainedSource.get().liveDrained();
        }
    }

    // Counted after the payload has been delivered rather than before it, so the last buffered one is still part of
    // the drain while it is being handled.
    private Mono<Void> deliver(Item item) {
        return deliverItem(item).doFinally(signal -> countTowardsDrain());
    }

    private Mono<Void> deliverItem(Item item) {
        MonoSink<Boolean> ack = item.ack();
        if (ack != null) {
            if (deliveredIds.contains(item.dedupKey())) {
                ack.success(true);
                return Mono.empty();
            }
            // Mono.defer so a synchronous throw from the fold becomes an onError signal onErrorResume can catch, rather
            // than aborting the whole pipeline.
            return Mono.defer(item.deliver())
                    .doOnSuccess(v -> {
                        deliveredIds.add(item.dedupKey());
                        ack.success(true);
                    })
                    .onErrorResume(error -> {
                        ack.error(error);
                        return Mono.empty();
                    });
        }
        // Replay payload: an error here propagates and fails the catch-up.
        return Mono.defer(item.deliver()).doOnSuccess(v -> deliveredIds.add(item.dedupKey()));
    }

    private Item replayedItem(T replayed) {
        return new Item(() -> deliver.apply(replayed), dedupKey(replayed), null);
    }

    // The blocking engine wraps its terminal failure in this message and this one used to propagate the raw cause, so
    // the same refusal read as a transient handler error on one stack and as a terminal one on the other. The recovery
    // differs (retry versus release and set up again), which is exactly what the message says, so both stacks say it.
    private PreDispatchRefusalException catchUpFailed(Throwable cause) {
        return new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), cause);
    }

    @SuppressWarnings("ConstantValue") // The function is declared non-null, but it is caller-supplied and unenforced.
    private String dedupKey(T payload) {
        String key = dedupId.apply(payload);
        if (key == null) {
            throw new PreDispatchRefusalException(this, HandoverMessages.dedupKeyRequired());
        }
        return key;
    }

    // A replayed payload has a null ack; a live payload carries the MonoSink whose completion (with whether it was
    // genuinely delivered) lets the caller acknowledge. The deliver supplier is bound at creation time, so Item
    // needs no type parameter of its own.
    private record Item(Supplier<Mono<Void>> deliver, String dedupKey, @Nullable MonoSink<Boolean> ack) {
    }
}
