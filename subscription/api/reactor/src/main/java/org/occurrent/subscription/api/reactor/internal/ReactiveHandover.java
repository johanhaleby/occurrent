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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.retry.Retry;

import java.util.ArrayList;
import java.util.List;
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
 * De-dup is two caches, not one. The replay records what it delivered in one, a live delivery records what it
 * delivered in the other, and every check reads both, so a payload is delivered once either way. What the two
 * caches decide is what happens to the copy that is not delivered. One the replay already delivered reaches
 * {@link Source#alreadyDeliveredByReplay(Object)}, because the replay ran inside the source's history phase and a
 * source that writes something down per delivery has written nothing down for it yet. One an earlier live delivery
 * already handled reaches nothing, because that delivery did all of it
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
 * <p>
 * <strong>This engine's ordering differs from the blocking one on purpose</strong>: here, the catch-up-complete
 * {@link Mono} returned by {@link #catchUp(Source)} completes, and the marker is persisted, <em>before</em> the
 * buffered live payloads are folded, because the returned {@code Mono} completes once the marker phase is done rather
 * than at the end of the live stream. It does <em>not</em> complete before the replayed payloads are folded: the marker
 * phase starts only after the replay phase has finished folding. Called from this engine's own code, it emits
 * {@code true} before the replay has run, see {@link #catchUp(Source)}. The blocking engine's
 * {@code BlockingHandover.catchUp} returns only <em>after</em> the buffered live
 * payloads are drained. Both are internally consistent. On either engine a live payload's {@code accept} returns, or
 * its {@link Mono} completes, only once its fold has actually run, including a payload buffered during the replay,
 * and here that can be after the catch-up-done signal has already fired. Here, a payload fed from this engine's own
 * code is the exception, see {@link #acceptReportingDelivery(Object)}. Neither ordering is "fixed" by this
 * extraction. Both are preserved as-is.
 * <p>
 * <strong>The replay runs on {@code boundedElastic}, not on the thread that called {@link #catchUp(Source)}.</strong>
 * This engine subscribes its own pipeline, so a caller that never touches the returned {@link Mono} still gets a
 * replay, and it gets one off its own thread. Join it through the returned {@code Mono} when the caller does want to
 * wait.
 */
@NullMarked
public final class ReactiveHandover<T, K> {

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
         * A stop is not a failure. {@link #markCaughtUp()} is not called and no terminal error is recorded, so the
         * next catch-up replays the whole history and the handover stays usable.
         * <p>
         * What the stop does with the live payloads depends on where the handover stood when the replay started. One
         * that had not gone live drains nothing and does not go live. The payloads it held and those arriving after
         * the stop are answered as not applied rather than left hanging, so {@link ReactiveHandover#accept(Object)}
         * errors for them and {@link ReactiveHandover#acceptReportingDelivery(Object)} completes {@code false}. One
         * that was already live delivers what it held back and goes on delivering, see
         * {@link ReactiveHandover#catchUp}.
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

        /**
         * A live payload arrived whose de-dup key the replay already delivered, so this engine did not deliver it a
         * second time. Called once per such payload, from the same {@code concatMap} every live payload runs through,
         * so it is serialized against the deliveries around it exactly as a delivery is.
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
         * An error signal from the returned {@link Mono} reaches the payload's own acknowledgement, the same way an
         * error from the fold does, so the source offers the payload again rather than losing what it owed. Called
         * again for every further copy of the same payload. The default emits nothing.
         *
         * @param payload The live payload that was not delivered.
         */
        default Mono<Void> alreadyDeliveredByReplay(T payload) {
            return Mono.empty();
        }

        /**
         * Undo {@link #markCaughtUp()}, so the next catch-up replays the history rather than skipping it. Called when
         * this handover starts failing, on the source whose catch-up delivers live and on the source whose catch-up
         * failed, see {@link ReactiveHandover#acceptReportingDelivery(Object)}, and on a source that wrote its marker
         * while this handover started failing. A payload whose caller was told it was taken in can have failed, and a
         * replay is the only thing left that can deliver it again. Awaited before this handover refuses anything for
         * that failure. An error signal from the returned {@link Mono} is retried 3 times and then logged, and the
         * handover goes on failing with the marker still in place. The default emits nothing.
         */
        default Mono<Void> forgetCaughtUp() {
            return Mono.empty();
        }
    }

    /**
     * Thrown by {@link #acceptReportingDelivery(Object)} and {@link #acceptIfLive(Object)} for a refusal decided
     * before any dispatch was attempted, a permanently failed catch-up, a handover that is failing, a full live
     * buffer with nothing draining it, or a {@code dedupId} function that returned {@code null} for the payload, none
     * of them a delivery. Also
     * what {@link #accept(Object)} errors with for every payload {@link #acceptReportingDelivery(Object)} would
     * complete {@code false} for, so the caller offers it again.
     * Distinct from any other {@link IllegalStateException} either method can error with, in particular one a
     * delivered payload's own handler errors with, so a caller that needs to tell those apart can catch this type
     * specifically instead of classifying every {@link IllegalStateException} alike. Mirrors
     * {@code BlockingHandover.PreDispatchRefusalException}.
     */
    public static final class PreDispatchRefusalException extends IllegalStateException {
        private final ReactiveHandover<?, ?> owner;

        PreDispatchRefusalException(ReactiveHandover<?, ?> owner, String message) {
            super(message);
            this.owner = owner;
        }

        PreDispatchRefusalException(ReactiveHandover<?, ?> owner, String message, Throwable cause) {
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
        public boolean thrownBy(ReactiveHandover<?, ?> handover) {
            return owner == handover;
        }
    }

    private final Function<T, Mono<Void>> deliver;
    private final Function<T, K> dedupId;
    private final String noun;
    private final int maxBufferedEvents;
    // Two caches rather than one, because the two suppressions they cause are not the same event. A key in
    // deliveredIds was delivered live, so suppressing its repeat is a plain no-op. A key in replayedIds was delivered
    // by the replay, inside the history phase, so suppressing the live copy owes the source a call to
    // Source.alreadyDeliveredByReplay(..) (ADR 137). One cache cannot tell those apart, and the replay's own volume
    // evicting the live keys is what made the live-redelivery de-dup empty exactly when the handover went live.
    private final BoundedIdCache<K> deliveredIds;
    private final BoundedIdCache<K> replayedIds;
    private final Sinks.Many<Item<K>> liveSink;
    // The sink's own queue, held so a stop can take out the payloads it answered. Left in, they would take up places
    // in it until a later catch-up went live.
    private final LinkedBlockingQueue<Item<K>> liveBuffer;
    // One per catch-up that reached its drain, holding the source to tell once its own buffered set is exhausted, the
    // last turn that belongs to it, and how many of those payloads are left. One per catch-up rather than one set of
    // fields, so a later catch-up neither takes over the drain of the one before it nor ends it early.
    private record Drain<S>(Source<S> source, long boundaryTurn, java.util.concurrent.atomic.AtomicLong remaining,
                            AtomicBoolean holdsReplayTurn) {
    }

    private final java.util.Queue<Drain<T>> drains = new java.util.concurrent.ConcurrentLinkedQueue<>();
    // The source whose replay filled replayedIds, so a live payload that replay already delivered can reach it. Set
    // when a replay starts rather than by every catchUp(Source), so a catch-up that replays nothing leaves it in
    // place, and cleared with replayedIds when a replay is abandoned.
    private final AtomicReference<@Nullable Source<T>> replaySource = new AtomicReference<>();
    // The live sink accepts one subscriber ever. A catch-up on a handover that is already live, a feed's goLive()
    // after its catchUp(), leaves the running pipeline alone instead of subscribing again, which the sink would
    // refuse and the error handler would record as a failed catch-up.
    private final AtomicBoolean liveSinkSubscribed = new AtomicBoolean();
    // Guards the three fields below. Live payloads wait at livePaused while a replay runs, even on a handover that is
    // already live, because a view that buffers during a replay throws that buffer away if the replay is stopped.
    private final Object liveGate = new Object();
    private Sinks.@Nullable Empty<Void> livePaused = null;
    // Whether the live pipeline is delivering a payload right now, so a replay can wait for it before it starts.
    private boolean liveDelivering = false;
    private Sinks.@Nullable Empty<Void> liveIdle = null;
    // Guards the field below. A replay takes its turn here and holds it until its drain ends, or until its catch-up
    // is stopped or fails, so two replays never fold into the view at once and the payloads a drain delivers are
    // checked against the keys of the replay before it. The first to finish takes the handover live, and a second replay running past that
    // point would have live payloads folded next to it and thrown away if it stops.
    private final Object replayTurn = new Object();
    // Completes when the replay holding the turn ends, so the next one in line takes it. Null while none is running.
    private Sinks.@Nullable Empty<Void> replayTurnReleased = null;
    // Acks of live payloads buffered but not yet folded, so a catch-up failure fails them rather than leaving the
    // caller's accept Monos hanging forever. The Boolean each carries is whether the payload was genuinely
    // delivered, not just whether the ack completed without error, see acceptReportingDelivery(..).
    private final Set<LiveAck> pendingLiveAcks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<@Nullable Throwable> terminalError = new AtomicReference<>();
    // Replaced on every failed catch-up, not only the first, so a call waiting for a replay can tell a catch-up failed
    // while it waited on a handover that had already failed before. A catch-up refusing because another one failed does
    // not replace it, so a later waiter gets that failure rather than the refusal wrapping it.
    private final AtomicReference<@Nullable RecordedFailure> latestFailure = new AtomicReference<>();
    // Taken by the first failure, a failed catch-up or a failed payload answered once it was queued, so only that one
    // forgets the marker and decides the cause. Set before the marker is forgotten, so a catch-up can tell not to
    // write it back.
    private final AtomicReference<@Nullable Failing> failureStarted = new AtomicReference<>();
    // Set once the first failure has forgotten the marker, and kept until this handover fails for good, which it does
    // once nothing taken in is left to deliver.
    private final AtomicReference<@Nullable Failing> failing = new AtomicReference<>();
    // Errored once this handover fails for good, which ends the pipeline delivering live payloads.
    private final Sinks.Empty<Void> failedForGood = Sinks.empty();
    // The source of the catch-up whose pipeline delivers live, one of those told to forget its marker when this
    // handover starts failing.
    private final AtomicReference<@Nullable Source<T>> liveSource = new AtomicReference<>();
    // Set while the current thread runs a fold or a Source callback of this handover. A catch-up or a live payload fed
    // there never waits, since the replay, the pause or the delivery it would wait for is waiting for that code.
    private final ThreadLocal<Boolean> runningOwnCode = new ThreadLocal<>();
    // Written into the context of this handover's pipeline, so a catch-up composed into a fold or a Source callback
    // finds it on whichever thread that runs.
    private final Object insideOwnPipeline = new Object();
    // Replays numbered in the order they start, and the number of the latest one that went live, so a catch-up own
    // code asked for before a replay started that has since gone live does not replay the same history again.
    private final java.util.concurrent.atomic.AtomicLong replaysStarted = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong latestReplayGoneLive = new java.util.concurrent.atomic.AtomicLong();
    private static final Logger log = LoggerFactory.getLogger(ReactiveHandover.class);
    // Long enough that a producer holding the serialization claim finishes its own offer and releases it, short
    // enough that a caller's accept does not wait on it for long. Waiting happens on a scheduler, not on the
    // offering thread, so the window costs a timer rather than a thread.
    // Attempts to forget a catch-up marker after the first, and the delay before the first of them, doubled for each.
    private static final int FORGET_RETRIES = 3;
    private static final java.time.Duration FORGET_FIRST_RETRY_DELAY = java.time.Duration.ofMillis(100);
    // How long one attempt to forget a catch-up marker may take before it counts as failed and is retried, so a store
    // that never answers cannot hold a failure back forever.
    private static final java.time.Duration FORGET_ATTEMPT_TIMEOUT = java.time.Duration.ofSeconds(5);
    private static final java.time.Duration CONCURRENT_EMISSION_RETRY_WINDOW = java.time.Duration.ofMillis(100);
    // How long to wait before offering again. The claim is released by one queue write, so this only has to be
    // long enough not to retry into the same instant.
    private static final java.time.Duration CONCURRENT_EMISSION_RETRY_DELAY = java.time.Duration.ofMillis(1);
    // Offers waiting their turn at the sink, oldest first, so the order they were made is the order they reach it.
    private final java.util.Queue<PendingOffer<K>> pendingOffers = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final AtomicBoolean offerDrainRunning = new AtomicBoolean();
    // Live payloads taken in and not yet delivered, wherever they are sitting. An offer waits in pendingOffers
    // until the drain hands it to the sink, and then in the sink's own queue until its handler runs, so counting
    // only one of the two would leave the other unbounded. maxBufferedEvents caps this, not either queue.
    private final java.util.concurrent.atomic.AtomicInteger liveBacklog = new java.util.concurrent.atomic.AtomicInteger();
    // Guards taking a place, stamping a payload with its turn, and queueing it, so the three cannot interleave.
    private final Object admission = new Object();
    // How many live payloads have been taken in, ever. A payload's own number is its turn, and the drain boundary
    // is the last turn taken in while the history was still being read.
    private final java.util.concurrent.atomic.AtomicLong admitted = new java.util.concurrent.atomic.AtomicLong();
    // The last turn that belongs to the drain. A payload taken in after this one is live delivery, not part of
    // what was buffered while the history was read, however early it happens to be delivered.
    //
    // This and the admission guard are two guards on one property, on purpose. The guard makes the queue order and
    // the turn order agree, which is what keeps deliveries in step with the count. The turn check makes the count
    // right whatever the order turns out to be. Either alone holds the property today, so neither can be
    // falsified by a test while the other is in place, and the pair is what keeps a later change to one of them
    // from quietly ending the drain early.
    private volatile boolean stopped = false;
    // Held to change the field below, and to write the stopped flag where a catch-up starts and where
    // stopIfNotCatchingUp() sets it, so that stop either comes before the catch-up starts or finds it running and lets
    // it answer the payloads. Every other write of stopped comes from a catch-up while it is counted here, so none of
    // them can interleave with that stop.
    private final Object catchUpsGuard = new Object();
    // Catch-ups from their catchUp(Source) call until they go live, are stopped or fail.
    private int catchUpsInProgress = 0;
    // Set once, right before the buffered live payloads are drained on a successful catch-up, and never cleared
    // afterwards, mirroring BlockingHandover's live field. acceptIfLive(..) reads this to refuse a payload outright,
    // without ever touching liveSink, rather than buffering it the way acceptReportingDelivery(..) does.
    private volatile boolean live = false;
    private volatile java.time.Duration forgetAttemptTimeout = FORGET_ATTEMPT_TIMEOUT;

    private ReactiveHandover(Function<T, Mono<Void>> deliver, Function<T, K> dedupId, CatchupThenLiveOptions options, String noun) {
        this.deliver = deliver;
        this.dedupId = dedupId;
        this.noun = noun;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache<>(options.dedupCacheSize());
        this.replayedIds = new BoundedIdCache<>(options.dedupCacheSize());
        // LinkedBlockingQueue(capacity), not ArrayBlockingQueue(capacity): both cap at maxBufferedEvents (up to 100k by
        // default) and reject past it the same way, but ArrayBlockingQueue pre-allocates its full backing array at
        // construction, roughly 800 KB held for the handover's whole lifetime whether or not the live feed ever
        // buffers anything. LinkedBlockingQueue allocates one node per buffered item, so memory tracks actual use.
        this.liveBuffer = new LinkedBlockingQueue<>(maxBufferedEvents);
        this.liveSink = Sinks.many().unicast().onBackpressureBuffer(liveBuffer);
    }

    /**
     * @param deliver Folds a payload, replayed during the catch-up or live once going live.
     * @param dedupId Extracts the replay-to-live de-dup key from a payload. Two payloads count as one only when their
     *                keys are equal, so the key has to hold everything that identifies a payload.
     * @param options De-dup cache size and live-buffer cap.
     * @param noun    The caller's noun for {@link HandoverMessages#catchUpFailed(String)}, e.g.
     *                {@code "projection feed"} or {@code "subscription"}, the same as the blocking engine takes.
     */
    public static <T, K> ReactiveHandover<T, K> create(
            Function<T, Mono<Void>> deliver, Function<T, K> dedupId, CatchupThenLiveOptions options, String noun) {
        Objects.requireNonNull(deliver, "deliver cannot be null");
        Objects.requireNonNull(dedupId, "dedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(noun, "noun cannot be null");
        return new ReactiveHandover<>(deliver, dedupId, options, noun);
    }

    /**
     * Feed a live payload. The returned {@link Mono} completes once the payload has been folded (or immediately if it
     * is a de-duplicated overlap). Payloads fed before or during the catch-up are buffered and delivered after the
     * replay, and the {@link Mono} completes only then.
     * <p>
     * Errors rather than completing whenever the payload was not folded, since the caller acknowledges on completion
     * and completing would acknowledge a payload nothing handled. That covers a replay stopped before the handover
     * went live, which errors every payload it held, a {@link #stopIfNotCatchingUp()} that stopped the handover, which
     * errors every payload waiting for a catch-up, a payload fed while this handover is stopped, and a failed
     * catch-up, which refuses every payload from then on. Recovery is the caller's to choose, not this engine's
     * (ADR 104), and for a broker listener it means not acknowledging, so the broker delivers the payload again.
     * <p>
     * Called from this handover's own code, it completes once the payload is queued, as
     * {@link #acceptReportingDelivery(Object)} describes.
     */
    public Mono<Void> accept(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.deferContextual(context -> offer(payload, ownCode(context)))
                .flatMap(outcome -> switch (outcome) {
                    case APPLIED -> Mono.<Void>empty();
                    case STOPPED -> Mono.error(new PreDispatchRefusalException(this, HandoverMessages.stoppedBeforeApplied(noun)));
                    case NOT_LIVE -> Mono.error(new AssertionError("Only acceptIfLive(..) answers a payload as not live."));
                });
    }

    /**
     * As {@link #accept(Object)}, except a payload that was not folded because this handover is stopped, or because
     * a replay that would have drained it was stopped, completes {@code false} rather than erroring. Waits for the
     * drain the same way {@link #accept(Object)} does.
     * <p>
     * Called from this handover's own code, the code {@link #catchUp(Source)} recognizes as its own, it completes
     * {@code true} once the payload is queued instead, since that code has to return before the payload can be
     * delivered. The payload is delivered after the delivery or callback it came from, in the order it was queued,
     * like a payload fed from anywhere else. A stop does not drop it, and the next catch-up that goes live delivers
     * it.
     * <p>
     * Nobody waits for such a payload once it is queued, so when its delivery fails this handover starts failing
     * rather than go on without it. A failed {@link #catchUp(Source)} starts it failing the same way, whether the
     * replay, the marker read or the marker write failed. It tells the source that delivers live and the source whose
     * catch-up failed to {@link Source#forgetCaughtUp() forget the marker}, then refuses every payload that does not
     * come from its own code with that failure, delivers every payload it has taken in and every payload its own code
     * feeds it meanwhile, and once none is left fails for good with that failure. A failed catch-up also answers each
     * payload from other code still waiting with its failure, and does not deliver those. Each payload answered
     * {@code true} gets its delivery attempt. Once the marker is gone, the next catch-up replays the history. An
     * attempt to forget that takes longer than 5 seconds counts as failed. A forget that still fails after 3 retries
     * is logged, and the marker then has to be deleted before the next catch-up. A payload that no replay can bring
     * back is lost only when its own delivery failed.
     *
     * @return A {@link Mono} that completes with {@code true} once the payload has been folded, live or by the drain,
     *         including a de-duplicated repeat of an already-delivered payload, or once it is queued when called from
     *         this handover's own code, and with {@code false} when this handover is stopped or the replay was stopped
     *         before going live, so the payload was never folded. Every {@code false} is safe to offer again. Errors
     *         for the other reasons {@link #accept(Object)} does.
     */
    public Mono<Boolean> acceptReportingDelivery(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.deferContextual(context -> offer(payload, ownCode(context)))
                .map(outcome -> outcome == Outcome.APPLIED);
    }

    private Mono<Outcome> offer(T payload, boolean answerOnceQueued) {
        return Mono.create(ackSink -> bufferOrDeliverLive(payload, ackSink, answerOnceQueued));
    }

    // Whether a call comes from a fold or a Source callback of this handover, asked when the call's Mono is
    // subscribed. The thread tells for code that subscribes it where this handover called that code, blocking or not,
    // and the subscriber's context for code that returns the Mono as part of its own.
    private boolean ownCode(ContextView context) {
        return runningOwnCode.get() != null || context.hasKey(insideOwnPipeline);
    }

    // Why a payload is refused before it is taken in, or null when it is not. A handover that is failing still takes
    // in what its own code feeds it, and delivers it before it fails for good.
    private @Nullable Throwable refusalCause(boolean ownCode) {
        Throwable failure = terminalError.get();
        return failure != null || ownCode ? failure : failingCause();
    }

    private @Nullable Throwable failingCause() {
        Failing current = failing.get();
        return current == null ? null : current.cause();
    }

    // The failure a catch-up compares against, from the moment this handover starts failing.
    private @Nullable Throwable failure() {
        Throwable failed = terminalError.get();
        if (failed != null) {
            return failed;
        }
        Failing started = failureStarted.get();
        return started == null ? null : started.cause();
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
     *         never coming back. Called from this handover's own code, it decides live or not the same way, and a
     *         payload it takes in completes {@code true} once it is queued, as
     *         {@link #acceptReportingDelivery(Object)} describes.
     */
    public Mono<Boolean> acceptIfLive(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.<Outcome>create(ackSink -> {
            boolean ownCode = ownCode(ackSink.contextView());
            Throwable failure = refusalCause(ownCode);
            if (failure != null) {
                ackSink.error(refusal(failure));
                return;
            }
            if (!live || liveDeliveryPaused()) {
                // Refuse without buffering, unlike acceptReportingDelivery. Covers "never started", "still
                // replaying", and "stopped mid-replay" alike, all three are "not live", and a caller here has
                // already promised it can redeliver, so there is nothing to gain by holding the payload instead of
                // asking again later. A replay on a handover that is already live pauses live delivery, and counts
                // as still replaying here, the same as on the blocking engine.
                ackSink.success(Outcome.NOT_LIVE);
                return;
            }
            bufferOrDeliverLive(payload, ackSink, ownCode);
        }).map(outcome -> outcome == Outcome.APPLIED);
    }

    // Shared by acceptReportingDelivery(..) and, once live, by acceptIfLive(..): registers the pending ack, re-checks
    // the terminal failure and stopped flag under the same race window acceptReportingDelivery has always had to
    // guard, then reserves the dedup key and hands the item to liveSink for the concatMap pipeline to drain.
    private void bufferOrDeliverLive(T payload, MonoSink<Outcome> ackSink, boolean answerOnceQueued) {
        LiveAck ack = new LiveAck(ackSink, answerOnceQueued);
        ackSink.onDispose(() -> pendingLiveAcks.remove(ack));
        Throwable failure = refusalCause(answerOnceQueued);
        if (failure != null) {
            ackSink.error(refusal(failure));
            return;
        }
        if (stopped && !live) {
            // Answered as stopped rather than buffered. The replay that would have drained this buffer was stopped,
            // so nothing is coming to fold it, and accept(..) errors so its caller offers the payload again. A
            // handover that has gone live delivers instead, whatever a stop left behind, since its live pipeline
            // runs.
            ackSink.success(Outcome.STOPPED);
            return;
        }
        pendingLiveAcks.add(ack);
        // Re-check both after registering. A stop or a failure landing between the checks above and this add
        // would otherwise leave the ack unresolved, because the handler that resolves the pending acks has
        // already run, and the caller's Mono would never complete.
        failure = refusalCause(answerOnceQueued);
        if (failure != null) {
            ackSink.error(refusal(failure));
            return;
        }
        if (stopped && !live) {
            answer(ack, Outcome.STOPPED);
            return;
        }
        K key;
        try {
            key = dedupKey(payload);
        } catch (RuntimeException keyFailure) {
            ackSink.error(keyFailure);
            return;
        }
        Item<K> item = new Item<>(() -> deliver.apply(payload), () -> alreadyDeliveredByReplay(payload), key, ack);
        if (offerToLiveSink(item, ack) && answerOnceQueued) {
            // Answered here rather than by its delivery, which cannot start before the code that fed it returns.
            answer(ack, Outcome.APPLIED);
        }
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
    // Returns whether the payload was queued, false when it was answered here instead.
    private boolean offerToLiveSink(Item<K> item, LiveAck ack) {
        // Taking a place, stamping the payload with its turn and queueing it are one step. Apart, a payload could
        // take a place and be queued behind one that took its place later, and the drain boundary below counts by
        // turn, so the two have to agree.
        synchronized (admission) {
            if (ack.droppedByStop()) {
                // A stop answered it between its registration and here, so its caller offers it again.
                return false;
            }
            if (liveBacklog.get() >= maxBufferedEvents) {
                ack.sink().error(new PreDispatchRefusalException(this, HandoverMessages.bufferOverflow(maxBufferedEvents)));
                return false;
            }
            liveBacklog.incrementAndGet();
            Item<K> stamped = item.withTurn(admitted.incrementAndGet());
            ack.admittedAs(stamped.turn());
            pendingOffers.add(new PendingOffer<>(stamped, ack.sink(), System.nanoTime() + CONCURRENT_EMISSION_RETRY_WINDOW.toNanos()));
        }
        drainPendingOffers();
        return true;
    }

    private void drainPendingOffers() {
        while (true) {
            // One drain at a time. A caller that finds one already running has left its own offer on the queue,
            // and the drain that is running re-checks the queue after it releases, below, so that offer is never
            // left with nobody to take it.
            if (!offerDrainRunning.compareAndSet(false, true)) {
                return;
            }
            boolean headWaitingOnSink;
            try {
                headWaitingOnSink = takeQueuedOffersToTheSink();
            } finally {
                offerDrainRunning.set(false);
            }
            if (headWaitingOnSink) {
                // Scheduled after releasing, never before. Scheduling first leaves a window where the retry runs,
                // finds this drain still holding, and returns, with this drain about to release and go home.
                Schedulers.parallel().schedule(this::drainPendingOffers,
                        CONCURRENT_EMISSION_RETRY_DELAY.toNanos(), TimeUnit.NANOSECONDS);
                return;
            }
            // An offer that arrived while this drain held the flag saw it set and returned, so look again before
            // leaving rather than letting its acknowledgement wait for a caller that never comes.
            if (pendingOffers.isEmpty()) {
                return;
            }
        }
    }

    // Takes the queue to the sink in order, oldest first. Returns true when it stopped because the head could not
    // be handed over yet and is waiting for another attempt, false when it emptied the queue.
    private boolean takeQueuedOffersToTheSink() {
        while (true) {
            PendingOffer<K> pending = pendingOffers.peek();
            if (pending == null) {
                return false;
            }
            if (droppedByStop(pending.item())) {
                pendingOffers.poll();
                continue;
            }
            Sinks.EmitResult result = liveSink.tryEmitNext(pending.item());
            if (!result.isFailure()) {
                pendingOffers.poll();
                // A stop between the check above and the emit found it in neither queue, so it is taken out here.
                // Until then deliverItem(..) skips it, since the stop claimed its acknowledgement, and no other offer
                // reaches the sink while this drain holds the flag.
                if (droppedByStop(pending.item())) {
                    liveBuffer.removeIf(queued -> queued == pending.item());
                }
                continue;
            }
            switch (result) {
                case FAIL_NON_SERIALIZED -> {
                    if (System.nanoTime() >= pending.deadline()) {
                        pendingOffers.poll();
                        dropFromBacklogAndDrains(pending.item());
                        pending.ack().error(new PreDispatchRefusalException(this, HandoverMessages.concurrentEmission()));
                        continue;
                    }
                    // Left at the head, so whatever runs next starts with it and the order holds.
                    return true;
                }
                // The live pipeline has ended, so nothing is coming to deliver this payload. No path reaches this, since
                // only the live phase takes from the sink and deliverItem(..) recovers from every live delivery error.
                // An error outside a delivery would end it, and the pipeline's error handler records and logs that
                // error. A null failure means the handler has not run yet, so the refusal has no cause attached, and
                // the handler's log line is where the cause shows up.
                case FAIL_TERMINATED, FAIL_CANCELLED -> {
                    pendingOffers.poll();
                    dropFromBacklogAndDrains(pending.item());
                    Throwable failure = failure();
                    pending.ack().error(failure == null
                            ? new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun))
                            : refusal(failure));
                }
                default -> {
                    pendingOffers.poll();
                    dropFromBacklogAndDrains(pending.item());
                    pending.ack().error(new PreDispatchRefusalException(this, HandoverMessages.bufferOverflow(maxBufferedEvents, result)));
                }
            }
        }
    }

    // The bookkeeping deliver(..) does in its doFinally, for a payload that never reaches it. Without it a drain that
    // counted the payload never reaches zero, so its source is never told its buffer drained and its replay turn is
    // never given back, which parks every later replay.
    private void dropFromBacklogAndDrains(Item<K> item) {
        if (!settledHere(item)) {
            return;
        }
        List<Drain<T>> exhausted;
        synchronized (admission) {
            liveBacklog.decrementAndGet();
            exhausted = countTowardsDrainUnderAdmission(item.turn());
        }
        tellDrainedSources(exhausted);
        endIfDrained();
    }

    // Nothing a source does here reaches the caller whose payload ended the drain. Its acknowledgement is still owed
    // and the offers behind it still have to be taken to the sink, and on the delivery path the pipeline drops what
    // is thrown from a doFinally anyway, so a throwing callback is logged rather than carried out of here. The turn
    // goes back either way.
    private void tellDrainedSources(List<Drain<T>> exhausted) {
        for (Drain<T> drain : exhausted) {
            try {
                runAsOwnCode(drain.source()::liveDrained);
            } catch (RuntimeException | Error e) {
                log.error("The catch-up-then-live handover for this {} failed while telling a source that the live "
                        + "payloads buffered during its catch-up had been delivered. They were delivered, and the "
                        + "handover goes on running.", noun, e);
            } finally {
                releaseReplayTurn(drain.holdsReplayTurn());
            }
        }
    }

    // An offer waiting its turn at the sink, with the point in time after which this engine stops offering it and
    // reports contention instead.
    private record PendingOffer<K>(Item<K> item, MonoSink<Outcome> ack, long deadline) {
    }

    /**
     * Whether this engine refuses every live payload from now on and will go on refusing. True once this handover
     * has started failing, which a failed {@link #catchUp(Source)} and a failed payload answered once it was queued
     * both do, see {@link #acceptReportingDelivery(Object)}, and never false again after that. False while replaying,
     * while buffering, and once live.
     * <p>
     * Distinct from a replay that is still running, which also cannot deliver but is going to succeed. A caller
     * deciding whether to stop for good needs to tell those two apart, and reading this after the fact is safe
     * precisely because it only ever goes from false to true.
     */
    public boolean refusesPermanently() {
        return terminalError.get() != null || failing.get() != null;
    }

    /**
     * Stop a handover that has not gone live, has no {@link #catchUp(Source)} running and is not failing, as a replay
     * stopped before going live does. Every payload waiting for a catch-up is answered as not applied, so
     * {@link #accept(Object)} errors and {@link #acceptReportingDelivery(Object)} completes {@code false} for it, and so
     * is every payload fed after the stop, until the next {@link #catchUp(Source)}. A catch-up started after the stop
     * does not deliver those payloads, so their callers offer them again. Without this, a payload fed before any
     * catch-up started would wait for one that a shutting-down application never runs.
     * <p>
     * A catch-up counts as running from the {@link #catchUp(Source)} call until it goes live, its replay is stopped
     * through {@link Source#keepReplaying()}, or it fails, also while it waits for another replay to end. Does nothing
     * while one runs, since that catch-up answers the waiting payloads itself. It delivers them once it goes live,
     * answers them as not applied when its replay is stopped through {@link Source#keepReplaying()}, and errors them
     * with its failure when it fails. Does nothing to a live handover either, or to one that is failing, which answers
     * them with its failure.
     */
    public void stopIfNotCatchingUp() {
        List<LiveAck> dropped;
        synchronized (catchUpsGuard) {
            if (live || catchUpsInProgress > 0 || failureStarted.get() != null) {
                return;
            }
            stopped = true;
            // Taken while catchUpsGuard is held, so a catch-up starting right after this stop cannot have its own
            // payloads taken here. No drain is registered while the handover is not live and nothing catches up or
            // fails, so this tells no source anything.
            dropped = dropPendingLiveAcks();
        }
        // Answered after catchUpsGuard is released, since an answer runs the caller's own code.
        dropped.forEach(ack -> ack.sink().success(Outcome.STOPPED));
    }

    // Gives the count back once per catch-up, whichever way it ends.
    private void catchUpEnded(AtomicBoolean counted) {
        if (!counted.compareAndSet(true, false)) {
            return;
        }
        synchronized (catchUpsGuard) {
            catchUpsInProgress--;
        }
    }

    /**
     * Run the one-time catch-up: replay the source's history, record the completion marker, then start delivering the
     * live feed. The returned {@link Mono} completes when the replay and marker are done (see the class javadoc for
     * how that relates to the buffered live payloads), emitting {@code true} when the catch-up finished and
     * {@code false} when {@link Source#keepReplaying()} stopped it partway. A failure errors it instead, and so does a
     * call while this handover is failing, see {@link #acceptReportingDelivery(Object)}, without replaying or writing
     * the marker. Called from code this handover is running, it emits {@code true} without waiting for the replay, see
     * below.
     * <p>
     * A replay waits for a live payload still being delivered and then holds the live payloads back until it ends,
     * which matters on a handover that is already live, a feed's {@code catchUp()} after its {@code goLive()}. They are
     * delivered when it ends, whether it completes or is stopped, since a view that buffers during a replay throws that
     * buffer away on a stop.
     * <p>
     * A catch-up with nothing to replay that arrives while a replay holds the live payloads back completes only once
     * that replay ends, so when it emits {@code true}, {@link #acceptIfLive(Object)} accepts unless a replay started
     * after this call. When a catch-up on this handover failed while it waited, it errors instead, with a catch-up
     * failure recorded while it waited as the cause. Its own refusal is not recorded as a failure.
     * <p>
     * A catch-up called from code this handover is running emits {@code true} without waiting, whether it has anything
     * to replay or not, since the replay or the hold on live delivery it would wait for cannot end before that code
     * returns. That {@code true} means the catch-up was asked for, not that it has run. It does not start before that
     * code returns, and it can still be stopped, or refused because another catch-up on this handover failed. Neither
     * is reported to that code. Once it runs, it replays like any other catch-up, holding live payloads back until it
     * ends, and a failure of it starts this handover failing like any failed catch-up. It replays nothing and writes no
     * marker when a replay that started after the call has gone live by then, since this handover takes every catch-up
     * to replay the same history. So calls that code makes before a replay starts are all answered by that one replay
     * once it goes live. Code that calls this for a
     * payload asks for another catch-up each time a replay delivers that payload again, and each of those catch-ups
     * whose {@link Source#isAlreadyCaughtUp()} answers {@code false} replays again. With a source that always answers
     * {@code false}, the replays do not end. That code is a fold, live or replayed, {@link Source#alreadyDeliveredByReplay(Object)},
     * {@link Source#replayStarted()}, {@link Source#replayCompleted()}, {@link Source#replayAbandoned()},
     * {@link Source#historyDone()} and {@link Source#liveDrained()}. This handover
     * recognizes the call when that code subscribes the returned {@code Mono} on the thread this handover called it on,
     * which blocking on it does, or returns the {@code Mono} as part of its own. Every other method here that takes a
     * payload recognizes its caller the same way. A call from that code errors while this handover is failing, the same
     * as a call from other code, and errors when reading the catch-up marker fails. While a replay holds live delivery
     * back, {@link #acceptIfLive(Object)} goes on refusing until that replay ends. Code that blocks on the result from
     * a thread it switched to is not recognized, so it waits like any other caller, for a replay or a hold on live
     * delivery that cannot end while that code blocks.
     */
    public Mono<Boolean> catchUp(Source<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        // A handover that is failing has forgotten its marker and is about to fail for good, so a catch-up that
        // replayed now would write the marker back and report a handover live that refuses every event. A caller
        // that asks for a catch-up once it has failed for good still gets one, which is what it asked for.
        // Read before failureStarted, so a failure that starts after this point is never mistaken for one this call
        // was made after, and the checks below refuse it.
        Throwable failedForGoodBefore = terminalError.get();
        Failing current = failureStarted.get();
        if (current != null && failedForGoodBefore == null) {
            return Mono.error(refusal(current.cause()));
        }
        // Counted from here until it goes live, is stopped or fails, so stopIfNotCatchingUp() lets this catch-up
        // answer the payloads waiting now. Taken before the source is asked anything, since asking it can take as long
        // as the source likes, and given back here when building or subscribing the pipeline throws, since no way that
        // pipeline ends can give it back then.
        AtomicBoolean counted = new AtomicBoolean(true);
        synchronized (catchUpsGuard) {
            catchUpsInProgress++;
            // A fresh catch-up revives a handover a previous one stopped, so stopping is recoverable by replaying
            // again rather than only by building a new one.
            stopped = false;
        }
        try {
            return startCountedCatchUp(source, failedForGoodBefore, counted);
        } catch (RuntimeException | Error e) {
            catchUpEnded(counted);
            throw e;
        }
    }

    private Mono<Boolean> startCountedCatchUp(Source<T> source, Throwable failedForGoodBefore, AtomicBoolean counted) {
        Sinks.One<Boolean> catchupDone = Sinks.one();

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = source.isAlreadyCaughtUp().cache();
        // Tracks whether replayStarted() ran and replayCompleted() has not yet closed it out, so the error handler
        // below knows whether there is a replay lifecycle left open to abandon, rather than calling replayAbandoned()
        // after a clean replayCompleted() has already told the view its batch is durable (ADR 110).
        AtomicBoolean replayOpen = new AtomicBoolean(false);
        // This catch-up's own hold on live delivery, installed only when it actually replays, and the only thing it
        // ever releases.
        Sinks.Empty<Void> pause = Sinks.empty();
        // This catch-up's own drain, so the payloads buffered while it read its history are counted against it and
        // against no other catch-up.
        AtomicReference<Drain<T>> myDrain = new AtomicReference<>();
        // Whether this catch-up is the replay holding the turn, so only the one that took it releases it.
        AtomicBoolean holdsReplayTurn = new AtomicBoolean();
        // Whether this catch-up subscribed the live sink, so a failure of its pipeline is known to end live delivery.
        AtomicBoolean deliversLive = new AtomicBoolean();
        // Read when this catch-up starts, so the check after the turn asks whether a catch-up failed while this one
        // waited rather than whether the handover had already failed when the caller asked for this one.
        Throwable failureBeforeWaiting = failedForGoodBefore;
        RecordedFailure latestFailureBeforeWaiting = latestFailure.get();
        // Set when this catch-up refuses because another one failed, so the error handler below does not record that
        // refusal as a failure of its own.
        AtomicBoolean refusedForAnotherFailure = new AtomicBoolean();
        // Set when markCaughtUp() is called, so a failure of this catch-up forgets a marker whose write errored and
        // can still have been stored. Cleared only after the check that follows a marker write has forgotten the
        // marker again, and left set once a write succeeded.
        AtomicBoolean markerMayBeWritten = new AtomicBoolean();
        // How many replays had started when own code asked for this catch-up, or -1 when no own code asked for it.
        java.util.concurrent.atomic.AtomicLong askedByOwnCodeAfter = new java.util.concurrent.atomic.AtomicLong(-1);
        // The number of this catch-up's replay, 0 while it has not started one.
        java.util.concurrent.atomic.AtomicLong replayNumber = new java.util.concurrent.atomic.AtomicLong();
        // Set when a replay that started after own code asked for this catch-up has gone live, so this catch-up
        // replays nothing and writes no marker.
        AtomicBoolean answeredByAnotherReplay = new AtomicBoolean();
        // Three sequential phases, not stages of one Flux.concat. The marker must not be written until every replayed
        // payload has actually been folded, and a concat sibling cannot express that: concatMap's prefetch drains the
        // replay into its queue, so the replay Flux completes as soon as its items are emitted and concat moves on to
        // the next sibling while the folds are still running. That wrote the marker mid-replay for an asynchronous
        // fold, and since the marker makes a restart skip the replay, the unfolded events were lost with no error.
        Mono<Void> replayFolded = alreadyDone.flatMap(done -> {
            if (done) {
                // Waits for a replay already holding live delivery back, so this call does not report the handover
                // live while acceptIfLive(..) still refuses. It waits only for the hold in place now, since this call
                // makes no promise about a replay that starts after it. It errors when a catch-up failed while it
                // waited, including on a handover that had already failed before this call.
                return awaitLiveDeliveryResumed().then(Mono.defer(() -> {
                    RecordedFailure failed = latestFailure.get();
                    if (failed == null || failed == latestFailureBeforeWaiting) {
                        return Mono.<Void>empty();
                    }
                    refusedForAnotherFailure.set(true);
                    return Mono.<Void>error(refusal(failed.cause()));
                }));
            }
            // Deferred, so the hold is installed once the turn is taken rather than when this pipeline is put together.
            return awaitReplayTurn(holdsReplayTurn).then(Mono.defer(() -> {
                // The catch-up this one waited for failing leaves the handover refusing everything and its caller
                // told to replace it. So this replay does not start and fold a history into a view its caller was
                // told to stop using. A caller that asks for a catch-up on a handover that had already failed still
                // gets one, which is what it asked for.
                Throwable failed = failure();
                if (failed == null || failed == failureBeforeWaiting) {
                    long asked = askedByOwnCodeAfter.get();
                    if (asked >= 0 && latestReplayGoneLive.get() > asked) {
                        answeredByAnotherReplay.set(true);
                        return Mono.<Void>empty();
                    }
                    return pauseLiveDelivery(pause);
                }
                refusedForAnotherFailure.set(true);
                return Mono.<Void>error(refusal(failed));
            })).then(Mono.defer(() -> {
                if (answeredByAnotherReplay.get()) {
                    return Mono.<Void>empty();
                }
                // Cleared again here, not only when this call was made, because the catch-up it waited for can have
                // stopped in between. The payloads arriving during this replay belong in its buffer, and a handover
                // left stopped would drop them.
                //
                // Written without the gate's monitor, which holds because this runs in a defer chained after
                // pauseLiveDelivery(pause) completed. A payload that reads false here is therefore entering a sink
                // this catch-up has already paused and is the one to resume. Moving this ahead of that pause, or out
                // of the defer, breaks it.
                stopped = false;
                // Every key belongs to the source a suppression reports to, so a new replay starts from none.
                replayedIds.clear();
                replaySource.set(source);
                replayNumber.set(replaysStarted.incrementAndGet());
                runAsOwnCode(source::replayStarted);
                replayOpen.set(true);
                return source.replay().map(this::replayedItem)
                        // Checked inside the concatMap function rather than upstream of it. An upstream takeWhile would
                        // run at emission, and concatMap prefetches, so it could race far ahead of the folds. This is
                        // serialized per payload with the fold itself, which is the same reason the phases here are
                        // sequential.
                        .concatMap(item -> source.keepReplaying() ? deliver(item) : Mono.error(CatchupStopped.INSTANCE))
                        .then()
                        // Ordered before the marker and before the live buffer drain, so anything a replay-aware view
                        // buffered is durable before either runs.
                        .then(subscribedAsOwnCode(Mono.defer(source::replayCompleted)))
                        .doOnSuccess(ignored -> replayOpen.set(false));
            }));
        });
        // This handover can start failing while this catch-up replays, and a marker written after the failure forgot
        // it makes the next catch-up skip the replay that delivers the failed payload again. So the marker is not
        // written once the failure has started, and is forgotten again when the failure started while it was being
        // written, since the failure's own forget can have run first. A failure marks its start before it forgets.
        Mono<Void> recordMarker = alreadyDone.flatMap(done -> {
            if (done || answeredByAnotherReplay.get()) {
                return Mono.<Void>empty();
            }
            Throwable before = failure();
            if (before != null && before != failureBeforeWaiting) {
                refusedForAnotherFailure.set(true);
                return Mono.<Void>error(refusal(before));
            }
            markerMayBeWritten.set(true);
            return source.markCaughtUp().then(Mono.defer(() -> {
                Throwable after = failure();
                if (after == null || after == failureBeforeWaiting) {
                    return Mono.<Void>empty();
                }
                refusedForAnotherFailure.set(true);
                return forgetCaughtUp(List.of(source))
                        .then(Mono.fromRunnable(() -> markerMayBeWritten.set(false)))
                        .then(Mono.<Void>error(refusal(after)));
            }));
        });

        replayFolded
                // Before the marker, before the catch-up signal and before the drain, on every path into it,
                // including the already-caught-up one that skipped the replay entirely. Ahead of the signal
                // specifically because a source's own subscriber to it runs inline and may forget the id, and this
                // running after that would leave state behind that nothing removes.
                .then(Mono.fromRunnable(() -> {
                    runAsOwnCode(source::historyDone);
                    // Taken after historyDone, under the same guard admission uses, so every payload already
                    // taken in has a turn at or below the boundary and every later one is above it. Counting
                    // deliveries alone was not enough: a payload taken in after the boundary, delivered before one
                    // taken in before it, would count against the drain and end it early.
                    synchronized (admission) {
                        Drain<T> drain = new Drain<>(source, admitted.get(), new java.util.concurrent.atomic.AtomicLong(liveBacklog.get()), holdsReplayTurn);
                        myDrain.set(drain);
                        drains.add(drain);
                    }
                }))
                .then(recordMarker)
                .doOnSuccess(ignored -> {
                    // Set before the live sink starts draining, the same point BlockingHandover.drainBufferAndGoLive
                    // flips its own live field. A payload acceptIfLive(..) sees after this point is treated as live
                    // even while whatever buffered ahead of it during the replay is still being delivered.
                    live = true;
                    // After live is set, so a stop that finds no catch-up running finds the handover live instead,
                    // and lets this drain deliver the payloads it is about to deliver.
                    catchUpEnded(counted);
                    latestReplayGoneLive.accumulateAndGet(replayNumber.get(), Math::max);
                    // Held here until the marker is written, not from the end of the replay, so a payload a handover
                    // that was already live held back is never delivered and acknowledged while a phase that can still
                    // fail is running. A failure fails its acknowledgement instead, and its caller offers it again.
                    resumeLiveDelivery(pause);
                    // An empty buffer has nothing to deliver, so its drain is over the moment the handover is.
                    // Signalled here rather than beside historyDone, so a listener that frees the id on this cannot
                    // do it while the marker is still unwritten. A buffer with anything in it reaches liveDrained
                    // from the last delivery instead, which also runs after this point.
                    boolean nothingBuffered;
                    Drain<T> drain = myDrain.get();
                    synchronized (admission) {
                        nothingBuffered = drain != null && drain.remaining().get() == 0L && drains.remove(drain);
                    }
                    if (nothingBuffered) {
                        runAsOwnCode(source::liveDrained);
                        // Otherwise the last delivery of the drain gives the turn back, since the payloads it holds
                        // were checked against this replay's keys and are reported to this replay's source.
                        releaseReplayTurn(holdsReplayTurn);
                    }
                    catchupDone.tryEmitValue(true);
                })
                .thenMany(Flux.defer(() -> {
                    if (!liveSinkSubscribed.compareAndSet(false, true)) {
                        return Flux.<Void>empty();
                    }
                    deliversLive.set(true);
                    liveSource.set(source);
                    return liveDelivery();
                }))
                // Covers the live folds as well as the replay, so a catch-up composed into either answers without
                // waiting for the replay or the pause that is waiting for it.
                .contextWrite(Context.of(insideOwnPipeline, Boolean.TRUE))
                // This engine subscribes its own pipeline rather than handing it back, so without a scheduler the
                // replay would run on whoever called catchUp, which is the Spring refresh thread for an annotated
                // projection. boundedElastic because the replay folds through blocking bridges.
                .subscribeOn(Schedulers.boundedElastic())
                // Nothing keeps this subscription, and the Mono handed back is the catch-up signal rather than the
                // pipeline, so a caller cancelling what it got drops its own listener and cannot cancel a replay.
                // That is why no path here restores the replay turn or the stopped flag on a cancel.
                .subscribe(ignored -> {
                }, error -> {
                    if (error == CatchupStopped.INSTANCE) {
                        // Stopped, not failed. No marker, no drain, and no terminal error, so the handover stays
                        // usable. A handover that never went live drops what it buffered and answers each of those
                        // payloads STOPPED, so accept(..) errors and its caller offers it again. One that was already
                        // live goes on delivering them, the same as the blocking engine.
                        boolean wasLive = live;
                        if (!wasLive) {
                            stopped = true;
                        }
                        abandonReplayWithoutMasking(source, replayOpen);
                        // Answered before the pause is lifted, the same order the failure path below uses, so a
                        // caller offering a payload again cannot have it delivered while the copy it is replacing is
                        // still waiting for an answer.
                        if (!wasLive) {
                            dropPendingLiveAcks().forEach(dropped -> dropped.sink().success(Outcome.STOPPED));
                        }
                        catchUpEnded(counted);
                        resumeLiveDelivery(pause);
                        releaseReplayTurn(holdsReplayTurn);
                        // Emitted last, so a caller that reacts to the stop by calling goLive() finds the payloads
                        // this stop answered already answered rather than answered while that call is running.
                        catchupDone.tryEmitValue(false);
                        return;
                    }
                    if (error == FailedForGood.INSTANCE) {
                        // The live pipeline ending because this handover failed for good, which failedForGood(..)
                        // has already dealt with.
                        return;
                    }
                    try {
                        failed(error, source, catchupDone, replayOpen, pause, myDrain, holdsReplayTurn, deliversLive,
                                refusedForAnotherFailure, markerMayBeWritten);
                    } finally {
                        // After failed(..) has marked this handover failing, so a stop that finds no catch-up running
                        // lets that failure answer the payloads.
                        catchUpEnded(counted);
                    }
                });

        // A call from a fold or a Source callback of this handover answers true without waiting, since the replay or
        // the pause it would wait for cannot end before that code does. The pipeline above runs once that code returns,
        // and replays nothing when a replay that started after this call has gone live by then.
        return Mono.deferContextual(context -> {
            if (!ownCode(context)) {
                return catchupDone.asMono();
            }
            askedByOwnCodeAfter.compareAndSet(-1, replaysStarted.get());
            return alreadyDone.thenReturn(true);
        });
    }

    // Claims every acknowledgement nothing has answered yet, so none of those payloads is delivered later. Each one
    // already admitted gives back its place in the backlog, in the sink's queue and in any drain counting it. The
    // caller answers them, outside the guard.
    private List<LiveAck> dropPendingLiveAcks() {
        List<LiveAck> dropped = new ArrayList<>();
        List<Drain<T>> exhausted = new ArrayList<>();
        synchronized (admission) {
            for (LiveAck ack : pendingLiveAcks) {
                if (!ack.dropForStop()) {
                    continue;
                }
                dropped.add(ack);
                if (ack.turn() != LiveAck.NOT_ADMITTED) {
                    liveBacklog.decrementAndGet();
                    exhausted.addAll(countTowardsDrainUnderAdmission(ack.turn()));
                }
            }
            liveBuffer.removeIf(ReactiveHandover::droppedByStop);
        }
        tellDrainedSources(exhausted);
        return dropped;
    }

    // Guarded so that a source's own replayAbandoned() throwing cannot replace the failure (or stop) that made the
    // engine call it in the first place; the contract asks the source not to throw here, but this engine does not
    // trust that. compareAndSet so a clean replayCompleted() (which already cleared replayOpen) is never followed by
    // an abandon call for a lifecycle that already closed successfully.
    private void abandonReplayWithoutMasking(Source<T> source, AtomicBoolean replayOpen) {
        if (replayOpen.compareAndSet(true, false)) {
            // A view that buffers during a replay discards that buffer here, so a key the replay left behind would
            // suppress the only copy of an event the read model never got. Forgetting it costs a view that wrote
            // through a second delivery, which at-least-once delivery allows.
            replayedIds.clear();
            replaySource.set(null);
            try {
                runAsOwnCode(source::replayAbandoned);
            } catch (Throwable ignored) {
                // Throwable rather than RuntimeException and Error, because a view written in Kotlin can throw a
                // checked exception without declaring it. One that got past here left the rest of the failure
                // handling unrun, so nothing recorded the failure, the payloads waiting for an answer never got one,
                // and the replay turn never went back.
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
    // Assumes the admission guard is held, which is what the drain snapshot is taken under. Without that, a delivery
    // finishing between the snapshot and the registration would be counted into a drain that nothing can decrement,
    // and that drain would never end. Returns the drains this payload exhausted, so their sources are told outside
    // the guard rather than under it.
    private List<Drain<T>> countTowardsDrainUnderAdmission(long turn) {
        List<Drain<T>> exhausted = new ArrayList<>(1);
        for (Drain<T> drain : drains) {
            if (turn > drain.boundaryTurn()) {
                continue;
            }
            long left = drain.remaining().updateAndGet(value -> value > 0L ? value - 1L : value);
            if (left == 0L && drains.remove(drain)) {
                exhausted.add(drain);
            }
        }
        return exhausted;
    }

    private static boolean droppedByStop(Item<?> item) {
        LiveAck ack = item.ack();
        return ack != null && ack.droppedByStop();
    }

    // Whether this path gives back the payload's place in the backlog and in the drains. A stop that answered the
    // payload already gave those back, so every other path settles the acknowledgement first and does nothing more
    // when the stop got there first.
    private boolean settledHere(Item<K> item) {
        LiveAck ack = item.ack();
        if (ack == null) {
            return true;
        }
        ack.settle();
        return !ack.droppedByStop();
    }

    // Counted after the payload has been delivered rather than before it, so the last buffered one is still part of
    // the drain while it is being handled.
    private Mono<Void> deliver(Item<K> item) {
        Mono<Void> delivery = item.ack() == null ? deliverItem(item) : deliverWhenNoReplayRuns(item);
        return delivery.doFinally(signal -> {
            if (!settledHere(item)) {
                return;
            }
            List<Drain<T>> exhausted;
            // The count and the backlog move together under the guard the drain snapshot is taken under, so a drain
            // registered right now either counts this payload and hears about it, or counts neither.
            synchronized (admission) {
                if (item.ack() != null) {
                    liveBacklog.decrementAndGet();
                }
                exhausted = countTowardsDrainUnderAdmission(item.turn());
            }
            tellDrainedSources(exhausted);
            endIfDrained();
        });
    }

    // A live payload waits here while a replay runs, and is delivered once that replay has ended, completed or stopped.
    private Mono<Void> deliverWhenNoReplayRuns(Item<K> item) {
        // A payload whose acknowledgement the failure of this handover answered with an error is not delivered here,
        // since its caller offers it again. That failure claimed the acknowledgement, so deliverItem(..) skips it. Every
        // other payload is delivered, whatever this handover's state, since its caller was told it was taken in.
        return Mono.defer(() -> {
            Sinks.Empty<Void> paused;
            synchronized (liveGate) {
                paused = livePaused;
                if (paused == null) {
                    liveDelivering = true;
                }
            }
            if (paused != null) {
                return paused.asMono().then(deliverWhenNoReplayRuns(item));
            }
            return deliverItem(item).doFinally(signal -> liveDeliveryEnded());
        });
    }

    // Completes once no live payload is being delivered, and none starts until the catch-up that owns this pause
    // releases it. The owner is what stops a concurrent catch-up with nothing to replay from opening a running
    // replay's pause and letting live payloads into a view that replay may still discard.
    // Completes once no other replay is running, with the turn taken. Subscribing again after the holder releases is
    // how two waiters settle which of them takes it, since both are told at once and only one wins the monitor.
    private Mono<Void> awaitReplayTurn(AtomicBoolean holdsTurn) {
        return Mono.defer(() -> {
            Sinks.Empty<Void> running;
            synchronized (replayTurn) {
                if (replayTurnReleased == null) {
                    replayTurnReleased = Sinks.empty();
                    holdsTurn.set(true);
                    return Mono.empty();
                }
                running = replayTurnReleased;
            }
            return running.asMono().then(Mono.defer(() -> awaitReplayTurn(holdsTurn)));
        });
    }

    // Called on every path out of a catch-up that took the turn, so a waiting replay is not held by one that ended.
    private void releaseReplayTurn(AtomicBoolean holdsTurn) {
        if (!holdsTurn.compareAndSet(true, false)) {
            return;
        }
        Sinks.Empty<Void> released;
        synchronized (replayTurn) {
            released = replayTurnReleased;
            replayTurnReleased = null;
        }
        if (released != null) {
            released.tryEmitEmpty();
        }
        endIfDrained();
    }

    private Mono<Void> pauseLiveDelivery(Sinks.Empty<Void> pause) {
        synchronized (liveGate) {
            if (livePaused == null) {
                livePaused = pause;
            }
            if (!liveDelivering) {
                return Mono.empty();
            }
            if (liveIdle == null) {
                liveIdle = Sinks.empty();
            }
            return liveIdle.asMono();
        }
    }

    // Completes once the hold on live delivery in place right now is released, or at once when there is none. A hold
    // is released on every path out of the replay that took it, completed, stopped or failed.
    private Mono<Void> awaitLiveDeliveryResumed() {
        return Mono.defer(() -> {
            Sinks.Empty<Void> paused;
            synchronized (liveGate) {
                paused = livePaused;
            }
            return paused == null ? Mono.<Void>empty() : paused.asMono();
        });
    }

    private boolean liveDeliveryPaused() {
        synchronized (liveGate) {
            return livePaused != null;
        }
    }

    private void liveDeliveryEnded() {
        Sinks.Empty<Void> idle;
        synchronized (liveGate) {
            liveDelivering = false;
            idle = liveIdle;
            liveIdle = null;
        }
        if (idle != null) {
            idle.tryEmitEmpty();
        }
    }

    private void resumeLiveDelivery(Sinks.Empty<Void> pause) {
        boolean owned;
        synchronized (liveGate) {
            owned = livePaused == pause;
            if (owned) {
                livePaused = null;
            }
        }
        if (owned) {
            pause.tryEmitEmpty();
        }
    }

    private Mono<Void> deliverItem(Item<K> item) {
        LiveAck liveAck = item.ack();
        if (liveAck != null) {
            if (!liveAck.settle()) {
                // A stop already answered it, and its caller offers it again.
                return Mono.empty();
            }
            if (deliveredIds.contains(item.dedupKey())) {
                answerDelivered(liveAck);
                return Mono.empty();
            }
            if (replayedIds.contains(item.dedupKey())) {
                // Applied by the replay already, so delivering it again would apply it twice. The source is told
                // instead, and the acknowledgement waits for that call rather than running ahead of it (ADR 137).
                return answerFailureTo(liveAck, item.dedupKey(), subscribedAsOwnCode(Mono.defer(item.alreadyDeliveredByReplay()))
                        .doOnSuccess(v -> answerDelivered(liveAck)));
            }
            // Mono.defer so a synchronous throw from the fold becomes an onError signal onErrorResume can catch, rather
            // than aborting the whole pipeline.
            return answerFailureTo(liveAck, item.dedupKey(), subscribedAsOwnCode(Mono.defer(item.deliver()))
                    .doOnSuccess(v -> {
                        deliveredIds.add(item.dedupKey());
                        answerDelivered(liveAck);
                    }));
        }
        // Replay payload: an error here propagates and fails the catch-up.
        return subscribedAsOwnCode(Mono.defer(item.deliver())).doOnSuccess(v -> replayedIds.add(item.dedupKey()));
    }

    // Takes the payload out of pendingLiveAcks before answering it, so a failure of this handover after it cannot
    // answer it again. The dispose hook comes too late for that when the whole call runs on one thread. The caller
    // has then not asked for the answer yet, so the sink holds the value back, and an error sent to it in the
    // meantime reaches the caller in its place.
    private void answer(LiveAck liveAck, Outcome outcome) {
        pendingLiveAcks.remove(liveAck);
        liveAck.sink().success(outcome);
    }

    // A payload answered once queued was answered then, so only one whose caller still waits is answered here.
    private void answerDelivered(LiveAck liveAck) {
        if (!liveAck.answeredOnceQueued()) {
            answer(liveAck, Outcome.APPLIED);
        }
    }

    // A payload whose caller still waits gets the failure, and the pipeline goes on. One answered once queued has
    // nobody left to tell, so its failure makes this handover start failing, see acceptReportingDelivery(..).
    private Mono<Void> answerFailureTo(LiveAck liveAck, K dedupKey, Mono<Void> delivery) {
        return delivery.onErrorResume(error -> {
            if (!liveAck.answeredOnceQueued()) {
                liveAck.sink().error(error);
                return Mono.empty();
            }
            return failingFrom(dedupKey, error);
        });
    }

    // Only one payload is delivered at a time, so no second failure of a queued payload can arrive while the first is
    // forgetting the marker. The marker is forgotten before anything is refused, so a caller that sees the refusal and
    // subscribes again finds it gone.
    private Mono<Void> failingFrom(K dedupKey, Throwable error) {
        Failing cause = new Failing(error, true);
        if (!failureStarted.compareAndSet(null, cause)) {
            log.error("A payload of this {} with de-dup key {} failed to be delivered while it was delivering what it "
                    + "had taken in before failing for good. Its caller was told it was taken in, and the replay of the "
                    + "next catch-up delivers it again if the history holds it. The rest is still delivered.",
                    noun, dedupKey, error);
            return Mono.empty();
        }
        log.error("A payload of this {} with de-dup key {} failed to be delivered after its caller was told it was taken "
                + "in. The {} refuses every other event from now on, delivers what it has already taken in, and then "
                + "fails for good. Fix the cause, then replace it, a subscription by cancelling it and subscribing "
                + "again, a projection feed by building a new one. Its catch-up replays the history once the catch-up "
                + "marker is gone.", noun, dedupKey, noun, error);
        Source<T> source = liveSource.get();
        return forgetCaughtUp(source == null ? List.of() : List.of(source))
                .then(Mono.fromRunnable(() -> {
                    startFailing(cause);
                    endIfDrained();
                }));
    }

    // Every failure of a catch-up ends here, the replay, the marker read, the marker write and the live pipeline
    // alike. The first failure of this handover makes it start failing, the way a failed queued payload does, so a
    // payload its own code was told was queued still gets its delivery. A later one only tells its own caller.
    private void failed(Throwable error, Source<T> source, Sinks.One<Boolean> catchupDone, AtomicBoolean replayOpen,
                        Sinks.Empty<Void> pause, AtomicReference<Drain<T>> myDrain, AtomicBoolean holdsReplayTurn,
                        AtomicBoolean deliversLive, AtomicBoolean refusedForAnotherFailure,
                        AtomicBoolean markerMayBeWritten) {
        abandonReplayWithoutMasking(source, replayOpen);
        // This catch-up's own drain goes with its failure, so a payload left in the live sink cannot count it down
        // later and tell a source whose catch-up failed that its buffer drained. Every other drain goes too only when
        // this pipeline was the one delivering live, since then nothing ends them and each would hold its replay turn
        // for good. Otherwise they end as their payloads are delivered.
        List<Drain<T>> abandonedDrains = new ArrayList<>();
        synchronized (admission) {
            if (deliversLive.get()) {
                abandonedDrains.addAll(drains);
                drains.clear();
            } else {
                Drain<T> ownDrain = myDrain.get();
                if (ownDrain != null && drains.remove(ownDrain)) {
                    abandonedDrains.add(ownDrain);
                }
            }
        }
        boolean refused = refusedForAnotherFailure.get();
        Failing cause = new Failing(error, false);
        if (refused || !failureStarted.compareAndSet(null, cause)) {
            // The first failure is the one that matters, so a later call refusing because of it does not take its
            // place and hide the cause. A catch-up of its own that failed on a handover that had already failed is
            // recorded, so a call waiting for a replay can tell a catch-up failed while it waited.
            if (!refused) {
                latestFailure.set(new RecordedFailure(error));
            }
            // A marker write that errored can still have been stored after the first failure forgot the marker, so
            // this catch-up forgets it again before it tells its caller. A write the store applies after this forget
            // keeps the marker.
            Mono<Void> forgetOwnMarker = markerMayBeWritten.get() ? forgetCaughtUp(List.of(source)) : Mono.empty();
            forgetOwnMarker.subscribe(null, null, () -> {
                abandonedDrains.forEach(abandoned -> releaseReplayTurn(abandoned.holdsReplayTurn()));
                // Logged only when the catch-up signal can no longer error, which is the live phase, where
                // catchupDone has already emitted and nothing else tells anyone.
                if (catchupDone.tryEmitError(error).isFailure() && !refused) {
                    log.error("A catch-up of this {} failed after it had already reported its catch-up done, while "
                            + "the {} was already failing or had failed.", noun, noun, error);
                }
                resumeLiveDelivery(pause);
                releaseReplayTurn(holdsReplayTurn);
            });
            return;
        }
        log.error("A catch-up of this {} failed. The {} refuses every event that does not come from its own handler "
                + "from now on, delivers the events its handler fed it, and then fails for good. Fix the cause, then "
                + "replace it, a subscription by cancelling it and subscribing again, a projection feed by building a "
                + "new one. Its catch-up replays the history once the catch-up marker is gone.", noun, noun, error);
        Source<T> live = liveSource.get();
        List<Source<T>> sources = live == null || live == source ? List.of(source) : List.of(source, live);
        // The pause and the replay turn stay held until this handover is failing, so a catch-up waiting for either
        // sees the failure when it resumes. A replay that runs meanwhile without waiting for this failure checks for the
        // failure before and after it writes the marker, see recordMarker in catchUp(..).
        forgetCaughtUp(sources).subscribe(null, null, () -> {
            // Published before the turns go back, because giving a turn back resumes a catch-up waiting for it on
            // this thread, and that catch-up reads this to decide whether to replay at all.
            startFailing(cause);
            if (deliversLive.get()) {
                // The pipeline delivering live payloads ended with an error of its own. Nothing takes from the sink
                // again, since it accepts one subscriber, so what it still holds is never delivered. No path reaches
                // this today, because answerFailureTo(..) recovers from every error of a live delivery.
                long undelivered = liveBacklog.get();
                if (undelivered > 0) {
                    log.error("The pipeline delivering live events of this {} ended, and {} events it had taken in "
                            + "were not delivered. The replay of the next catch-up delivers each one the history holds.",
                            noun, undelivered);
                }
                failedForGood();
            } else {
                deliverWhatWasTakenIn();
            }
            abandonedDrains.forEach(abandoned -> releaseReplayTurn(abandoned.holdsReplayTurn()));
            resumeLiveDelivery(pause);
            releaseReplayTurn(holdsReplayTurn);
            endIfDrained();
            // The catch-up signal errors with the raw cause, since that caller asked about the catch-up itself. In the
            // live phase it has already emitted, and the log line above is what tells anyone then.
            catchupDone.tryEmitError(error);
        });
    }

    // Refuses every payload that does not come from this handover's own code from now on. After a failed catch-up,
    // each such payload still waiting gets that refusal too and is not delivered, since the view that catch-up
    // replayed into may be thrown away. After a failed queued payload, those are delivered like the rest.
    private void startFailing(Failing cause) {
        failing.compareAndSet(null, cause);
        latestFailure.set(new RecordedFailure(cause.cause()));
        if (!cause.queuedDeliveryFailed()) {
            dropPendingLiveAcks().forEach(ack -> ack.sink().error(refusal(cause.cause())));
        }
    }

    // A failure during a replay can come before any catch-up took the live sink, and every payload answered once it
    // was queued is waiting there. This takes the sink so those are delivered, and does nothing when a live pipeline
    // already runs.
    private void deliverWhatWasTakenIn() {
        if (!liveSinkSubscribed.compareAndSet(false, true)) {
            return;
        }
        liveDelivery()
                .contextWrite(Context.of(insideOwnPipeline, Boolean.TRUE))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(ignored -> {
                }, error -> {
                    if (error != FailedForGood.INSTANCE) {
                        log.error("The pipeline delivering what this {} had taken in before failing for good ended "
                                + "with an error. What it still held was not delivered. The replay of the next catch-up "
                                + "delivers each one the history holds.", noun, error);
                        failedForGood();
                    }
                });
    }

    private Flux<Void> liveDelivery() {
        return liveSink.asFlux().concatMap(this::deliver).mergeWith(failedForGood.asMono());
    }

    // Fails this handover for good once it is failing and nothing it took in is left to deliver. A replay still
    // holding its turn is waited for, since a payload its folds feed is taken in and delivered too.
    private void endIfDrained() {
        if (failing.get() == null || terminalError.get() != null || liveBacklog.get() != 0L) {
            return;
        }
        synchronized (replayTurn) {
            if (replayTurnReleased != null) {
                return;
            }
        }
        failedForGood();
    }

    private void failedForGood() {
        Throwable cause = failingCause();
        if (cause == null || !terminalError.compareAndSet(null, cause)) {
            return;
        }
        dropPendingLiveAcks().forEach(ack -> ack.sink().error(refusal(cause)));
        failedForGood.tryEmitError(FailedForGood.INSTANCE);
    }

    // Retried, since a marker left in place makes the next catch-up skip the replay that delivers a failed payload
    // again, and an attempt that takes longer than forgetAttemptTimeout counts as failed. Never errors, and always
    // completes, so the failure goes on either way, and the log line tells an operator what to remove.
    private Mono<Void> forgetCaughtUp(List<Source<T>> sources) {
        return Flux.fromIterable(sources)
                .concatMap(source -> subscribedAsOwnCode(Mono.defer(source::forgetCaughtUp))
                        .timeout(forgetAttemptTimeout)
                        .retryWhen(Retry.backoff(FORGET_RETRIES, FORGET_FIRST_RETRY_DELAY))
                        .onErrorResume(forgetFailure -> {
                            log.error("The catch-up marker of this {} could not be forgotten, so its next catch-up "
                                    + "skips the replay that delivers a failed event again. Delete the catch-up marker "
                                    + "of this {} before replacing it.", noun, noun, forgetFailure);
                            return Mono.empty();
                        }))
                .then();
    }

    // Package-private for the test of a forget that never completes, which would otherwise wait out four attempts of
    // FORGET_ATTEMPT_TIMEOUT. Not part of this handover's contract.
    void forgetAttemptTimeout(java.time.Duration timeout) {
        this.forgetAttemptTimeout = Objects.requireNonNull(timeout, "timeout cannot be null");
    }

    // The cause of a refusal while this handover is failing or has failed, worded for what failed.
    private PreDispatchRefusalException refusal(Throwable cause) {
        Failing current = failureStarted.get();
        if (current != null && current.queuedDeliveryFailed()) {
            return new PreDispatchRefusalException(this, HandoverMessages.queuedEventFailed(noun), cause);
        }
        return catchUpFailed(cause);
    }

    // Why this handover is failing, and whether that was a payload answered once it was queued rather than a catch-up.
    private record Failing(Throwable cause, boolean queuedDeliveryFailed) {
    }

    /**
     * Ends the live pipeline once this handover has failed for good. A singleton with no stack trace, compared by
     * identity, for the same reasons as {@link CatchupStopped}.
     */
    private static final class FailedForGood extends RuntimeException {
        private static final FailedForGood INSTANCE = new FailedForGood();

        private FailedForGood() {
            super("The handover failed for good", null, false, false);
        }
    }

    // Marks the thread only while it subscribes the fold or callback, so a later task on the same pooled thread is not
    // taken for code of this handover. The mark is lifted while a completion or error goes downstream, since a fold
    // that completes on the thread that subscribed it would otherwise leave the thread marked while code that is not
    // this handover's handles that signal, such as what a caller of acceptIfLive(..) runs next.
    private Mono<Void> subscribedAsOwnCode(Mono<Void> code) {
        return Mono.from(subscriber -> runAsOwnCode(() -> code.subscribe(new CoreSubscriber<Void>() {
            @Override
            public Context currentContext() {
                return subscriber instanceof CoreSubscriber<?> downstream ? downstream.currentContext() : Context.empty();
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                subscriber.onSubscribe(subscription);
            }

            @Override
            public void onNext(Void value) {
                runOutsideOwnCode(() -> subscriber.onNext(value));
            }

            @Override
            public void onError(Throwable error) {
                runOutsideOwnCode(() -> subscriber.onError(error));
            }

            @Override
            public void onComplete() {
                runOutsideOwnCode(subscriber::onComplete);
            }
        })));
    }

    private void runOutsideOwnCode(Runnable code) {
        Boolean outer = runningOwnCode.get();
        runningOwnCode.remove();
        try {
            code.run();
        } finally {
            if (outer != null) {
                runningOwnCode.set(outer);
            }
        }
    }

    private void runAsOwnCode(Runnable code) {
        Boolean outer = runningOwnCode.get();
        runningOwnCode.set(Boolean.TRUE);
        try {
            code.run();
        } finally {
            if (outer == null) {
                runningOwnCode.remove();
            }
        }
    }

    // A new instance per failure, so two failures of the same exception can still be told apart.
    private record RecordedFailure(Throwable cause) {
    }

    private Item<K> replayedItem(T replayed) {
        return new Item<>(() -> deliver.apply(replayed), Mono::empty, dedupKey(replayed), null);
    }

    // Resolved when the suppression happens rather than when the item was made, because a payload buffered during the
    // replay is made before that replay has started. A key in replayedIds means a replay set replaySource, so the null
    // branch is only reached when an abandon cleared both between the key check and this call.
    private Mono<Void> alreadyDeliveredByReplay(T payload) {
        Source<T> source = replaySource.get();
        return source == null ? Mono.empty() : source.alreadyDeliveredByReplay(payload);
    }

    // The blocking engine wraps its terminal failure in this message and this one used to propagate the raw cause, so
    // the same refusal read as a transient handler error on one stack and as a terminal one on the other. The recovery
    // differs (retry versus release and set up again), which is exactly what the message says, so both stacks say it.
    private PreDispatchRefusalException catchUpFailed(Throwable cause) {
        return new PreDispatchRefusalException(this, HandoverMessages.catchUpFailed(noun), cause);
    }

    @SuppressWarnings("ConstantValue") // The function is declared non-null, but it is caller-supplied and unenforced.
    private K dedupKey(T payload) {
        K key = dedupId.apply(payload);
        if (key == null) {
            throw new PreDispatchRefusalException(this, HandoverMessages.dedupKeyRequired());
        }
        return key;
    }

    // How a live payload's offer ended. acceptReportingDelivery(..) and acceptIfLive(..) report APPLIED as true and
    // everything else as false, and accept(..) errors with the reason.
    private enum Outcome {
        APPLIED, NOT_LIVE, STOPPED
    }

    // A live payload's acknowledgement. A stop and the path that delivers or refuses the payload both claim it, and
    // the first to claim it answers it and gives back the payload's place in the backlog and in the drains.
    private static final class LiveAck {
        private static final long NOT_ADMITTED = 0L;
        private static final int OPEN = 0;
        private static final int SETTLED = 1;
        private static final int DROPPED_BY_STOP = 2;

        private final MonoSink<Outcome> sink;
        // Answered once queued, so nobody waits for its delivery and a failure of it fails this handover instead.
        private final boolean answeredOnceQueued;
        private final java.util.concurrent.atomic.AtomicInteger state = new java.util.concurrent.atomic.AtomicInteger(OPEN);
        // Written and read under the admission guard only.
        private long turn = NOT_ADMITTED;

        private LiveAck(MonoSink<Outcome> sink, boolean answeredOnceQueued) {
            this.sink = sink;
            this.answeredOnceQueued = answeredOnceQueued;
        }

        private MonoSink<Outcome> sink() {
            return sink;
        }

        private boolean answeredOnceQueued() {
            return answeredOnceQueued;
        }

        private boolean settle() {
            return state.compareAndSet(OPEN, SETTLED);
        }

        private boolean dropForStop() {
            return state.compareAndSet(OPEN, DROPPED_BY_STOP);
        }

        private boolean droppedByStop() {
            return state.get() == DROPPED_BY_STOP;
        }

        private void admittedAs(long turn) {
            this.turn = turn;
        }

        private long turn() {
            return turn;
        }
    }

    // A replayed payload has a null ack. A live payload carries a LiveAck, whose sink completes with whether it was
    // genuinely delivered, so the caller can acknowledge. Both suppliers are bound to the payload at creation time, so
    // the key is the only type Item needs.
    private record Item<K>(Supplier<Mono<Void>> deliver, Supplier<Mono<Void>> alreadyDeliveredByReplay, K dedupKey,
                       @Nullable LiveAck ack, long turn) {
        private Item(Supplier<Mono<Void>> deliver, Supplier<Mono<Void>> alreadyDeliveredByReplay, K dedupKey,
                     @Nullable LiveAck ack) {
            this(deliver, alreadyDeliveredByReplay, dedupKey, ack, Long.MAX_VALUE);
        }

        private Item<K> withTurn(long turn) {
            return new Item<>(deliver, alreadyDeliveredByReplay, dedupKey, ack, turn);
        }
    }
}
