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
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.HandoverOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The shared reactive catch-up-then-live coordination: the replay, a catch-up-complete marker step, and the live feed
 * are composed into one ordered pipeline with {@link Flux#concat}, live payloads arriving during the replay are
 * buffered in a bounded unicast sink, and the replay-to-live overlap is de-duplicated by an id extracted from the
 * payload. Because the whole pipeline is serialized by {@code concatMap}, the de-dup cache needs no locking. Extracted
 * from (and mirrors exactly) the reactor projection feed and the reactor push subscription model.
 * <p>
 * {@code T} is the payload type, one for both phases. The caller decides what a payload carries, so where a replayed
 * payload has metadata a live one may not, that difference lives in the payload rather than in this engine's signature.
 * The live-versus-replay distinction that this engine does care about is {@link Item#ack()}, decided per payload at
 * runtime, not per type.
 * <p>
 * <strong>This engine's ordering differs from the blocking one on purpose</strong>: here, the catch-up-complete
 * {@link Mono} returned by {@link #catchUp(Source)} completes, and the marker is persisted, <em>before</em> the
 * buffered live payloads are folded (the marker step and the live sink are both stages of the same
 * {@code Flux.concat}, and the returned {@code Mono} completes at the marker stage, not at the end of the live
 * stream). The blocking engine's {@code BlockingHandover.catchUp} returns only <em>after</em> the buffered live
 * payloads are drained. Both are internally consistent: a blocking {@code accept} call returns before its payload is
 * folded during the catch-up window, whereas here a live payload's {@code accept} {@link Mono} completes only once
 * its fold has actually run (including payloads buffered during the replay), even though the catch-up-done signal
 * itself already fired. Neither ordering is "fixed" by this extraction; both are preserved as-is.
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
    }

    private final Function<T, Mono<Void>> deliver;
    private final Function<T, String> dedupId;
    private final int maxBufferedEvents;
    private final BoundedIdCache deliveredIds;
    private final Sinks.Many<Item> liveSink;
    // Acks of live payloads buffered but not yet folded, so a catch-up failure fails them rather than leaving the
    // caller's accept Monos hanging forever.
    private final Set<MonoSink<Void>> pendingLiveAcks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<@Nullable Throwable> terminalError = new AtomicReference<>();

    private ReactiveHandover(Function<T, Mono<Void>> deliver, Function<T, String> dedupId, HandoverOptions options) {
        this.deliver = deliver;
        this.dedupId = dedupId;
        this.maxBufferedEvents = options.maxBufferedEvents();
        this.deliveredIds = new BoundedIdCache(options.dedupCacheSize());
        this.liveSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(maxBufferedEvents));
    }

    /**
     * @param deliver Folds a payload, replayed during the catch-up or live once going live.
     * @param dedupId Extracts the replay-to-live de-dup key from a payload.
     * @param options De-dup cache size and live-buffer cap.
     */
    public static <T> ReactiveHandover<T> create(
            Function<T, Mono<Void>> deliver, Function<T, String> dedupId, HandoverOptions options) {
        Objects.requireNonNull(deliver, "deliver cannot be null");
        Objects.requireNonNull(dedupId, "dedupId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        return new ReactiveHandover<>(deliver, dedupId, options);
    }

    /**
     * Feed a live payload. The returned {@link Mono} completes once the payload has been folded (or immediately if it
     * is a de-duplicated overlap). Payloads fed before or during the catch-up are buffered and delivered after the
     * replay.
     */
    public Mono<Void> accept(T payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        return Mono.create(ackSink -> {
            ackSink.onDispose(() -> pendingLiveAcks.remove(ackSink));
            Throwable failure = terminalError.get();
            if (failure != null) {
                ackSink.error(failure);
                return;
            }
            pendingLiveAcks.add(ackSink);
            // Re-check after registering: if the catch-up failed concurrently, fail this ack rather than hang.
            failure = terminalError.get();
            if (failure != null) {
                ackSink.error(failure);
                return;
            }
            String key = dedupId.apply(payload);
            Item item = new Item(() -> deliver.apply(payload), key, ackSink);
            Sinks.EmitResult result = liveSink.tryEmitNext(item);
            if (result.isFailure()) {
                ackSink.error(new IllegalStateException(HandoverMessages.bufferOverflow(maxBufferedEvents, result)));
            }
        });
    }

    /**
     * Run the one-time catch-up: replay the source's history, record the completion marker, then start delivering the
     * live feed. The returned {@link Mono} completes when the replay and marker are done (see the class javadoc for
     * how that relates to the buffered live payloads).
     */
    public Mono<Void> catchUp(Source<T> source) {
        Objects.requireNonNull(source, "source cannot be null");
        Sinks.One<Void> catchupDone = Sinks.one();

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = source.isAlreadyCaughtUp().cache();
        Flux<Item> replay = alreadyDone
                .flatMapMany(done -> done
                        ? Flux.empty()
                        : source.replay().map(this::replayedItem));
        Flux<Item> markerThenLive = Flux.concat(
                alreadyDone.flatMap(done -> done ? Mono.<Void>empty() : source.markCaughtUp()).thenMany(Flux.<Item>empty()),
                Mono.<Item>fromRunnable(catchupDone::tryEmitEmpty),
                liveSink.asFlux());

        Flux.concat(replay, markerThenLive)
                .concatMap(this::deliver)
                .subscribe(ignored -> {
                }, error -> {
                    // A catch-up-phase failure terminates the pipeline before the buffered live payloads are drained.
                    // Fail their acks and reject later ones, so the caller sees the error instead of hanging.
                    terminalError.set(error);
                    catchupDone.tryEmitError(error);
                    pendingLiveAcks.forEach(sink -> sink.error(error));
                });

        return catchupDone.asMono();
    }

    // Serialized by concatMap, so the de-dup cache is touched by one thread at a time and needs no synchronization.
    private Mono<Void> deliver(Item item) {
        MonoSink<Void> ack = item.ack();
        if (ack != null) {
            if (deliveredIds.contains(item.dedupKey())) {
                ack.success();
                return Mono.empty();
            }
            // Mono.defer so a synchronous throw from the fold becomes an onError signal onErrorResume can catch, rather
            // than aborting the whole pipeline.
            return Mono.defer(item.deliver())
                    .doOnSuccess(v -> {
                        deliveredIds.add(item.dedupKey());
                        ack.success();
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
        return new Item(() -> deliver.apply(replayed), dedupId.apply(replayed), null);
    }

    // A replayed payload has a null ack; a live payload carries the MonoSink whose completion lets the caller
    // acknowledge. The deliver supplier is bound at creation time, so Item needs no type parameter of its own.
    private record Item(Supplier<Mono<Void>> deliver, String dedupKey, @Nullable MonoSink<Void> ack) {
    }
}
