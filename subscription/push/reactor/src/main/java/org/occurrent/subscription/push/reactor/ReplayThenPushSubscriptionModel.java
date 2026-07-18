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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.internal.BoundedIdCache;
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

/**
 * The reactive counterpart of the blocking {@code ReplayThenPushSubscriptionModel}: a one-time <strong>bootstrap
 * catch-up</strong> in front of a reactive {@link PushSubscriptionModel}. On first subscribe it replays a projection's
 * history from the event store, then hands over to the live push feed, so a brand-new or rebuilt projection is
 * backfilled before it consumes the broker.
 * <p>
 * The replay, a bootstrap-complete marker step, and the live feed are composed into one ordered pipeline with
 * {@link Flux#concat}: the replay is consumed first, then the marker is recorded, then the live feed. Live events that
 * arrive during the replay are buffered in a unicast sink until the pipeline reaches them, so nothing is lost across the
 * seam, and the overlap is de-duplicated by event id. Because the whole pipeline is serialized by {@code concatMap}, the
 * de-dup cache needs no locking.
 * <p>
 * Contract (see ADR 62 and the blocking model): bootstrap is Occurrent's job and runs once per subscription id, guarded
 * by an optional {@link CheckpointStorage} marker so a restart skips it. Live-resume is the broker's job, so no live
 * position watermark is persisted and delivery is at-least-once over idempotent folds. A live event's {@code accept}
 * {@link Mono} completes only once its handler has run (including events buffered during the replay), so the listener
 * can acknowledge after processing. Only stream and capability-agnostic subscription filters can be replayed.
 */
@NullMarked
public class ReplayThenPushSubscriptionModel implements Subscribable {

    /** @see org.occurrent.subscription.push.reactor.ReplayThenPushSubscriptionModel */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage bootstrapMarker;
    private final int dedupCacheSize;
    private final int maxBufferedEvents;

    public ReplayThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage bootstrapMarker) {
        this(reader, liveFeed, bootstrapMarker, DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    public ReplayThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.bootstrapMarker = bootstrapMarker;
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
        this.dedupCacheSize = dedupCacheSize;
        this.maxBufferedEvents = maxBufferedEvents;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = replayFilterFor(filter);

        BoundedIdCache deliveredIds = new BoundedIdCache(dedupCacheSize);
        Sinks.Many<Item> liveSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(maxBufferedEvents));
        Sinks.One<Void> bootstrapDone = Sinks.one();
        // Track the acks of live events buffered but not yet delivered, so a bootstrap failure fails them rather than
        // leaving the listener's accept Monos hanging forever.
        Set<MonoSink<Void>> pendingLiveAcks = ConcurrentHashMap.newKeySet();
        AtomicReference<Throwable> terminalError = new AtomicReference<>();

        // Register on the live feed first, so events committing during the replay are buffered in the sink, not lost.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.create(ackSink -> {
            ackSink.onDispose(() -> pendingLiveAcks.remove(ackSink));
            Throwable failure = terminalError.get();
            if (failure != null) {
                ackSink.error(failure);
                return;
            }
            pendingLiveAcks.add(ackSink);
            // Re-check after registering: if the bootstrap failed concurrently, fail this ack rather than hang.
            failure = terminalError.get();
            if (failure != null) {
                ackSink.error(failure);
                return;
            }
            Sinks.EmitResult result = liveSink.tryEmitNext(new Item(cloudEvent, ackSink));
            if (result.isFailure()) {
                ackSink.error(new IllegalStateException("Live event buffer overflowed during bootstrap replay (cap "
                        + maxBufferedEvents + "). The history is too large to buffer the live feed across a full replay. "
                        + "Rebuild offline from the event store instead of bootstrapping over a live feed. Emit result: " + result));
            }
        }));

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = alreadyBootstrapped(subscriptionId).cache();
        Flux<Item> replay = alreadyDone
                .flatMapMany(done -> done
                        ? Flux.empty()
                        : reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning()).map(Item::replayed));
        Flux<Item> markerThenLive = Flux.concat(
                alreadyDone.flatMap(done -> done ? Mono.<Void>empty() : markBootstrapped(subscriptionId)).thenMany(Flux.<Item>empty()),
                Mono.<Item>fromRunnable(() -> bootstrapDone.tryEmitEmpty()),
                liveSink.asFlux());

        Flux.concat(replay, markerThenLive)
                .concatMap(item -> deliver(item, action, deliveredIds))
                .subscribe(ignored -> {
                }, error -> {
                    // A bootstrap-phase failure terminates the pipeline before the buffered live events are drained.
                    // Fail their acks and reject later ones, so the listener sees the error instead of hanging.
                    terminalError.set(error);
                    bootstrapDone.tryEmitError(error);
                    pendingLiveAcks.forEach(sink -> sink.error(error));
                });

        return new AlreadyStartedSubscription(subscriptionId, bootstrapDone.asMono());
    }

    // Serialized by concatMap, so the de-dup cache is touched by one thread at a time and needs no synchronization.
    private static Mono<Void> deliver(Item item, Function<CloudEvent, Mono<Void>> action, BoundedIdCache deliveredIds) {
        CloudEvent cloudEvent = item.event();
        String id = cloudEvent.getId();
        if (item.ack() != null) {
            // Live event: de-dup against the overlap, then run the handler and complete its accept Mono so the listener
            // can acknowledge only after processing. A handler error is reported to the listener but does not stop the
            // pipeline (the next event is still delivered).
            if (deliveredIds.contains(id)) {
                item.ack().success();
                return Mono.empty();
            }
            // Mono.defer so a synchronous throw from action.apply becomes an onError signal onErrorResume can catch,
            // rather than aborting the whole pipeline.
            return Mono.defer(() -> action.apply(cloudEvent))
                    .doOnSuccess(v -> {
                        deliveredIds.add(id);
                        item.ack().success();
                    })
                    .onErrorResume(error -> {
                        item.ack().error(error);
                        return Mono.empty();
                    });
        }
        // Replay event: an error here propagates and fails the bootstrap.
        return Mono.defer(() -> action.apply(cloudEvent)).doOnSuccess(v -> deliveredIds.add(id));
    }

    private Mono<Boolean> alreadyBootstrapped(String subscriptionId) {
        return bootstrapMarker == null ? Mono.just(false) : bootstrapMarker.read(subscriptionId).hasElement();
    }

    private Mono<Void> markBootstrapped(String subscriptionId) {
        if (bootstrapMarker == null) {
            return Mono.empty();
        }
        // The stored position marks that the bootstrap replay completed at this head, not a live resume watermark.
        return reader.currentPosition()
                .flatMap(head -> bootstrapMarker.save(subscriptionId, GlobalCheckpoint.of(head)))
                .then();
    }

    private static Filter replayFilterFor(@Nullable SubscriptionFilter filter) {
        return switch (filter) {
            case null -> Filter.all();
            case StreamSubscriptionFilter streamSubscriptionFilter -> streamSubscriptionFilter.filter();
            case AgnosticSubscriptionFilter agnosticSubscriptionFilter -> agnosticSubscriptionFilter.filter();
            default ->
                    throw new IllegalArgumentException("Cannot bootstrap-replay a " + filter.getClass().getSimpleName()
                            + ". Only a stream or capability-agnostic subscription filter can be replayed in position order.");
        };
    }

    // A replayed event has a null ack; a live event carries the MonoSink whose completion lets the listener acknowledge.
    private record Item(CloudEvent event, @Nullable MonoSink<Void> ack) {
        static Item replayed(CloudEvent event) {
            return new Item(event, null);
        }
    }

    private record AlreadyStartedSubscription(String id, Mono<Void> started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
