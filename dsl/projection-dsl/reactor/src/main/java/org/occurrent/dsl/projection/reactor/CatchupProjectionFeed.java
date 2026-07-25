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

package org.occurrent.dsl.projection.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.BoundedIdCache;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The reactor counterpart of the blocking {@code CatchupProjectionFeed}: feeds a projection with
 * <strong>domain events</strong> and gives it a one-time <strong>catch-up</strong>, without the double
 * encode/decode of routing domain events through the CloudEvent push feed.
 * <p>
 * The live path is conversion-free: {@link #accept(Object)} folds a domain event straight into the read model. Only the
 * catch-up reads the event store (CloudEvents) and decodes each replayed event once with the {@link CloudEventConverter}.
 * The replay, a catch-up-complete marker step, and the live feed are composed into one ordered pipeline with
 * {@link Flux#concat}: the replay is consumed first, then the marker is recorded, then live events buffered in a bounded
 * unicast sink during the replay flow through, de-duplicated by an event-id extracted from the <em>domain</em> event.
 * Because the pipeline is serialized by {@code concatMap}, the de-dup cache needs no locking.
 * <p>
 * Contract (see ADR 62): catch-up is Occurrent's job, live-resume is the broker's. A live event's {@code accept}
 * {@link Mono} completes only after its handler runs, so the listener can acknowledge after processing. Delivery is
 * at-least-once, so the fold must be idempotent. The buffer is bounded and fails loud on overflow.
 * <p>
 * The replay decodes CloudEvents, so {@link EventMetadata} is available there and is folded with the event. A live
 * domain event arrives with no CloudEvent behind it and is folded with {@link EventMetadata#empty()}. A projection
 * keyed by metadata therefore resolves its instance from the metadata during the replay and from the event alone once
 * live, the same split as the blocking {@code CatchupProjectionFeed}.
 */
@NullMarked
public final class CatchupProjectionFeed<E> {

    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    private final BiFunction<EventMetadata, E, Mono<Void>> fold;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final String id;
    private final int maxBufferedEvents;
    private final BoundedIdCache deliveredIds;
    private final Sinks.Many<Item<E>> liveSink;
    // Acks of live events buffered but not yet folded, so a catch-up failure fails them rather than leaving the
    // listener's accept Monos hanging forever.
    private final Set<MonoSink<Void>> pendingLiveAcks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Throwable> terminalError = new AtomicReference<>();

    private CatchupProjectionFeed(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.id = id;
        this.fold = fold;
        this.replayFilter = replayFilter;
        this.reader = reader;
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException("The reader does not write positions (writesPosition() returns false), so the catch-up cannot replay history in position order. Supply a reader from a positioned event store.");
        }
        this.converter = converter;
        this.eventId = eventId;
        this.catchupMarker = catchupMarker;
        this.maxBufferedEvents = maxBufferedEvents;
        this.deliveredIds = new BoundedIdCache(dedupCacheSize);
        this.liveSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(maxBufferedEvents));
    }

    /**
     * Create a feed materializing {@code projection} into the blocking {@code repository} (folded on
     * {@code boundedElastic}). See the blocking {@code CatchupProjectionFeed} for the parameter contract.
     */
    public static <S extends @Nullable Object, E, ID> CatchupProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        return create(id, projection, repository, reader, converter, eventId, catchupMarker,
                DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    /**
     * As {@link #create(String, Projection, ViewStateRepository, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with an explicit de-dup cache size and live-buffer cap.
     */
    public static <S extends @Nullable Object, E, ID> CatchupProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        // The metadata-aware fold, so a projection keyed by metadata (a stream id, say) resolves the same instance during
        // the replay as it does live. reactiveUpdate(...) would hardwire EventMetadata.empty() and mis-key every
        // replayed event.
        BiFunction<EventMetadata, E, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return create(id, fold, filter, reader, converter, eventId, catchupMarker, dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Create a feed driving an existing reactive {@code fold}, replaying stored events matching {@code replayFilter}.
     * The reactor analog of the blocking {@code create(id, MaterializedView, Filter, ...)}: the caller supplies the fold
     * (for example {@code Projections.reactiveUpdate(materializedView)}) and the filter that selects the events to replay.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, Function<E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        return create(id, fold, replayFilter, reader, converter, eventId, catchupMarker,
                DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    /**
     * As {@link #create(String, Function, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with an explicit de-dup cache size and live-buffer cap.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, Function<E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(fold, "fold cannot be null");
        // A caller-supplied one-argument fold has no metadata channel, so the replay drops the metadata it decoded.
        return create(id, (metadata, event) -> fold.apply(event), replayFilter, reader, converter, eventId, catchupMarker, dedupCacheSize, maxBufferedEvents);
    }

    private static <E> CatchupProjectionFeed<E> create(
            String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(fold, "fold cannot be null");
        Objects.requireNonNull(replayFilter, "replayFilter cannot be null");
        Objects.requireNonNull(reader, "reader cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(eventId, "eventId cannot be null");
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
        return new CatchupProjectionFeed<>(id, fold, replayFilter, reader, converter, eventId, catchupMarker, dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Feed a live domain event. The returned {@link Mono} completes once the event has been folded (or immediately if it
     * is a de-duplicated overlap), so the listener can acknowledge after processing. Events fed before or during the
     * catch-up are buffered and delivered after the replay.
     *
     * @param event The domain event received from the external source.
     * @return A {@link Mono} that completes when the event has been handled.
     */
    public Mono<Void> accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
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
            Sinks.EmitResult result = liveSink.tryEmitNext(new Item<>(EventMetadata.empty(), event, ackSink));
            if (result.isFailure()) {
                ackSink.error(new IllegalStateException("Live event buffer overflowed during catch-up replay (cap "
                        + maxBufferedEvents + "). "
                        + "The history is too large to buffer the live feed across a full replay. Rebuild offline from "
                        + "the event store instead of catching up over a live feed. Emit result: " + result));
            }
        });
    }

    /**
     * Run the one-time catch-up: replay the projection's history from the store (decoding each event once), record the
     * completion marker, then start delivering the live feed. The returned {@link Mono} completes when the replay and
     * marker are done. Call once, after wiring the live feed.
     *
     * @return A {@link Mono} that completes when the catch-up replay has finished and the feed has gone live.
     */
    public Mono<Void> catchUp() {
        Sinks.One<Void> catchupDone = Sinks.one();

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = alreadyCaughtUp().cache();
        Flux<Item<E>> replay = alreadyDone
                .flatMapMany(done -> done
                        ? Flux.empty()
                        : reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())
                        .map(this::replayedItem));
        Flux<Item<E>> markerThenLive = Flux.concat(
                alreadyDone.flatMap(done -> done ? Mono.<Void>empty() : markCaughtUp()).thenMany(Flux.<Item<E>>empty()),
                Mono.<Item<E>>fromRunnable(catchupDone::tryEmitEmpty),
                liveSink.asFlux());

        Flux.concat(replay, markerThenLive)
                .concatMap(this::deliver)
                .subscribe(ignored -> {
                }, error -> {
                    // A catch-up-phase failure terminates the pipeline before the buffered live events are drained.
                    // Fail their acks and reject later ones, so the listener sees the error instead of hanging.
                    terminalError.set(error);
                    catchupDone.tryEmitError(error);
                    pendingLiveAcks.forEach(sink -> sink.error(error));
                });

        return catchupDone.asMono();
    }

    // Serialized by concatMap, so the de-dup cache is touched by one thread at a time and needs no synchronization.
    private Mono<Void> deliver(Item<E> item) {
        E event = item.event();
        String key = eventKey(event);
        MonoSink<Void> ack = item.ack();
        if (ack != null) {
            if (deliveredIds.contains(key)) {
                ack.success();
                return Mono.empty();
            }
            // Mono.defer so a synchronous throw from the fold becomes an onError signal onErrorResume can catch, rather
            // than aborting the whole pipeline.
            return Mono.defer(() -> fold.apply(item.metadata(), event))
                    .doOnSuccess(v -> {
                        deliveredIds.add(key);
                        ack.success();
                    })
                    .onErrorResume(error -> {
                        ack.error(error);
                        return Mono.empty();
                    });
        }
        return Mono.defer(() -> fold.apply(item.metadata(), event)).doOnSuccess(v -> deliveredIds.add(key));
    }

    // A null id would collapse every such event to one de-dup key and silently drop deliveries, so fail loud instead.
    private String eventKey(E event) {
        return Objects.requireNonNull(eventId.apply(event), "The eventId function returned null; every domain event must have a stable non-null id for de-duplication.");
    }

    private Mono<Boolean> alreadyCaughtUp() {
        return catchupMarker == null ? Mono.just(false) : catchupMarker.read(id).hasElement();
    }

    private Mono<Void> markCaughtUp() {
        if (catchupMarker == null) {
            return Mono.empty();
        }
        return reader.currentPosition()
                .flatMap(head -> catchupMarker.save(id, GlobalCheckpoint.of(head)))
                .then();
    }

    private Item<E> replayedItem(CloudEvent cloudEvent) {
        return new Item<>(EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent), null);
    }

    // A replayed event has a null ack; a live event carries the MonoSink whose completion lets the listener acknowledge.
    private record Item<E>(EventMetadata metadata, E event, @Nullable MonoSink<Void> ack) {
    }
}
