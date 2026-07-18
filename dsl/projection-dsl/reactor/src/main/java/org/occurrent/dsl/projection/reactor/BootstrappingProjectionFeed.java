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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
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
import java.util.function.Function;

/**
 * The reactor counterpart of the blocking {@code BootstrappingProjectionFeed}: feeds a projection with
 * <strong>domain events</strong> and gives it a one-time <strong>bootstrap catch-up</strong>, without the double
 * encode/decode of routing domain events through the CloudEvent push feed.
 * <p>
 * The live path is conversion-free: {@link #accept(Object)} folds a domain event straight into the read model. Only the
 * bootstrap reads the event store (CloudEvents) and decodes each replayed event once with the {@link CloudEventConverter}.
 * The replay, a bootstrap-complete marker step, and the live feed are composed into one ordered pipeline with
 * {@link Flux#concat}: the replay is consumed first, then the marker is recorded, then live events buffered in a bounded
 * unicast sink during the replay flow through, de-duplicated by an event-id extracted from the <em>domain</em> event.
 * Because the pipeline is serialized by {@code concatMap}, the de-dup cache needs no locking.
 * <p>
 * Contract (see ADR 62): bootstrap is Occurrent's job, live-resume is the broker's. A live event's {@code accept}
 * {@link Mono} completes only after its handler runs, so the listener can acknowledge after processing. Delivery is
 * at-least-once, so the fold must be idempotent. The buffer is bounded and fails loud on overflow.
 */
@NullMarked
public final class BootstrappingProjectionFeed<E> {

    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    private final Function<E, Mono<Void>> fold;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage bootstrapMarker;
    private final String id;
    private final BoundedIdCache deliveredIds;
    private final Sinks.Many<Item<E>> liveSink;
    // Acks of live events buffered but not yet folded, so a bootstrap failure fails them rather than leaving the
    // listener's accept Monos hanging forever.
    private final Set<MonoSink<Void>> pendingLiveAcks = ConcurrentHashMap.newKeySet();
    private final AtomicReference<Throwable> terminalError = new AtomicReference<>();

    private BootstrappingProjectionFeed(String id, Function<E, Mono<Void>> fold, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.id = id;
        this.fold = fold;
        this.replayFilter = replayFilter;
        this.reader = reader;
        this.converter = converter;
        this.eventId = eventId;
        this.bootstrapMarker = bootstrapMarker;
        this.deliveredIds = new BoundedIdCache(dedupCacheSize);
        this.liveSink = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(maxBufferedEvents));
    }

    /**
     * Create a feed materializing {@code projection} into the blocking {@code repository} (folded on
     * {@code boundedElastic}). See the blocking {@code BootstrappingProjectionFeed} for the parameter contract.
     */
    public static <S extends @Nullable Object, E, ID> BootstrappingProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage bootstrapMarker) {
        return create(id, projection, repository, reader, converter, eventId, bootstrapMarker,
                DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    /**
     * As {@link #create(String, Projection, ViewStateRepository, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with an explicit de-dup cache size and live-buffer cap.
     */
    public static <S extends @Nullable Object, E, ID> BootstrappingProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        Objects.requireNonNull(reader, "reader cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(eventId, "eventId cannot be null");
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
        Function<E, Mono<Void>> fold = Projections.reactiveUpdate(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return new BootstrappingProjectionFeed<>(id, fold, filter, reader, converter, eventId, bootstrapMarker, dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Feed a live domain event. The returned {@link Mono} completes once the event has been folded (or immediately if it
     * is a de-duplicated overlap), so the listener can acknowledge after processing. Events fed before or during the
     * bootstrap are buffered and delivered after the replay.
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
            // Re-check after registering: if the bootstrap failed concurrently, fail this ack rather than hang.
            failure = terminalError.get();
            if (failure != null) {
                ackSink.error(failure);
                return;
            }
            Sinks.EmitResult result = liveSink.tryEmitNext(new Item<>(event, ackSink));
            if (result.isFailure()) {
                ackSink.error(new IllegalStateException("Live event buffer overflowed during bootstrap replay. "
                        + "The history is too large to buffer the live feed across a full replay. Rebuild offline from "
                        + "the event store instead of bootstrapping over a live feed. Emit result: " + result));
            }
        });
    }

    /**
     * Run the one-time bootstrap: replay the projection's history from the store (decoding each event once), record the
     * completion marker, then start delivering the live feed. The returned {@link Mono} completes when the replay and
     * marker are done. Call once, after wiring the live feed.
     *
     * @return A {@link Mono} that completes when the bootstrap replay has finished and the feed has gone live.
     */
    public Mono<Void> bootstrap() {
        Sinks.One<Void> bootstrapDone = Sinks.one();

        // Evaluate the marker once and reuse it, so the replay and the "record marker" step agree, and the marker is
        // written only when the replay actually ran (not on a restart that skips it).
        Mono<Boolean> alreadyDone = alreadyBootstrapped().cache();
        Flux<Item<E>> replay = alreadyDone
                .flatMapMany(done -> done
                        ? Flux.empty()
                        : reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())
                        .map(converter::toDomainEvent).map(this::replayedItem));
        Flux<Item<E>> markerThenLive = Flux.concat(
                alreadyDone.flatMap(done -> done ? Mono.<Void>empty() : markBootstrapped()).thenMany(Flux.<Item<E>>empty()),
                Mono.<Item<E>>fromRunnable(bootstrapDone::tryEmitEmpty),
                liveSink.asFlux());

        Flux.concat(replay, markerThenLive)
                .concatMap(this::deliver)
                .subscribe(ignored -> {
                }, error -> {
                    // A bootstrap-phase failure terminates the pipeline before the buffered live events are drained.
                    // Fail their acks and reject later ones, so the listener sees the error instead of hanging.
                    terminalError.set(error);
                    bootstrapDone.tryEmitError(error);
                    pendingLiveAcks.forEach(sink -> sink.error(error));
                });

        return bootstrapDone.asMono();
    }

    // Serialized by concatMap, so the de-dup cache is touched by one thread at a time and needs no synchronization.
    private Mono<Void> deliver(Item<E> item) {
        E event = item.event();
        String key = eventId.apply(event);
        MonoSink<Void> ack = item.ack();
        if (ack != null) {
            if (deliveredIds.contains(key)) {
                ack.success();
                return Mono.empty();
            }
            // Mono.defer so a synchronous throw from the fold becomes an onError signal onErrorResume can catch, rather
            // than aborting the whole pipeline.
            return Mono.defer(() -> fold.apply(event))
                    .doOnSuccess(v -> {
                        deliveredIds.add(key);
                        ack.success();
                    })
                    .onErrorResume(error -> {
                        ack.error(error);
                        return Mono.empty();
                    });
        }
        return Mono.defer(() -> fold.apply(event)).doOnSuccess(v -> deliveredIds.add(key));
    }

    private Mono<Boolean> alreadyBootstrapped() {
        return bootstrapMarker == null ? Mono.just(false) : bootstrapMarker.read(id).hasElement();
    }

    private Mono<Void> markBootstrapped() {
        if (bootstrapMarker == null) {
            return Mono.empty();
        }
        return reader.currentPosition()
                .flatMap(head -> bootstrapMarker.save(id, GlobalCheckpoint.of(head)))
                .then();
    }

    private Item<E> replayedItem(E event) {
        return new Item<>(event, null);
    }

    // A replayed event has a null ack; a live event carries the MonoSink whose completion lets the listener acknowledge.
    private record Item<E>(E event, @Nullable MonoSink<Void> ack) {
    }
}
