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
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.handover.HandoverMessages;
import org.occurrent.subscription.handover.HandoverOptions;
import org.occurrent.subscription.handover.reactor.ReactiveHandover;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
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
 * <p>
 * The catch-up-then-live coordination itself (the bounded live sink, the de-dup cache, and the
 * replay-then-marker-then-live pipeline shape) is delegated to {@link ReactiveHandover}, shared with
 * {@code CatchupThenPushSubscriptionModel}.
 */
@NullMarked
public final class CatchupProjectionFeed<E> {

    public static final int DEFAULT_DEDUP_CACHE_SIZE = HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE;
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = HandoverOptions.DEFAULT_MAX_BUFFERED_EVENTS;

    private final BiFunction<EventMetadata, E, Mono<Void>> fold;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final String id;

    private final ReactiveHandover<E, ReplayedEvent<E>> handover;

    private CatchupProjectionFeed(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.id = id;
        this.fold = fold;
        this.replayFilter = replayFilter;
        this.reader = reader;
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.converter = converter;
        this.eventId = eventId;
        this.catchupMarker = catchupMarker;
        this.handover = ReactiveHandover.create(
                event -> fold.apply(EventMetadata.empty(), event), this::eventKey,
                replayed -> fold.apply(replayed.metadata(), replayed.event()), replayed -> eventKey(replayed.event()),
                new HandoverOptions(dedupCacheSize, maxBufferedEvents));
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
        return handover.accept(event);
    }

    /**
     * Run the one-time catch-up: replay the projection's history from the store (decoding each event once), record the
     * completion marker, then start delivering the live feed. The returned {@link Mono} completes when the replay and
     * marker are done. Call once, after wiring the live feed.
     *
     * @return A {@link Mono} that completes when the catch-up replay has finished and the feed has gone live.
     */
    public Mono<Void> catchUp() {
        return handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return CatchupProjectionFeed.this.alreadyCaughtUp();
            }

            @Override
            public Flux<ReplayedEvent<E>> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())
                        .map(CatchupProjectionFeed.this::replayedItem);
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return CatchupProjectionFeed.this.markCaughtUp();
            }
        });
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

    private ReplayedEvent<E> replayedItem(CloudEvent cloudEvent) {
        return new ReplayedEvent<>(EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent));
    }

    // A replayed event carries the metadata decoded from its CloudEvent; a live event (delivered via accept(E)) has
    // none, so the two deliveries fold through the same BiFunction with EventMetadata.empty() for the live case.
    private record ReplayedEvent<E>(EventMetadata metadata, E event) {
    }
}
