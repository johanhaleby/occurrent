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
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * The reactor counterpart of the blocking {@code DomainEventFeed}: a register-only sink the application owns and feeds
 * with <strong>domain events</strong>, fanning each one out to every registered projection, with a per-projection
 * catch-up. It lets one external feed drive several projections without any CloudEvent conversion on the live
 * path. See the blocking {@code DomainEventFeed} for the full contract.
 */
@NullMarked
public final class DomainEventFeed<E> {

    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CopyOnWriteArrayList<CatchupProjectionFeed<E>> feeds = new CopyOnWriteArrayList<>();

    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter,
                           Function<E, String> eventId, @Nullable CheckpointStorage catchupMarker) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.converter = Objects.requireNonNull(converter, "converter cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.catchupMarker = catchupMarker;
    }

    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId) {
        this(reader, converter, eventId, null);
    }

    /**
     * Register a projection to be fed and caught up by this feed, materializing into the blocking {@code repository}
     * (folded on {@code boundedElastic}).
     */
    public <S extends @Nullable Object, ID> void register(String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        CatchupProjectionFeed<E> feed = CatchupProjectionFeed.create(id, projection, repository, reader, converter, eventId, catchupMarker);
        feeds.add(feed);
    }

    /**
     * Register a projection driving an existing reactive {@code fold} (for example
     * {@code Projections.reactiveUpdate(materializedView)}), replaying stored events matching {@code replayFilter}. The
     * reactor analog of the blocking {@code register(id, MaterializedView, Filter)}.
     */
    public void register(String id, Function<E, Mono<Void>> fold, Filter replayFilter) {
        feeds.add(CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker));
    }

    /**
     * Feed a live domain event to every registered projection, sequentially. The returned {@link Mono} completes once
     * every projection has handled it, so the listener can acknowledge after processing.
     */
    public Mono<Void> accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        // Avoid allocating a Flux pipeline per live event for the common single-projection feed.
        return switch (feeds.size()) {
            case 0 -> Mono.empty();
            case 1 -> feeds.get(0).accept(event);
            default -> Flux.fromIterable(feeds).concatMap(feed -> feed.accept(event)).then();
        };
    }

    /**
     * Run the one-time catch-up of every registered projection. The returned {@link Mono} completes once every
     * projection has caught up and gone live.
     */
    public Mono<Void> catchUpAll() {
        return Flux.fromIterable(feeds).concatMap(CatchupProjectionFeed::catchUp).then();
    }
}
