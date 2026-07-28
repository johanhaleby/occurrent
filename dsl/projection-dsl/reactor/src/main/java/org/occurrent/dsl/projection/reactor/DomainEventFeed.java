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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.BiFunction;

/**
 * The reactor counterpart of the blocking {@code DomainEventFeed}: a register-only sink the application owns and feeds
 * with <strong>domain events</strong>, fanning each one out to every registered projection, with a per-projection
 * catch-up. It lets one external feed drive several projections without any CloudEvent conversion on the live
 * path. See the blocking {@code DomainEventFeed} for the full contract.
 * <p>
 * The {@code occurrent.subscription.catchup-then-live.*} properties do <strong>not</strong> reach this feed. Your
 * application declares this bean, so tune its catch-up by passing {@link CatchupThenLiveOptions} to the constructor.
 */
@NullMarked
public final class DomainEventFeed<E> {

    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;
    private final CopyOnWriteArrayList<CatchupProjectionFeed<E>> feeds = new CopyOnWriteArrayList<>();
    private final Set<String> registeredIds = ConcurrentHashMap.newKeySet();

    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter,
                           Function<E, String> eventId, @Nullable CheckpointStorage catchupMarker) {
        this(reader, converter, eventId, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * As {@link #DomainEventFeed(PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)}, with
     * explicit handover {@code options} applied to every projection registered on this feed.
     */
    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter,
                           Function<E, String> eventId, @Nullable CheckpointStorage catchupMarker,
                           CatchupThenLiveOptions options) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.converter = Objects.requireNonNull(converter, "converter cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId) {
        this(reader, converter, eventId, null);
    }

    /**
     * Register a projection to be fed and caught up by this feed, materializing into the blocking {@code repository}
     * (folded on {@code boundedElastic}).
     */
    public <S extends @Nullable Object, ID> void register(String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        // Fail fast on the common duplicate-id case before building a feed. registeredIds.add(id) after creation stays
        // the authoritative, race-safe check: this is only an optimization, not a substitute for it.
        if (registeredIds.contains(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        CatchupProjectionFeed<E> feed = CatchupProjectionFeed.create(id, projection, repository, reader, converter, eventId, catchupMarker, options);
        // Reserve the id only once the feed exists, so a failed registration (an invalid reader, for example) never
        // permanently burns the id. Each id must be unique because it is the durable checkpoint key.
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        feeds.add(feed);
    }

    /**
     * Register a projection driving an existing reactive {@code fold} (for example
     * {@code Projections.reactiveUpdate(materializedView)}), replaying stored events matching {@code replayFilter}. The
     * reactor analog of the blocking {@code register(id, MaterializedView, Filter)}.
     */
    public void register(String id, Function<E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        // Fail fast on the common duplicate-id case before building a feed. registeredIds.add(id) after creation stays
        // the authoritative, race-safe check: this is only an optimization, not a substitute for it.
        if (registeredIds.contains(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        CatchupProjectionFeed<E> feed = CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options);
        // Reserve the id only once the feed exists, so a failed registration never permanently burns the id.
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        feeds.add(feed);
    }

    /**
     * Register a projection driving a metadata-aware reactive {@code fold}, the form that can key or fold on the event's
     * {@link EventMetadata}. The replay always supplies the metadata it decoded from the CloudEvent, and the live path
     * supplies whatever the source passed to {@link #accept(EventMetadata, Object)}.
     */
    public void register(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        // Fail fast on the common duplicate-id case before building a feed. registeredIds.add(id) after creation stays
        // the authoritative, race-safe check: this is only an optimization, not a substitute for it.
        if (registeredIds.contains(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        CatchupProjectionFeed<E> feed = CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options);
        // Reserve the id only once the feed exists, so a failed registration never permanently burns the id.
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        feeds.add(feed);
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
     * Feed a live domain event to every registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with {@link EventMetadata#empty()}.
     */
    public Mono<Void> accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return switch (feeds.size()) {
            case 0 -> Mono.empty();
            case 1 -> feeds.get(0).accept(metadata, event);
            default -> Flux.fromIterable(feeds).concatMap(feed -> feed.accept(metadata, event)).then();
        };
    }

    /**
     * Run the one-time catch-up of every registered projection. The returned {@link Mono} completes once every
     * projection has caught up and gone live.
     * <p>
     * An error on the returned {@link Mono} is terminal for the whole feed, so let it reach the caller and do not
     * start the application. The projection that failed rejects every later event, and because the projections are
     * fed in registration order, one that failed early blocks the ones behind it. Unlike a subscription model, the
     * feed does not drop the failed projection: the application asked for it, so running on without it is worse than
     * not running. Fix the cause and build a new feed.
     */
    public Mono<Void> catchUpAll() {
        return Flux.fromIterable(feeds).concatMap(CatchupProjectionFeed::catchUp).then();
    }
}
