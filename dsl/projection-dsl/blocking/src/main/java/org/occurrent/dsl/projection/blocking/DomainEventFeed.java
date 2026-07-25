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

package org.occurrent.dsl.projection.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * The domain-event twin of {@code PushSubscriptionModel}: a register-only sink the application owns and feeds with
 * <strong>domain events</strong>, fanning each one out to every registered projection, with a per-projection catch-up.
 * It lets one external feed (a RabbitMQ or Kafka listener with its own message converter) drive several
 * projections without any CloudEvent conversion on the live path.
 * <p>
 * The application declares it as a bean carrying the domain-specific {@code eventId} function (the catch-up de-dup key)
 * plus the CloudEvent-layer collaborators (the store {@link PositionOrderedReader}, the {@link CloudEventConverter} used
 * only to decode replayed history, and an optional {@link CheckpointStorage} catch-up marker), registers projections on
 * it (directly, or through {@code @Projection(source = PUSH)}), and feeds each received domain event to
 * {@link #accept(Object)} from its listener. Each registration is a {@link CatchupProjectionFeed}, so the
 * contract, live-resume owned by the broker, at-least-once idempotent folds, bounded buffering, is per projection.
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

    /**
     * @param reader          The store read used to replay history during each projection's catch-up.
     * @param converter       Decodes replayed CloudEvents to domain events (replay only, never the live path).
     * @param eventId         Extracts a stable id from a domain event, the replay-to-live de-dup key, shared by every
     *                        projection on this feed.
     * @param catchupMarker Records per-projection catch-up completion so a restart skips the replay, or {@code null}
     *                        to always catch up.
     */
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
     * Register a projection to be fed and caught up by this feed, materializing into {@code repository}.
     */
    public <S extends @Nullable Object, ID> void register(String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        MaterializedView<E> view = Projections.materializedView(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        register(id, view, filter);
    }

    /**
     * Register a projection driving an existing {@link MaterializedView}, replaying stored events matching
     * {@code replayFilter}.
     */
    public void register(String id, MaterializedView<E> view, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        // Fail fast on the common duplicate-id case before building a feed. registeredIds.add(id) after creation stays
        // the authoritative, race-safe check: this is only an optimization, not a substitute for it.
        if (registeredIds.contains(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        CatchupProjectionFeed<E> feed = CatchupProjectionFeed.create(id, view, replayFilter, reader, converter, eventId, catchupMarker, options);
        // Reserve the id only once the feed exists, so a failed registration never permanently burns the id.
        if (!registeredIds.add(id)) {
            throw new IllegalArgumentException("A projection with id '" + id + "' is already registered on this feed");
        }
        feeds.add(feed);
    }

    /**
     * Feed a live domain event to every registered projection, on the calling thread. Call this from the broker
     * listener, acknowledging the message only once it returns. An exception from any projection propagates.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        for (CatchupProjectionFeed<E> feed : feeds) {
            feed.accept(event);
        }
    }

    /**
     * Feed a live domain event to every registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with no metadata.
     */
    public void accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        for (CatchupProjectionFeed<E> feed : feeds) {
            feed.accept(metadata, event);
        }
    }

    /**
     * Run the one-time catch-up of every registered projection (replay history, then go live). Call once, after all
     * projections are registered and the live feed is wired.
     */
    public void catchUpAll() {
        for (CatchupProjectionFeed<E> feed : feeds) {
            feed.catchUp();
        }
    }
}
