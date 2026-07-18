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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Feeds a projection with <strong>domain events</strong> and gives it a one-time <strong>bootstrap catch-up</strong>,
 * without the double encode/decode of routing domain events through the CloudEvent push feed.
 * <p>
 * The live path is conversion-free: {@link #accept(Object)} folds a domain event straight into the view. Only the
 * bootstrap reads the event store, which holds CloudEvents, so the replay decodes each once with the
 * {@link CloudEventConverter}. On {@link #bootstrap()} it registers the live feed first (buffering), replays the
 * projection's history from the store in position order, folds each replayed event, then drains the buffer and goes
 * live, de-duplicating the replay-to-live overlap by an event-id extracted from the <em>domain</em> event (so the
 * de-dup does not depend on the CloudEvent id). A one-shot marker (an optional {@link CheckpointStorage}) makes a
 * restart skip the replay.
 * <p>
 * Contract (see ADR 62): bootstrap is Occurrent's job, live-resume is the broker's (acknowledge after {@code accept}
 * returns). No live position watermark is kept, so delivery is at-least-once and the fold must be idempotent. The
 * de-dup cache and the live buffer are bounded; the buffer fails loud on overflow.
 */
@NullMarked
public final class BootstrappingProjectionFeed<E> {

    /** Recently folded event ids retained to de-duplicate the replay-to-live overlap. */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;
    /** Cap on events buffered from the live feed during the bootstrap replay before failing loud. */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    private final MaterializedView<E> view;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage bootstrapMarker;
    private final String id;
    private final int maxBufferedEvents;

    private final Object lock = new Object();
    private final Queue<E> buffer = new ArrayDeque<>();
    private final BoundedIdCache deliveredIds;
    private boolean live = false;

    private BootstrappingProjectionFeed(String id, MaterializedView<E> view, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.id = id;
        this.view = view;
        this.replayFilter = replayFilter;
        this.reader = reader;
        this.converter = converter;
        this.eventId = eventId;
        this.bootstrapMarker = bootstrapMarker;
        this.maxBufferedEvents = maxBufferedEvents;
        this.deliveredIds = new BoundedIdCache(dedupCacheSize);
    }

    /**
     * Create a feed materializing {@code projection} into {@code repository}.
     *
     * @param id              The projection's id, used as the single-instance key and the bootstrap-marker key.
     * @param projection      The projection to feed.
     * @param repository      The read-model store.
     * @param reader          Reads the projection's history in position order for the bootstrap replay.
     * @param converter       Decodes replayed CloudEvents to domain events (replay only, never the live path).
     * @param eventId         Extracts a stable id from a domain event, the replay-to-live de-dup key.
     * @param bootstrapMarker Records that the bootstrap finished so a restart skips it, or {@code null} to always
     *                        bootstrap.
     */
    public static <S extends @Nullable Object, E, ID> BootstrappingProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage bootstrapMarker) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
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
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        MaterializedView<E> view = Projections.materializedView(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return create(id, view, filter, reader, converter, eventId, bootstrapMarker, dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Create a feed driving an existing {@link MaterializedView} (for example one built with the view DSL's
     * {@code materialized(...)} with its own retry/locking policy). {@code replayFilter} selects which stored events to
     * replay; use {@link Filter#all()} to replay everything and let the fold ignore unhandled types.
     */
    public static <E> BootstrappingProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage bootstrapMarker) {
        return create(id, view, replayFilter, reader, converter, eventId, bootstrapMarker,
                DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    /**
     * As {@link #create(String, MaterializedView, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with an explicit de-dup cache size and live-buffer cap.
     */
    public static <E> BootstrappingProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(view, "view cannot be null");
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
        return new BootstrappingProjectionFeed<>(id, view, replayFilter, reader, converter, eventId, bootstrapMarker,
                dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Feed a live domain event. Buffered while the bootstrap replay runs, folded directly afterwards, on the calling
     * thread. Call this from the broker listener, acknowledging the message only once it returns.
     *
     * @param event The domain event received from the external source.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        synchronized (lock) {
            if (live) {
                deliverLive(event);
                return;
            }
            if (buffer.size() >= maxBufferedEvents) {
                throw new IllegalStateException("Live event buffer overflowed during bootstrap replay (cap "
                        + maxBufferedEvents + "). The history is too large to buffer the live feed across a full replay. "
                        + "Rebuild offline from the event store instead of bootstrapping over a live feed.");
            }
            buffer.add(event);
        }
    }

    /**
     * Run the one-time bootstrap: replay the projection's history from the store (decoding each event once), then drain
     * the buffered live events and go live. Skipped if the bootstrap marker already records completion. Call this once,
     * after wiring the live feed, so events arriving during the replay are captured.
     */
    public void bootstrap() {
        if (isAlreadyBootstrapped()) {
            drainBufferAndGoLive();
            return;
        }
        try (Stream<CloudEvent> history = reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())) {
            history.forEach(cloudEvent -> {
                E event = converter.toDomainEvent(cloudEvent);
                view.update(event);
                synchronized (lock) {
                    deliveredIds.add(eventId.apply(event));
                }
            });
        }
        drainBufferAndGoLive();
        markBootstrapped();
    }

    private void drainBufferAndGoLive() {
        synchronized (lock) {
            for (E buffered : buffer) {
                deliverLive(buffered);
            }
            buffer.clear();
            live = true;
        }
    }

    // Must be called holding lock. Folds unless the event was already folded by the replay or an earlier live copy.
    private void deliverLive(E event) {
        String key = eventId.apply(event);
        if (deliveredIds.contains(key)) {
            return;
        }
        view.update(event);
        deliveredIds.add(key);
    }

    private boolean isAlreadyBootstrapped() {
        return bootstrapMarker != null && bootstrapMarker.exists(id);
    }

    private void markBootstrapped() {
        if (bootstrapMarker != null) {
            // The stored position marks that the bootstrap replay completed, not a live resume watermark.
            bootstrapMarker.save(id, GlobalCheckpoint.of(reader.currentPosition()));
        }
    }

    private static final class BoundedIdCache {
        private final int maxSize;
        private final Set<String> ids;
        private final Queue<String> order;

        private BoundedIdCache(int maxSize) {
            this.maxSize = maxSize;
            this.ids = new HashSet<>(Math.min(maxSize, 1024));
            this.order = new ArrayDeque<>();
        }

        boolean contains(String id) {
            return ids.contains(id);
        }

        void add(String id) {
            if (ids.add(id)) {
                order.add(id);
                if (order.size() > maxSize) {
                    ids.remove(order.poll());
                }
            }
        }
    }
}
