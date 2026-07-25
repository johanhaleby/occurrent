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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.HandoverOptions;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Feeds a projection with <strong>domain events</strong> and gives it a one-time <strong>catch-up</strong>,
 * without the double encode/decode of routing domain events through the CloudEvent push feed.
 * <p>
 * The live path is conversion-free: {@link #accept(Object)} folds a domain event straight into the view. Only the
 * catch-up reads the event store, which holds CloudEvents, so the replay decodes each once with the
 * {@link CloudEventConverter}. On {@link #catchUp()} it registers the live feed first (buffering), replays the
 * projection's history from the store in position order, folds each replayed event, then drains the buffer and goes
 * live, de-duplicating the replay-to-live overlap by an event-id extracted from the <em>domain</em> event (so the
 * de-dup does not depend on the CloudEvent id). A one-shot marker (an optional {@link CheckpointStorage}) makes a
 * restart skip the replay.
 * <p>
 * A replayed event has a CloudEvent behind it, so the replay always folds with real {@link EventMetadata}. A live event
 * does not, so metadata on the live path is something the source supplies: use {@link #accept(EventMetadata, Object)}
 * when the broker message carries the stream id, version or position, which is what a projection keyed on metadata
 * needs to work live as well as during the replay. {@link #accept(Object)} folds with no metadata, and a projection
 * keyed on metadata fed that way fails loud rather than silently skipping the event.
 * <p>
 * Contract (see ADR 62): catch-up is Occurrent's job, live-resume is the broker's (acknowledge after {@code accept}
 * returns). No live position watermark is kept, so delivery is at-least-once and the fold must be idempotent. The
 * de-dup cache and the live buffer are bounded; the buffer fails loud on overflow. The "acknowledge after processing"
 * guarantee holds for the live phase. During the catch-up window {@link #accept(Object)} buffers the event and returns
 * before it is folded, so a message may be acknowledged before it is applied. That is safe because the marker is written
 * only after the drain, so a crash mid-catch-up re-replays the whole history from the store, the backstop for any event
 * acknowledged but not yet folded.
 * <p>
 * The catch-up-then-live coordination itself (the buffer, the de-dup cache, and the drain-then-mark ordering) is
 * delegated to {@link BlockingHandover}, shared with {@code CatchupThenPushSubscriptionModel}.
 */
@NullMarked
public final class CatchupProjectionFeed<E> {

    /** Recently folded event ids retained to de-duplicate the replay-to-live overlap. */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE;
    /** Cap on events buffered from the live feed during the catch-up replay before failing loud. */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = HandoverOptions.DEFAULT_MAX_BUFFERED_EVENTS;

    private final MaterializedView<E> view;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final String id;

    private final BlockingHandover<Delivered<E>> handover;

    private CatchupProjectionFeed(String id, MaterializedView<E> view, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage catchupMarker, HandoverOptions options) {
        this.id = id;
        this.view = view;
        this.replayFilter = replayFilter;
        this.reader = reader;
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.converter = converter;
        this.eventId = eventId;
        this.catchupMarker = catchupMarker;
        this.handover = BlockingHandover.create(
                this::deliver, delivered -> eventKey(delivered.event()), options, "projection feed");
    }

    /**
     * Create a feed materializing {@code projection} into {@code repository}.
     *
     * @param id              The projection's id, used as the single-instance key and the catch-up-marker key.
     * @param projection      The projection to feed.
     * @param repository      The read-model store.
     * @param reader          Reads the projection's history in position order for the catch-up replay.
     * @param converter       Decodes replayed CloudEvents to domain events (replay only, never the live path).
     * @param eventId         Extracts a stable id from a domain event, the replay-to-live de-dup key.
     * @param catchupMarker Records that the catch-up finished so a restart skips it, or {@code null} to always
     *                        catch up.
     */
    public static <S extends @Nullable Object, E, ID> CatchupProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
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
        MaterializedView<E> view = Projections.materializedView(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return create(id, view, filter, reader, converter, eventId, catchupMarker, dedupCacheSize, maxBufferedEvents);
    }

    /**
     * Create a feed driving an existing {@link MaterializedView} (for example one built with the view DSL's
     * {@code materialized(...)} with its own retry/locking policy). {@code replayFilter} selects which stored events to
     * replay; use {@link Filter#all()} to replay everything and let the fold ignore unhandled types.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        return create(id, view, replayFilter, reader, converter, eventId, catchupMarker,
                DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    /**
     * As {@link #create(String, MaterializedView, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with an explicit de-dup cache size and live-buffer cap.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(view, "view cannot be null");
        Objects.requireNonNull(replayFilter, "replayFilter cannot be null");
        Objects.requireNonNull(reader, "reader cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(eventId, "eventId cannot be null");
        HandoverOptions options = new HandoverOptions(dedupCacheSize, maxBufferedEvents);
        return new CatchupProjectionFeed<>(id, view, replayFilter, reader, converter, eventId, catchupMarker, options);
    }

    /**
     * Feed a live domain event. Buffered while the catch-up replay runs, folded directly afterwards, on the calling
     * thread. Call this from the broker listener, acknowledging the message only once it returns.
     *
     * @param event The domain event received from the external source.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        handover.accept(Delivered.live(event));
    }

    /**
     * Feed a live domain event together with the {@link EventMetadata} the source knows about it, so a projection keyed on
     * the stream id, version or position works on the live path and not only during the catch-up replay. Use this when the
     * broker message carries those values (as headers, say) and your listener can read them. Otherwise call
     * {@link #accept(Object)}, which folds with no metadata.
     *
     * @param metadata The metadata the source has for this event.
     * @param event    The domain event received from the external source.
     */
    public void accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        handover.accept(Delivered.live(metadata, event));
    }

    // Two routes on purpose. MaterializedView.update(E) and update(EventMetadata, E) are separate interface methods a
    // caller's view may implement differently, so an event fed without metadata must still take the one-argument route
    // rather than the metadata one carrying EventMetadata.empty(). A replayed delivery always has metadata, so it always
    // takes the metadata route.
    private void deliver(Delivered<E> delivered) {
        EventMetadata metadata = delivered.metadata();
        if (metadata == null) {
            view.update(delivered.event());
        } else {
            view.update(metadata, delivered.event());
        }
    }

    /**
     * Run the one-time catch-up: replay the projection's history from the store (decoding each event once), then drain
     * the buffered live events and go live. Skipped if the catch-up marker already records completion. Call this once,
     * after wiring the live feed, so events arriving during the replay are captured.
     */
    public void catchUp() {
        handover.catchUp(new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                return CatchupProjectionFeed.this.isAlreadyCaughtUp();
            }

            @Override
            public Stream<Delivered<E>> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())
                        // Replay decodes CloudEvents, so metadata is available here (unlike the live accept(E) path).
                        .map(cloudEvent -> Delivered.replayed(EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent)));
            }

            @Override
            public void markCaughtUp() {
                CatchupProjectionFeed.this.markCaughtUp();
            }
        });
    }

    // A null id would collapse every such event to one de-dup key and silently drop deliveries, so fail loud instead.
    private String eventKey(E event) {
        return Objects.requireNonNull(eventId.apply(event), "The eventId function returned null; every domain event must have a stable non-null id for de-duplication.");
    }

    private boolean isAlreadyCaughtUp() {
        return catchupMarker != null && catchupMarker.exists(id);
    }

    private void markCaughtUp() {
        if (catchupMarker != null) {
            // The stored position marks that the catch-up replay completed, not a live resume watermark.
            catchupMarker.save(id, GlobalCheckpoint.of(reader.currentPosition()));
        }
    }

    /**
     * One delivery, live or replayed, with whatever metadata it had. Null metadata means none was given, which is what
     * picks the one-argument {@link MaterializedView} overload in {@link #deliver}.
     * <p>
     * The canonical constructor is private so the only ways in are the three factories below. That keeps "a replayed
     * delivery always has metadata" checkable: {@link #replayed} takes a non-null {@link EventMetadata} under
     * {@code @NullMarked}, so a future edit cannot quietly construct a replayed delivery without it. That guarantee used
     * to come from having two separate carrier types.
     */
    private record Delivered<E>(@Nullable EventMetadata metadata, E event) {

        private Delivered {
        }

        /** A live delivery the source gave no metadata for. */
        static <E> Delivered<E> live(E event) {
            return new Delivered<>(null, event);
        }

        /** A live delivery the source supplied metadata for. */
        static <E> Delivered<E> live(EventMetadata metadata, E event) {
            return new Delivered<>(metadata, event);
        }

        /** A replayed delivery, which always has the metadata decoded from its CloudEvent. */
        static <E> Delivered<E> replayed(EventMetadata metadata, E event) {
            return new Delivered<>(metadata, event);
        }
    }
}
