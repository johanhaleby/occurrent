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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.internal.HandoverMessages;

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
 * de-dup cache and the live buffer are bounded. The buffer fails loud on overflow. The "acknowledge after processing"
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

    private final MaterializedView<E> view;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final String id;

    private final BlockingHandover<Delivered<E>> handover;
    // Read by the replay once per event, so stopCatchUp() takes effect at the next event rather than at the end.
    private volatile boolean stopped = false;

    private CatchupProjectionFeed(String id, MaterializedView<E> view, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
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
        return create(id, projection, repository, reader, converter, eventId, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * As {@link #create(String, Projection, ViewStateRepository, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with explicit handover {@code options}.
     */
    public static <S extends @Nullable Object, E, ID> CatchupProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        MaterializedView<E> view = Projections.materializedView(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return create(id, view, filter, reader, converter, eventId, catchupMarker, options);
    }

    /**
     * Create a feed driving an existing {@link MaterializedView} (for example one built with the view DSL's
     * {@code materialized(...)} with its own retry/locking policy). {@code replayFilter} selects which stored events to
     * replay, use {@link Filter#all()} to replay everything and let the fold ignore unhandled types.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        return create(id, view, replayFilter, reader, converter, eventId, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * As {@link #create(String, MaterializedView, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with explicit handover {@code options}.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, MaterializedView<E> view, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(view, "view cannot be null");
        Objects.requireNonNull(replayFilter, "replayFilter cannot be null");
        Objects.requireNonNull(reader, "reader cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(eventId, "eventId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
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

    // Package-private. Lets DomainEventFeed.acceptCloudEvent(CloudEvent) tell a genuinely dropped live event (this
    // feed's replay was stopped) apart from one that was actually buffered or delivered, which
    // BlockingHandover.accept(..) alone cannot report through its void contract.
    boolean acceptReportingDelivery(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.acceptReportingDelivery(Delivered.live(metadata, event));
    }

    // Package-private. Lets DomainEventFeed.acceptCloudEvent(CloudEvent) refuse rather than buffer an event it can
    // redeliver, one evaluation deciding both the live check and the accept, see BlockingHandover.acceptIfLive(..)
    // for why that matters.
    boolean acceptIfLive(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.acceptIfLive(Delivered.live(metadata, event));
    }

    // Two routes on purpose. MaterializedView.update(E) and update(EventMetadata, E) are separate interface methods a
    // caller's view may implement differently, so an event fed without metadata must still take the one-argument route
    // rather than the metadata one carrying EventMetadata.empty(). A replayed delivery always has metadata, so it always
    // takes the metadata route.
    private void deliver(Delivered<E> delivered) {
        @Nullable EventMetadata metadata = delivered.metadata();
        if (metadata == null) {
            view.update(delivered.event());
        } else {
            view.update(metadata, delivered.event());
        }
    }

    // Package-private: lets DomainEventFeed.catchUp(String) check that the id it was given is the one registered.
    String id() {
        return id;
    }

    // Package-private. Delegates to the handover instead of tracking a second copy of its own live/failed state,
    // since every past attempt at keeping that copy in sync found another way it could drift from the original.
    // See BlockingHandover.isReadyForLiveDelivery()'s own javadoc for exactly what this answers.
    boolean isReadyForLiveDelivery() {
        return handover.isReadyForLiveDelivery();
    }

    // Package-private, beside isReadyForLiveDelivery() and for the same reason: the handover owns this state, so
    // asking it beats tracking a second copy. False until this feed's own catch-up throws and true forever after,
    // which is what makes it safe to read after catching the refusal rather than at the moment it was thrown.
    boolean refusesPermanently() {
        return handover.refusesPermanently();
    }

    /**
     * Run the one-time catch-up: replay the projection's history from the store (decoding each event once), then drain
     * the buffered live events and go live. Skipped if the catch-up marker already records completion. Call this once,
     * after wiring the live feed, so events arriving during the replay are captured.
     * <p>
     * Runs on the calling thread. A caller that wants it off that thread runs it on a thread it owns, and calls
     * {@link #stopCatchUp()} to bring it back down.
     */
    public void catchUp() {
        // Cleared here rather than only in the handover, so a feed stopped once can catch up again instead of
        // stopping instantly on the first replayed event.
        stopped = false;
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
            public boolean keepReplaying() {
                return !stopped;
            }

            @Override
            public void markCaughtUp() {
                CatchupProjectionFeed.this.markCaughtUp();
            }

            @Override
            public void replayStarted() {
                if (view instanceof ReplayAware replayAware) {
                    replayAware.replayStarted();
                }
            }

            @Override
            public void replayCompleted() {
                if (view instanceof ReplayAware replayAware) {
                    replayAware.replayCompleted();
                }
            }

            @Override
            public void replayAbandoned() {
                if (view instanceof ReplayAware replayAware) {
                    replayAware.replayAbandoned();
                }
            }
        });
    }

    /**
     * Go live without a catch-up: drain whatever live events have buffered since {@link #accept} started being
     * called, then deliver events directly from here on. Use this instead of {@link #catchUp()} for a feed whose
     * events are not in the local event store, so there is nothing to replay. No completion marker is written, since
     * nothing was replayed, so a later {@link #catchUp()} still replays the full history.
     * <p>
     * Delivery is still at-least-once here, so the view has to tolerate the same event arriving twice. The de-dup
     * cache only suppresses the overlap between a replay and the live feed, and there is no replay on this path, so
     * it is not a guard against your broker redelivering a message.
     */
    public void goLive() {
        handover.catchUp(new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                return true;
            }

            @Override
            public Stream<Delivered<E>> replay() {
                throw new AssertionError("isAlreadyCaughtUp() is true, so this must never be called.");
            }

            @Override
            public void markCaughtUp() {
                throw new AssertionError("isAlreadyCaughtUp() is true, so nothing here was caught up to mark.");
            }
        });
    }

    /**
     * Stop a replay still in flight. It notices at its next event and unwinds without draining the live buffer, going
     * live, or writing the completion marker, so a partial replay is never recorded as a finished one and the next
     * {@link #catchUp()} replays the whole history again. A stop is not a failure: the feed stays usable rather than
     * rejecting every later event.
     */
    public void stopCatchUp() {
        stopped = true;
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
     * Construct through the three factories rather than the canonical constructor. That keeps "a replayed delivery
     * always has metadata" checkable, because {@link #replayed} takes a non-null {@link EventMetadata} under
     * {@code @NullMarked} while the canonical constructor accepts a nullable one, so JSpecify flags a replayed delivery
     * built without metadata. That guarantee used to come from having two separate carrier types.
     * <p>
     * The reactor feed's sibling carrier spells "none given" as {@link EventMetadata#empty()} rather than null, so the
     * two are not interchangeable despite both engines now taking one type parameter. Reconciling them is tracked
     * separately, see the 2026-07-27 amendment in ADR 62.
     */
    private record Delivered<E>(@Nullable EventMetadata metadata, E event) {

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
