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
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.internal.ReactiveHandover;
import org.occurrent.subscription.internal.HandoverMessages;
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

    private final BiFunction<EventMetadata, E, Mono<Void>> fold;
    private final Filter replayFilter;
    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final Function<E, String> eventId;
    private final @Nullable CheckpointStorage catchupMarker;
    private final String id;

    private final ReactiveHandover<DeliveredEvent<E>> handover;
    // Read by the replay once per event, so stopCatchUp() takes effect at the next event rather than at the end.
    private volatile boolean stopped = false;

    private CatchupProjectionFeed(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter, PositionOrderedReader reader,
                                        CloudEventConverter<E> converter, Function<E, String> eventId,
                                        @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
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
                delivered -> fold.apply(delivered.metadata(), delivered.event()), delivered -> eventKey(delivered.event()),
                options, "projection feed");
    }

    /**
     * Create a feed materializing {@code projection} into the blocking {@code repository} (folded on
     * {@code boundedElastic}). See the blocking {@code CatchupProjectionFeed} for the parameter contract.
     */
    public static <S extends @Nullable Object, E, ID> CatchupProjectionFeed<E> create(
            String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
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
        // The metadata-aware fold, so a projection keyed by metadata (a stream id, say) resolves the same instance during
        // the replay as it does live. reactiveUpdate(...) would hardwire EventMetadata.empty() and mis-key every
        // replayed event.
        BiFunction<EventMetadata, E, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        return create(id, fold, filter, reader, converter, eventId, catchupMarker, options);
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
        return create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * As {@link #create(String, Function, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with explicit handover {@code options}.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, Function<E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        Objects.requireNonNull(fold, "fold cannot be null");
        // A caller-supplied one-argument fold has no metadata channel, so the replay drops the metadata it decoded.
        return create(id, (metadata, event) -> fold.apply(event), replayFilter, reader, converter, eventId, catchupMarker, options);
    }

    /**
     * Create a feed driving a metadata-aware {@code fold}, the form that can key or fold on the event's
     * {@link EventMetadata}. Prefer this over
     * {@link #create(String, Function, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)}
     * when the fold reads metadata: the replay always supplies the metadata it decoded from the CloudEvent, and the live
     * path supplies whatever the source passed to {@link #accept(EventMetadata, Object)}.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker) {
        return create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * As {@link #create(String, BiFunction, Filter, PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage)},
     * with explicit handover {@code options}.
     */
    public static <E> CatchupProjectionFeed<E> create(
            String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter,
            PositionOrderedReader reader, CloudEventConverter<E> converter, Function<E, String> eventId,
            @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(fold, "fold cannot be null");
        Objects.requireNonNull(replayFilter, "replayFilter cannot be null");
        Objects.requireNonNull(reader, "reader cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(eventId, "eventId cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        return new CatchupProjectionFeed<>(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options);
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
        return handover.accept(new DeliveredEvent<>(EventMetadata.empty(), event));
    }

    /**
     * Feed a live domain event together with the {@link EventMetadata} the source knows about it, so a projection keyed on
     * the stream id, version or position works on the live path and not only during the catch-up replay. Use this when the
     * broker message carries those values (as headers, say) and your listener can read them. Otherwise call
     * {@link #accept(Object)}, which folds with {@link EventMetadata#empty()}.
     *
     * @param metadata The metadata the source has for this event.
     * @param event    The domain event received from the external source.
     * @return A {@link Mono} that completes when the event has been handled.
     */
    public Mono<Void> accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.accept(new DeliveredEvent<>(metadata, event));
    }

    // Package-private. Lets DomainEventFeed.acceptCloudEvent(CloudEvent) tell a genuinely dropped live event (this
    // feed's replay was stopped) apart from one that was actually buffered or delivered, which
    // ReactiveHandover.accept(..) alone cannot report through its Mono<Void> contract.
    Mono<Boolean> acceptReportingDelivery(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.acceptReportingDelivery(new DeliveredEvent<>(metadata, event));
    }

    // Package-private. Lets DomainEventFeed.acceptCloudEvent(CloudEvent) refuse rather than buffer an event it can
    // redeliver, one evaluation deciding both the live check and the accept, see ReactiveHandover.acceptIfLive(..)
    // for why that matters.
    Mono<Boolean> acceptIfLive(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.acceptIfLive(new DeliveredEvent<>(metadata, event));
    }

    /**
     * Run the one-time catch-up: replay the projection's history from the store (decoding each event once), record the
     * completion marker, then start delivering the live feed. The returned {@link Mono} completes when the replay and
     * marker are done. Call once, after wiring the live feed.
     *
     * @return A {@link Mono} that completes when the catch-up replay has finished and the feed has gone live.
     */
    public Mono<Void> catchUp() {
        // Cleared here rather than on subscribe, so a feed stopped once can catch up again instead of stopping
        // instantly on the first replayed event. Deliberately NOT wrapped in Mono.defer: the handover subscribes its
        // own pipeline as soon as this call is made, so deferring would let a re-subscription of the returned Mono
        // start a second catch-up over the same one-subscriber live sink, which fails it permanently.
        stopped = false;
        // then() drops whether the catch-up finished or was stopped. A stop here is always one this feed's own owner
        // asked for, so it already knows.
        return handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return CatchupProjectionFeed.this.alreadyCaughtUp();
            }

            @Override
            public Flux<DeliveredEvent<E>> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())
                        .map(CatchupProjectionFeed.this::replayedItem);
            }

            @Override
            public boolean keepReplaying() {
                return !stopped;
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return CatchupProjectionFeed.this.markCaughtUp();
            }

            @Override
            public void replayStarted() {
                if (fold instanceof ReactiveReplayAware replayAware) {
                    replayAware.replayStarted();
                }
            }

            @Override
            public Mono<Void> replayCompleted() {
                if (fold instanceof ReactiveReplayAware replayAware) {
                    return replayAware.replayCompleted();
                }
                return Mono.empty();
            }

            @Override
            public void replayAbandoned() {
                if (fold instanceof ReactiveReplayAware replayAware) {
                    replayAware.replayAbandoned();
                }
            }
        }).then();
    }

    /**
     * Go live without a catch-up: skip the one-time replay and start delivering buffered live events. Use this
     * instead of {@link #catchUp()} for a feed whose events are not in the local event store, so there is nothing to
     * replay. No completion marker is recorded, since nothing was replayed, so a later {@link #catchUp()} still
     * replays the full history.
     * <p>
     * Call once, the same as {@link #catchUp()}. A second call on the same feed does not error, but does nothing: it
     * tries to subscribe this feed's live sink a second time, which the sink rejects because it accepts only one
     * subscriber ever, and nothing surfaces that rejection to the caller.
     * <p>
     * Delivery is still at-least-once here, so the view has to tolerate the same event arriving twice. The de-dup
     * cache only suppresses the overlap between a replay and the live feed, and there is no replay on this path, so
     * it is not a guard against your broker redelivering a message.
     *
     * @return A {@link Mono} that completes once the feed is live.
     */
    public Mono<Void> goLive() {
        return handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return Mono.just(true);
            }

            @Override
            public Flux<DeliveredEvent<E>> replay() {
                throw new AssertionError("isAlreadyCaughtUp() is true, so this must never be called.");
            }

            @Override
            public Mono<Void> markCaughtUp() {
                throw new AssertionError("isAlreadyCaughtUp() is true, so nothing here was caught up to mark.");
            }
        }).then();
    }

    /**
     * Stop a replay still in flight. It notices at its next event and unwinds without draining the live buffer, going
     * live, or recording the completion marker, so a partial replay is never recorded as a finished one and the next
     * {@link #catchUp()} replays the whole history again. A stop is not a failure: the feed stays usable rather than
     * failing every later event.
     */
    public void stopCatchUp() {
        stopped = true;
    }

    // Package-private: lets DomainEventFeed check the id it was given and name the projection it already has.
    String id() {
        return id;
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

    private DeliveredEvent<E> replayedItem(CloudEvent cloudEvent) {
        return new DeliveredEvent<>(EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent));
    }

    // Carries whatever metadata the delivery had: decoded from the CloudEvent on the replay, supplied by the source
    // on the live path, or empty when the source gave none. Live and replay share this because the reactor fold is a
    // single BiFunction, unlike the blocking stack where MaterializedView has two separate update overloads.
    private record DeliveredEvent<E>(EventMetadata metadata, E event) {
    }
}
