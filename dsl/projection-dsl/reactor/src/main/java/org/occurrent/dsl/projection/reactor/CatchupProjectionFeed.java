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
import java.util.concurrent.atomic.AtomicLong;
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

    private final ReactiveHandover<DeliveredEvent<E>, String> handover;
    // Counts stopCatchUp() calls. Read by the replay once per event, so a stop takes effect at the next event rather
    // than at the end.
    private final AtomicLong stops = new AtomicLong();

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
     * catch-up are buffered and delivered after the replay, and their {@link Mono} completes only then.
     * <p>
     * It errors with an {@link IllegalStateException} instead when the event was not folded, because the catch-up was
     * stopped before the feed went live, {@link #stopCatchUp()} was called while the feed had not gone live and no
     * catch-up was running, the event was fed after either of those stops and before the next {@link #catchUp()} or
     * {@link #goLive()}, the feed is failing or has failed, or the live buffer is full. The listener must not
     * acknowledge it, and the broker delivers it again.
     * <p>
     * Called from inside this feed's fold, it completes once the event is queued instead, since this feed folds one
     * event at a time and the event cannot be folded before that fold returns. The fold's call is recognized when the
     * returned {@link Mono} is part of the {@link Mono} the fold returns, or is subscribed, blocking or not, on the
     * thread this feed called the fold on. The event is folded after that fold, in the order it was fed. When folding
     * it fails, this feed starts failing, and a failed catch-up starts it failing the same way. It deletes its catch-up
     * marker, refuses every later event that does not come from its fold, folds the events it has already taken in and
     * those its fold feeds it meanwhile, and then fails for good. A failed catch-up also refuses each event from
     * anywhere else that is still waiting, and does not fold it. Build a new feed, and once the marker is gone its catch-up replays the history. When deleting
     * the marker still fails after 3 retries, the feed logs an error naming the feed id, and the marker has to be
     * deleted by hand before building a new feed. An event that no replay can bring back is lost only when its own
     * fold failed.
     *
     * @param event The domain event received from the external source.
     * @return A {@link Mono} that completes when the event has been folded.
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
     * <p>
     * Completes and errors for the same reasons {@link #accept(Object)} does.
     *
     * @param metadata The metadata the source has for this event.
     * @param event    The domain event received from the external source.
     * @return A {@link Mono} that completes when the event has been folded.
     */
    public Mono<Void> accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return handover.accept(new DeliveredEvent<>(metadata, event));
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
     * <p>
     * A call the view makes while this feed is calling it, from its fold or from a callback such as
     * {@code replayStarted()}, completes without waiting for the replay, since that replay cannot start before the
     * view's code returns. The call is recognized when the returned {@link Mono} is part of the {@link Mono} the view's
     * code returns, or is subscribed, blocking or not, on the thread this feed called that code on. A view that blocks
     * on it from a thread it switched to is not recognized, and waits for a replay that cannot start while it blocks.
     * Completing then means the catch-up was asked for, not that it has run. It does not start before the view's code
     * returns, and it can still be stopped by {@link #stopCatchUp()}, or refused because another catch-up on this feed
     * failed, and neither reaches the view. When its replay fails, this feed refuses every later event that does not
     * come from the view's fold, the same as after any failed catch-up. When the view makes several such calls
     * before the replay they asked for starts, that replay runs once for all of them.
     * <p>
     * A view that calls this for an event a replay delivers asks for another catch-up each time a replay delivers that
     * event again. Each of those catch-ups replays again unless it finds the catch-up marker written, so a feed built
     * without a {@link CheckpointStorage} for that marker replays without end.
     *
     * @return A {@link Mono} that completes when the catch-up replay has finished and the feed has gone live, or, for a
     *         call the view makes while this feed is calling it, once the catch-up has been asked for.
     */
    public Mono<Void> catchUp() {
        // A stop before this call does not stop this catch-up, and a stop after it does, even when the view makes
        // this call while a replay runs. Clearing a shared flag here would undo a stop the running replay has not
        // noticed yet. Deliberately NOT wrapped in Mono.defer: the handover subscribes its own pipeline as soon as
        // this call is made, so deferring would let a re-subscription of the returned Mono start a second catch-up
        // over the same one-subscriber live sink, which fails it permanently.
        long stopsWhenAsked = stops.get();
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
                return stops.get() == stopsWhenAsked;
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return CatchupProjectionFeed.this.markCaughtUp();
            }

            @Override
            public Mono<Void> forgetCaughtUp() {
                return CatchupProjectionFeed.this.forgetCaughtUp();
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

            @Override
            public Mono<Void> alreadyDeliveredByReplay(DeliveredEvent<E> delivered) {
                if (fold instanceof ReactiveReplayAware replayAware) {
                    return replayAware.alreadyDeliveredByReplay(delivered.metadata());
                }
                return Mono.empty();
            }
        }).then();
    }

    /**
     * Go live without a catch-up: skip the one-time replay and start delivering buffered live events. Use this
     * instead of {@link #catchUp()} for a feed whose events are not in the local event store, so there is nothing to
     * replay. No completion marker is recorded, since nothing was replayed, so a later {@link #catchUp()} still
     * replays the full history.
     * <p>
     * A second call, or a call after {@link #catchUp()} has finished, finds the feed already live and changes nothing.
     * A live copy of an event that catch-up applied is still de-duplicated and still recorded.
     * <p>
     * A call while {@link #catchUp()} is still replaying completes only once that replay has ended. When a catch-up on
     * this feed failed while it waited, it errors with an {@link IllegalStateException} whose cause is a catch-up
     * failure recorded while it waited. A call the view makes while this feed is calling it, from its fold or from a
     * callback such as {@code replayStarted()}, completes without waiting for the replay, since that replay cannot end
     * before the call does.
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

            // A catchUp() after this call records its marker, and this source can still be the one that delivers
            // live, so it forgets that marker the same way.
            @Override
            public Mono<Void> forgetCaughtUp() {
                return CatchupProjectionFeed.this.forgetCaughtUp();
            }
        }).then();
    }

    /**
     * Stop a replay still in flight. It notices at its next event and unwinds without recording the completion marker,
     * so a partial replay is never recorded as a finished one and the next {@link #catchUp()} replays the whole
     * history again. A stop is not a failure: the feed stays usable rather than failing every later event.
     * <p>
     * A replay notices a stop at the next event it hands to the view, and notices only a stop that came after its own
     * {@link #catchUp()} call. A catch-up asked for before the stop still notices it when another {@link #catchUp()}
     * comes after the stop, from the view or from anywhere else. A catch-up with no event left to hand to the view does
     * not notice a stop, and finishes as if there had been none, so the feed goes live. That is a {@link #goLive()},
     * even one called before the stop and waiting for the stopped replay, one whose history is empty, one that finds
     * the marker already written, and one whose replay has handed its last event to the view, for example while a view
     * that buffers during a replay writes that buffer in {@code replayCompleted()}.
     * <p>
     * What a stop the replay notices does with the live events depends on where the feed stood when the replay started.
     * One that had not gone live drains nothing and does not go live, and the {@link Mono} {@link #accept(Object)}
     * returned for each event it held errors rather than completing, the same as for an event fed after the stop, so
     * the listener does not acknowledge it and the broker delivers it again. One replaying after a {@link #goLive()}
     * delivers what it held while the replay ran and goes on delivering, since those events were accepted by a feed
     * that was already live.
     * <p>
     * A feed with no catch-up running that has not gone live stops the same way. The {@link Mono}
     * {@link #accept(Object)} returned for an event fed before the stop, or after it and before the next
     * {@link #catchUp()} or {@link #goLive()}, errors, so a shutting-down application that never starts a catch-up does
     * not leave it waiting. When this method finds no catch-up running and stops the feed, the error handling of
     * each event still waiting runs on the calling thread, unless the listener's own pipeline moves it. A catch-up
     * started after the stop does not fold that event, and the broker delivers it again. A catch-up counts as running
     * from the {@link #catchUp()} or {@link #goLive()} call until the feed goes live, its replay notices a stop, or it
     * fails. A stop that comes during a {@link #catchUp()} call that then throws, for example because reading the
     * catch-up marker threw, still errors the {@link Mono} of each waiting event, unless another catch-up takes the
     * feed live and applies that event. That error handling runs where the last of the catch-ups running then ends, on
     * the thread whose {@link #catchUp()} call threw or on a thread that runs another catch-up.
     * <p>
     * A view that buffers during a replay discards that buffer on a stop, so after a {@link #goLive()} the live copy
     * of an event the stopped replay delivered is delivered again rather than skipped as a duplicate. A view that
     * wrote the event through receives it twice, which at-least-once delivery allows.
     */
    public void stopCatchUp() {
        stops.incrementAndGet();
        handover.stopIfNotCatchingUp();
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

    // Names the id, since the handover logs this when every attempt failed and an operator then deletes the marker by
    // hand.
    private Mono<Void> forgetCaughtUp() {
        return catchupMarker == null ? Mono.empty() : catchupMarker.delete(id)
                .onErrorMap(error -> new IllegalStateException("Could not delete the catch-up marker of projection "
                        + "feed " + id + ".", error));
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
