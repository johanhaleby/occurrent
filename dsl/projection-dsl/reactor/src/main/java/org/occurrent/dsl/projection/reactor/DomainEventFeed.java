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
import org.occurrent.subscription.internal.SingleConsumerMessages;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.BiFunction;

/**
 * The reactor counterpart of the blocking {@code DomainEventFeed}: a register-only sink the application owns and feeds
 * with <strong>domain events</strong>, giving one projection a catch-up and then a live feed, without any CloudEvent
 * conversion on the live path. See the blocking {@code DomainEventFeed} for the full contract.
 * <p>
 * <strong>One feed feeds one projection</strong>, and a second {@link #register} is refused. The acknowledgement is
 * what forces it: the listener has exactly one decision per received message, so several projections on one feed would
 * share it, and a projection that keeps failing would hold up every projection behind it. Declare one feed per
 * projection, each fed by its own queue. See ADR 90.
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
    // The one projection registered on this feed, or null while it is free.
    private final AtomicReference<@Nullable CatchupProjectionFeed<E>> feed = new AtomicReference<>();

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
     * Register the projection this feed drives, materializing into the blocking {@code repository} (folded on
     * {@code boundedElastic}).
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public <S extends @Nullable Object, ID> void register(String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        claim(id, CatchupProjectionFeed.create(id, projection, repository, reader, converter, eventId, catchupMarker, options));
    }

    /**
     * Register the projection this feed drives, as an existing reactive {@code fold} (for example
     * {@code Projections.reactiveUpdate(materializedView)}) replaying stored events matching {@code replayFilter}. The
     * reactor analog of the blocking {@code register(id, MaterializedView, Filter)}.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, Function<E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        claim(id, CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options));
    }

    /**
     * Register the projection this feed drives, as a metadata-aware reactive {@code fold}, the form that can key or
     * fold on the event's {@link EventMetadata}. The replay always supplies the metadata it decoded from the
     * CloudEvent, and the live path supplies whatever the source passed to {@link #accept(EventMetadata, Object)}.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        claim(id, CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options));
    }

    // The projection feed is built by the caller before this runs, so a registration that fails validation (an
    // unpositioned reader, say) leaves the feed free rather than permanently taken by a projection that never existed.
    private void claim(String id, CatchupProjectionFeed<E> registering) {
        if (!feed.compareAndSet(null, registering)) {
            CatchupProjectionFeed<E> existing = feed.get();
            throw new IllegalArgumentException(SingleConsumerMessages.singleConsumerOnly(
                    "DomainEventFeed", "projection", existing == null ? "<unknown>" : existing.id(), id));
        }
    }

    /**
     * Feed a live domain event to the registered projection. The returned {@link Mono} completes once the projection
     * has handled it, so the listener can acknowledge after processing.
     */
    public Mono<Void> accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        CatchupProjectionFeed<E> registered = feed.get();
        return registered == null ? Mono.empty() : registered.accept(event);
    }

    /**
     * Feed a live domain event to the registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with {@link EventMetadata#empty()}.
     */
    public Mono<Void> accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        CatchupProjectionFeed<E> registered = feed.get();
        return registered == null ? Mono.empty() : registered.accept(metadata, event);
    }

    /**
     * Run the one-time catch-up of the registered projection. The returned {@link Mono} completes once it has caught
     * up and gone live, and is empty when nothing is registered.
     * <p>
     * An error on the returned {@link Mono} is terminal for this feed, so let it reach the caller and do not start the
     * application. The projection rejects every later event afterwards. Unlike a subscription model, the feed does not
     * drop it: the application asked for this projection, so running on without it is worse than not running. Fix the
     * cause and build a new feed.
     * <p>
     * Named for when a feed could carry several projections. It carries one.
     */
    public Mono<Void> catchUpAll() {
        CatchupProjectionFeed<E> registered = feed.get();
        return registered == null ? Mono.empty() : registered.catchUp();
    }

    /**
     * Stop a catch-up replay that is still in flight, so a shutting-down application does not leave one folding into
     * a store that is closing with it. The replay notices at its next event and unwinds without recording the
     * completion marker, so the next start replays the whole history again.
     * <p>
     * Stopping is what a caller cannot do for itself. Backgrounding is not, since the returned {@link Mono} from
     * {@link #catchUpAll()} is the caller's to compose or not.
     */
    public void stopCatchUp() {
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered != null) {
            registered.stopCatchUp();
        }
    }

    /**
     * Run the one-time catch-up of the single projection registered under {@code id}. Use this instead of
     * {@link #catchUpAll()} when a projection is registered well after the others on this feed already went live, so
     * that catching it up does not re-run the catch-up of a projection that already ran it.
     *
     * The lookup happens when the returned {@link Mono} is subscribed, so a projection registered between building
     * this {@link Mono} and subscribing to it is found. An id that matches nothing fails the {@link Mono} with an
     * {@link IllegalArgumentException} rather than throwing here.
     *
     * @return A {@link Mono} that completes once that projection has caught up and gone live.
     */
    public Mono<Void> catchUp(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        return Mono.defer(() -> {
            CatchupProjectionFeed<E> registered = feed.get();
            if (registered == null || !registered.id().equals(id)) {
                return Mono.error(new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed."));
            }
            return registered.catchUp();
        });
    }
}
