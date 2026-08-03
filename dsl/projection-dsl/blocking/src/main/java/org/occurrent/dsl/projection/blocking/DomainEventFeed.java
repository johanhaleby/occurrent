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
import org.occurrent.subscription.internal.SingleConsumerMessages;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The domain-event twin of {@code PushSubscriptionModel}: a register-only sink the application owns and feeds with
 * <strong>domain events</strong>, giving one projection a catch-up and then a live feed. It lets an external source
 * (a RabbitMQ or Kafka listener with its own message converter) drive a projection without any CloudEvent conversion
 * on the live path.
 * <p>
 * The application declares it as a bean carrying the domain-specific {@code eventId} function (the catch-up de-dup key)
 * plus the CloudEvent-layer collaborators (the store {@link PositionOrderedReader}, the {@link CloudEventConverter} used
 * only to decode replayed history, and an optional {@link CheckpointStorage} catch-up marker), registers a projection on
 * it (directly, or through {@code @Projection(source = PUSH)}), and feeds each received domain event to
 * {@link #accept(Object)} from its listener. The registration is a {@link CatchupProjectionFeed}, which owns the
 * contract: the broker decides where the live feed resumes, an event can arrive more than once so the fold has to be
 * safe to repeat, and the buffer holding live events during the replay has a fixed size.
 * <p>
 * <strong>One feed feeds one projection</strong>, and a second {@link #register} is refused. The acknowledgement is
 * what forces it: the listener has exactly one decision per received message, so several projections on one feed would
 * share it, and a projection that keeps failing would hold up every projection behind it. Declare one feed per
 * projection, each fed by its own queue. See ADR 88.
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
    // The one projection registered on this feed, or null while it is free. Cleared by nothing today: a feed has no
    // unregister, so this is only ever set once in practice, but reading it is what names the collision.
    private final AtomicReference<@Nullable CatchupProjectionFeed<E>> feed = new AtomicReference<>();

    /**
     * @param reader          The store read used to replay history during the projection's catch-up.
     * @param converter       Decodes replayed CloudEvents to domain events (replay only, never the live path).
     * @param eventId         Extracts a stable id from a domain event, the replay-to-live de-dup key.
     * @param catchupMarker Records catch-up completion so a restart skips the replay, or {@code null}
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
     * Register the projection this feed drives, materializing into {@code repository}.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public <S extends @Nullable Object, ID> void register(String id, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        Objects.requireNonNull(projection, "projection cannot be null");
        Objects.requireNonNull(repository, "repository cannot be null");
        MaterializedView<E> view = Projections.materializedView(projection, repository, id);
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        register(id, view, filter);
    }

    /**
     * Register the projection this feed drives, as an existing {@link MaterializedView} replaying stored events
     * matching {@code replayFilter}.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, MaterializedView<E> view, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        // Built before the slot is claimed, so a registration that fails validation (an unpositioned reader, say)
        // leaves the feed free rather than permanently taken by a projection that never existed.
        CatchupProjectionFeed<E> registering = CatchupProjectionFeed.create(id, view, replayFilter, reader, converter, eventId, catchupMarker, options);
        if (!feed.compareAndSet(null, registering)) {
            CatchupProjectionFeed<E> existing = feed.get();
            throw new IllegalArgumentException(SingleConsumerMessages.singleConsumerOnly(
                    "DomainEventFeed", "projection", existing == null ? "<unknown>" : existing.id(), id));
        }
    }

    /**
     * Feed a live domain event to the registered projection, on the calling thread. Call this from the broker
     * listener, acknowledging the message only once it returns. An exception from the projection propagates.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered != null) {
            registered.accept(event);
        }
    }

    /**
     * Feed a live domain event to the registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with no metadata.
     */
    public void accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered != null) {
            registered.accept(metadata, event);
        }
    }

    /**
     * Run the one-time catch-up of the registered projection (replay history, then go live). Call once, after the
     * projection is registered and the live feed is wired. A no-op when nothing is registered.
     * <p>
     * A failure here is terminal for this feed, so let it reach the caller and do not start the application. The
     * projection rejects every later event afterwards. Unlike a subscription model, the feed does not drop it: the
     * application asked for this projection, so running on without it is worse than not running. Fix the cause and
     * build a new feed.
     * <p>
     * Named for when a feed could carry several projections. It carries one, so this and {@link #catchUp(String)} do
     * the same thing whenever the id matches.
     */
    public void catchUpAll() {
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered != null) {
            registered.catchUp();
        }
    }

    /**
     * Run the one-time catch-up of the projection registered under {@code id}. Use this over {@link #catchUpAll()}
     * when the caller knows which projection it means and wants a mismatch to fail rather than pass silently.
     *
     * @throws IllegalArgumentException if no projection with that id is registered on this feed
     */
    public void catchUp(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered == null || !registered.id().equals(id)) {
            throw new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed.");
        }
        registered.catchUp();
    }

    /**
     * Stop a catch-up replay that is still in flight, so a shutting-down application does not leave one folding into
     * a store that is closing with it. The replay notices at its next event and unwinds without writing the
     * completion marker, so the next start replays the whole history again.
     * <p>
     * Stopping is what a caller cannot do for itself. Backgrounding is not, since a caller that wants the replay off
     * its own thread can run {@link #catchUpAll()} on a thread it owns, which is what the Spring starter does for
     * {@code startupMode = BACKGROUND}.
     */
    public void stopCatchUp() {
        CatchupProjectionFeed<E> registered = feed.get();
        if (registered != null) {
            registered.stopCatchUp();
        }
    }
}
