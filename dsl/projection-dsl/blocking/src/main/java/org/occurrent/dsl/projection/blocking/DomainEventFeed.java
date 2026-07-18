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
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The domain-event twin of {@code PushSubscriptionModel}: a register-only sink the application owns and feeds with
 * <strong>domain events</strong>, fanning each one out to every registered projection, with a per-projection bootstrap
 * catch-up. It lets one external feed (a RabbitMQ or Kafka listener with its own message converter) drive several
 * projections without any CloudEvent conversion on the live path.
 * <p>
 * The application declares it as a bean carrying the domain-specific {@code eventId} function (the catch-up de-dup key)
 * plus the CloudEvent-layer collaborators (the store {@link PositionOrderedReader}, the {@link CloudEventConverter} used
 * only to decode replayed history, and an optional {@link CheckpointStorage} bootstrap marker), registers projections on
 * it (directly, or through {@code @Projection(source = PUSH)}), and feeds each received domain event to
 * {@link #accept(Object)} from its listener. Each registration is a {@link BootstrappingProjectionFeed}, so the
 * contract, live-resume owned by the broker, at-least-once idempotent folds, bounded buffering, is per projection.
 */
@NullMarked
public final class DomainEventFeed<E> {

    private final PositionOrderedReader reader;
    private final CloudEventConverter<E> converter;
    private final java.util.function.Function<E, String> eventId;
    private final @Nullable CheckpointStorage bootstrapMarker;
    private final CopyOnWriteArrayList<BootstrappingProjectionFeed<E>> feeds = new CopyOnWriteArrayList<>();

    /**
     * @param reader          The store read used to replay history during each projection's bootstrap.
     * @param converter       Decodes replayed CloudEvents to domain events (replay only, never the live path).
     * @param eventId         Extracts a stable id from a domain event, the replay-to-live de-dup key, shared by every
     *                        projection on this feed.
     * @param bootstrapMarker Records per-projection bootstrap completion so a restart skips the replay, or {@code null}
     *                        to always bootstrap.
     */
    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter,
                           java.util.function.Function<E, String> eventId, @Nullable CheckpointStorage bootstrapMarker) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.converter = Objects.requireNonNull(converter, "converter cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.bootstrapMarker = bootstrapMarker;
    }

    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter, java.util.function.Function<E, String> eventId) {
        this(reader, converter, eventId, null);
    }

    /**
     * Register a projection to be fed and bootstrapped by this feed, materializing into {@code repository}.
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
        feeds.add(BootstrappingProjectionFeed.create(id, view, replayFilter, reader, converter, eventId, bootstrapMarker));
    }

    /**
     * Feed a live domain event to every registered projection, on the calling thread. Call this from the broker
     * listener, acknowledging the message only once it returns. An exception from any projection propagates.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        for (BootstrappingProjectionFeed<E> feed : feeds) {
            feed.accept(event);
        }
    }

    /**
     * Run the one-time bootstrap of every registered projection (replay history, then go live). Call once, after all
     * projections are registered and the live feed is wired.
     */
    public void bootstrapAll() {
        for (BootstrappingProjectionFeed<E> feed : feeds) {
            feed.bootstrap();
        }
    }
}
