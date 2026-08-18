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
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.internal.SingleConsumerMessages;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The reactor counterpart of the blocking {@code DomainEventFeed}: a register-only sink the application owns and feeds
 * with <strong>domain events</strong>, giving one projection a catch-up and then a live feed, without any CloudEvent
 * conversion on the live path, through {@link #accept(Object)} and {@link #accept(EventMetadata, Object)}.
 * {@link #acceptCloudEvent(CloudEvent)} is the one exception, for a listener that has a {@link CloudEvent} to
 * rebuild rather than an already-decoded domain event. See the blocking {@code DomainEventFeed} for the full contract.
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
    private final DataFieldReader dataFieldReader;
    // The one projection registered on this feed, or null while it is free. Paired with the Filter it was
    // registered with, so acceptCloudEvent always reads the two together.
    private final AtomicReference<@Nullable Registered<E>> feed = new AtomicReference<>();

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
        this(reader, converter, eventId, catchupMarker, options, DataFieldReader.refusing());
    }

    /**
     * As {@link #DomainEventFeed(PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage, CatchupThenLiveOptions)},
     * additionally answering a {@code data} payload condition on a registration's replay filter by reading it
     * through {@code dataFieldReader} instead of refusing it. Only {@link #acceptCloudEvent(CloudEvent)} consults this, since
     * that is the one entry point that evaluates a filter live rather than only during the replay.
     */
    public DomainEventFeed(PositionOrderedReader reader, CloudEventConverter<E> converter,
                           Function<E, String> eventId, @Nullable CheckpointStorage catchupMarker,
                           CatchupThenLiveOptions options, DataFieldReader dataFieldReader) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.converter = Objects.requireNonNull(converter, "converter cannot be null");
        this.eventId = Objects.requireNonNull(eventId, "eventId cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.dataFieldReader = Objects.requireNonNull(dataFieldReader, DataFieldReader.class.getSimpleName() + " cannot be null");
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
        // Derived once, here, and passed into the Filter-taking create(..) overload below rather than the
        // Projection-taking one, which would derive it again from the same converter and projection. Two
        // derivations can drift if the converter is not a pure function of its input, and even a pure one is the
        // "two copies that nothing compares" shape this class exists to avoid.
        Filter filter = ProjectionFilters.filterFor(converter, projection);
        BiFunction<EventMetadata, E, Mono<Void>> fold = Projections.reactiveUpdateWithMetadata(projection, repository, id);
        claim(id, filter, CatchupProjectionFeed.create(id, fold, filter, reader, converter, eventId, catchupMarker, options));
    }

    /**
     * Register the projection this feed drives, as an existing reactive {@code fold} (for example
     * {@code Projections.reactiveUpdate(materializedView)}) replaying stored events matching {@code replayFilter}. The
     * reactor analog of the blocking {@code register(id, MaterializedView, Filter)}.
     * <p>
     * {@code replayFilter} is also what {@link #acceptCloudEvent(CloudEvent)} matches live events against, wrapped as an
     * {@link AgnosticSubscriptionFilter}. The one filter given here is the only one this feed ever holds, so the
     * replay and the live path can never disagree about which events are this projection's. This does not build that
     * matcher, or otherwise change what {@code replayFilter} was already accepted for before
     * {@link #acceptCloudEvent} existed. The store still evaluates it during the replay however it always has,
     * including a {@code data} payload condition this feed has no {@link DataFieldReader} for, since that evaluation
     * has nothing to do with this feed's own {@link DataFieldReader}. {@link #acceptCloudEvent} is what needs one,
     * and only refuses such a filter there, the first time it is called, so a caller that never touches the live
     * CloudEvent path keeps registering exactly the filters it always could.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, Function<E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        claim(id, replayFilter, CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options));
    }

    /**
     * Register the projection this feed drives, as a metadata-aware reactive {@code fold}, the form that can key or
     * fold on the event's {@link EventMetadata}. The replay always supplies the metadata it decoded from the
     * CloudEvent, and the live path supplies whatever the source passed to {@link #accept(EventMetadata, Object)}.
     * <p>
     * {@code replayFilter} is also what {@link #acceptCloudEvent(CloudEvent)} matches live events against, for the reason
     * {@link #register(String, Function, Filter)} gives.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, BiFunction<EventMetadata, E, Mono<Void>> fold, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        claim(id, replayFilter, CatchupProjectionFeed.create(id, fold, replayFilter, reader, converter, eventId, catchupMarker, options));
    }

    // The projection feed is built by the caller before this runs, so a registration that fails validation (an
    // unpositioned reader, say) leaves the feed free rather than permanently taken by a projection that never
    // existed.
    private void claim(String id, Filter replayFilter, CatchupProjectionFeed<E> registering) {
        if (!feed.compareAndSet(null, new Registered<>(registering, replayFilter))) {
            Registered<E> existing = feed.get();
            throw new IllegalArgumentException(SingleConsumerMessages.singleConsumerOnly(
                    "DomainEventFeed", "projection", existing == null ? "<unknown>" : existing.catchupFeed().id(), id));
        }
    }

    /**
     * Whether a projection is registered on this feed, so a listener can ask before it feeds one rather than finding
     * out from {@link #accept(Object)}. The feed's answer to {@code RegisteringSubscribable.hasSubscriptions()} on the
     * push subscription model.
     */
    public boolean hasProjection() {
        return feed.get() != null;
    }

    /**
     * Feed a live domain event to the registered projection. The returned {@link Mono} completes once the projection
     * has handled it, so the listener can acknowledge after processing.
     * <p>
     * The returned {@link Mono} fails with an {@link IllegalStateException} when no projection is registered. Refused
     * rather than completed empty, because the listener acknowledges on completion and the broker discards what it
     * acknowledges, so completing would lose the event. See ADR 104.
     */
    public Mono<Void> accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        return registeredProjection().flatMap(registered -> registered.catchupFeed().accept(event));
    }

    /**
     * Feed a live domain event to the registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with {@link EventMetadata#empty()}.
     * <p>
     * Fails with an {@link IllegalStateException} when no projection is registered, for the reason
     * {@link #accept(Object)} gives.
     */
    public Mono<Void> accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        return registeredProjection().flatMap(registered -> registered.catchupFeed().accept(metadata, event));
    }

    /**
     * Feed a live event as a {@link CloudEvent} rather than an already-decoded domain event. This matches it against
     * the {@link Filter} {@link #register} was called with, decodes it with this feed's {@link CloudEventConverter}
     * only if it matches, delivers it, and completes with which of the three {@link RoutingOutcome}s happened. Call
     * this from a broker listener that has a CloudEvent to rebuild rather than a domain event and an
     * {@link EventMetadata} already in hand, and acknowledge on {@link RoutingOutcome#DELIVERED} once the returned
     * {@link Mono} completes, and on {@link RoutingOutcome#FILTERED}, where redelivering would loop forever since
     * the event is simply not this projection's. Named distinctly from {@link #accept(Object)} rather than
     * overloaded onto it, since a {@code DomainEventFeed<CloudEvent>} would otherwise let the compiler silently pick
     * between two overloads with different behavior for the same argument.
     * <p>
     * A non-matching event is never decoded, so a converter that only knows how to decode this projection's own
     * event types never sees one it was not built for. {@link EventMetadata} for a matched event comes from
     * {@link EventMetadata#from(CloudEvent)} on {@code cloudEvent} itself, the same way a replayed delivery's does.
     * <p>
     * This feed holds no filter of its own beyond the one {@link #register} was called with. The live match is
     * always evaluated against that same filter, so the replay and the live path can never disagree about which
     * events are this projection's. That matcher is built fresh from the registered {@link Filter} on every call
     * rather than at {@link #register}, so a {@code data} payload condition this feed has no {@link DataFieldReader}
     * for is refused here, the first time this method is called, rather than blocking {@link #register} for a
     * caller that never calls this method at all.
     * <p>
     * Completes with {@link RoutingOutcome#NOT_DELIVERABLE} rather than {@link RoutingOutcome#DELIVERED} when a
     * matching event arrives after {@link #stopCatchUp()} interrupted a replay still in flight. The catch-up-then-live
     * engine behind this feed drops such an event rather than delivering or buffering it, and this method reads that
     * signal back instead of assuming delivery from a normal completion.
     * <p>
     * Fails with an {@link IllegalStateException} when no projection is registered, for the reason
     * {@link #accept(Object)} gives, rather than completing with {@link RoutingOutcome#NOT_DELIVERABLE}. This feed,
     * unlike a push subscription model, has no write path to protect, and ADR 104 already refuses here for
     * {@link #accept(Object)} and {@link #accept(EventMetadata, Object)}.
     */
    public Mono<RoutingOutcome> acceptCloudEvent(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        return registeredProjection().flatMap(registered -> {
            Predicate<CloudEvent> liveMatcher = SubscriptionFilterMatcher.matcherFor(AgnosticSubscriptionFilter.filter(registered.replayFilter()), dataFieldReader);
            if (!liveMatcher.test(cloudEvent)) {
                return Mono.just(RoutingOutcome.FILTERED);
            }
            E event = converter.toDomainEvent(cloudEvent);
            return registered.catchupFeed().acceptReportingDelivery(EventMetadata.from(cloudEvent), event)
                    .map(delivered -> delivered ? RoutingOutcome.DELIVERED : RoutingOutcome.NOT_DELIVERABLE);
        });
    }

    // The one place the "nothing registered" refusal is spelled, so every accept overload and catchUpAll cannot
    // drift apart on it. Deferred, so a projection registered between assembling the returned Mono and subscribing to
    // it is still found, which is the lateness catchUp(String) and goLive(String) already have.
    private Mono<Registered<E>> registeredProjection() {
        return Mono.defer(() -> {
            Registered<E> registered = feed.get();
            return registered == null
                    ? Mono.error(new IllegalStateException(SingleConsumerMessages.noConsumerRegistered("DomainEventFeed", "projection")))
                    : Mono.just(registered);
        });
    }

    // Pairs a registration with the Filter it was registered with, so acceptCloudEvent always matches against the
    // one filter this feed was actually given. See the Filter-taking register(..) overloads' javadoc for why there
    // is only ever one filter here, and its own javadoc for why it is not turned into a matcher until then.
    private record Registered<E>(CatchupProjectionFeed<E> catchupFeed, Filter replayFilter) {
    }

    /**
     * Run the one-time catch-up of the registered projection. The returned {@link Mono} completes once it has caught
     * up and gone live, and fails with an {@link IllegalStateException} when nothing is registered. It used to complete
     * empty in that case, which meant a feed nobody registered on caught up "successfully" and then silently fed
     * nothing.
     * <p>
     * An error on the returned {@link Mono} is terminal for this feed, so let it reach the caller and do not start the
     * application. The projection rejects every later event afterwards. Unlike a subscription model, the feed does not
     * drop it: the application asked for this projection, so running on without it is worse than not running. Fix the
     * cause and build a new feed.
     * <p>
     * Named for when a feed could carry several projections. It carries one.
     */
    public Mono<Void> catchUpAll() {
        return registeredProjection().flatMap(registered -> registered.catchupFeed().catchUp());
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
        Registered<E> registered = feed.get();
        if (registered != null) {
            registered.catchupFeed().stopCatchUp();
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
            Registered<E> registered = feed.get();
            if (registered == null || !registered.catchupFeed().id().equals(id)) {
                return Mono.error(new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed."));
            }
            return registered.catchupFeed().catchUp();
        });
    }

    /**
     * Go live without a catch-up: skip the one-time replay for the single projection registered under {@code id}.
     * Use this instead of {@link #catchUp(String)} when this feed's events are not in the local event store, so
     * there is nothing to replay. No completion marker is recorded, so a later {@link #catchUp(String)} on the same
     * projection still replays the full history.
     * <p>
     * The lookup happens when the returned {@link Mono} is subscribed, the same as {@link #catchUp(String)}. An id
     * that matches nothing fails the {@link Mono} with an {@link IllegalArgumentException} rather than throwing here.
     *
     * @return A {@link Mono} that completes once that projection is live.
     */
    public Mono<Void> goLive(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        return Mono.defer(() -> {
            Registered<E> registered = feed.get();
            if (registered == null || !registered.catchupFeed().id().equals(id)) {
                return Mono.error(new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed."));
            }
            return registered.catchupFeed().goLive();
        });
    }
}
