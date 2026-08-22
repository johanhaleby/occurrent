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
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.UnreadableLiveFilterException;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.internal.SingleConsumerMessages;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The domain-event twin of {@code PushSubscriptionModel}: a register-only sink the application owns and feeds with
 * <strong>domain events</strong>, giving one projection a catch-up and then a live feed. It lets an external source
 * (a RabbitMQ or Kafka listener with its own message converter) drive a projection without any CloudEvent conversion
 * on the live path, through {@link #accept(Object)} and {@link #accept(EventMetadata, Object)}.
 * {@link #acceptCloudEvent(CloudEvent)} is the one exception, for a listener that has a {@link CloudEvent} to
 * rebuild rather than an already-decoded domain event.
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
    // The one projection registered on this feed, or null while it is free. Cleared by nothing today: a feed has no
    // unregister, so this is only ever set once in practice, but reading it is what names the collision. Paired with
    // the Filter it was registered with, so acceptCloudEvent always reads the two together.
    private final AtomicReference<@Nullable Registered<E>> feed = new AtomicReference<>();

    /**
     * @param reader          The store read used to replay history during the projection's catch-up.
     * @param converter       Decodes replayed CloudEvents to domain events. {@link #accept(Object)} and
     *                        {@link #accept(EventMetadata, Object)} never use it on the live path, unlike
     *                        {@link #acceptCloudEvent(CloudEvent)}, which does.
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
        this(reader, converter, eventId, catchupMarker, options, DataFieldReader.refusing());
    }

    /**
     * As {@link #DomainEventFeed(PositionOrderedReader, CloudEventConverter, Function, CheckpointStorage, CatchupThenLiveOptions)},
     * additionally answering a {@code data} payload condition on the replay filter given to {@link #register} by
     * reading it through {@code dataFieldReader} instead of refusing it. Only {@link #acceptCloudEvent(CloudEvent)} consults
     * this, since that is the one entry point that evaluates the filter live rather than only during the replay.
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
     * <p>
     * {@code replayFilter} is also what {@link #acceptCloudEvent(CloudEvent)} matches live events against, wrapped as an
     * {@link AgnosticSubscriptionFilter}. The one filter given here is the only one this feed ever holds, so the
     * replay and the live path can never disagree about which events are this projection's. This does not build that
     * matcher, or otherwise change what {@code replayFilter} was already accepted for before {@link #acceptCloudEvent}
     * existed. The store still evaluates it during the replay however it always has, including a {@code data} payload
     * condition this feed has no {@link DataFieldReader} for, since that evaluation has nothing to do with this
     * feed's own {@link DataFieldReader}. {@link #acceptCloudEvent} is what needs one, and only refuses such a filter
     * there, the first time it is called, permanently, so a caller that never touches the live CloudEvent path keeps
     * registering exactly the filters it always could.
     *
     * @throws IllegalArgumentException if a projection is already registered on this feed
     */
    public void register(String id, MaterializedView<E> view, Filter replayFilter) {
        Objects.requireNonNull(id, "id cannot be null");
        // Built before the slot is claimed, so a registration that fails validation (an unpositioned reader, say)
        // leaves the feed free rather than permanently taken by a projection that never existed.
        CatchupProjectionFeed<E> registering = CatchupProjectionFeed.create(id, view, replayFilter, reader, converter, eventId, catchupMarker, options);
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
     * Whether the registered projection can safely be fed a live event right now, so a listener can gate its own
     * consumption on more than just {@link #hasProjection()}. Delegates to the underlying catch-up-then-live
     * handover rather than tracking this separately, so it is {@code true} only once
     * {@link #catchUpAll()}/{@link #catchUp(String)} or {@link #goLive(String)} has actually reached live,
     * {@code false} while either is still replaying or buffering ahead of its own drain, and {@code false} forever
     * once either has thrown, since that failure is permanent and a later call reaching live does not clear it. A
     * live event fed through {@link #acceptCloudEvent(CloudEvent)} while this answers {@code false} is never
     * buffered: it is refused outright, and {@link #acceptCloudEvent(CloudEvent)}'s own javadoc covers what it
     * reports for that, and for a permanently failed catch-up. {@code false} for an unregistered feed, the same as
     * {@link #hasProjection()} rather than the {@link IllegalStateException} {@link #accept(Object)} throws, so a
     * listener can check both together before it has anything registered at all.
     */
    public boolean isReadyForLiveDelivery() {
        Registered<E> registered = feed.get();
        return registered != null && registered.catchupFeed().isReadyForLiveDelivery();
    }

    /**
     * Whether the registered projection's catch-up has permanently failed, so every later
     * {@link #acceptCloudEvent(CloudEvent)} on this registration refuses the event and will go on refusing.
     * {@code false} until a {@link #catchUpAll()}, {@link #catchUp(String)} or {@link #goLive(String)} attempt has
     * thrown, and never {@code false} again after that, since the failure it records is never cleared.
     * <p>
     * Distinct from {@link #isReadyForLiveDelivery()}, which is also {@code false} while a replay that is going to
     * succeed is still running. A listener deciding whether to stop consuming for good needs to tell those two
     * apart, and because this only ever goes from {@code false} to {@code true} it is safe to read after catching
     * a refusal rather than at the moment the refusal was thrown.
     * <p>
     * A refusal that escaped a handler which reached into some other projection feed or subscription model leaves
     * this {@code false}, which is what lets a listener tell its own feed's permanent refusal from one that is not
     * its own. {@code false} for an unregistered feed, the same as {@link #isReadyForLiveDelivery()}, so a listener
     * can ask both together before it has anything registered at all.
     */
    public boolean refusesPermanently() {
        Registered<E> registered = feed.get();
        return registered != null && registered.catchupFeed().refusesPermanently();
    }

    /**
     * Feed a live domain event to the registered projection, on the calling thread. Call this from the broker
     * listener, acknowledging the message only once it returns. An exception from the projection propagates.
     *
     * @throws IllegalStateException if no projection is registered on this feed. Refused rather than accepted,
     *                               because the listener acknowledges once this returns and the broker discards what
     *                               it acknowledges, so returning normally would lose the event. See ADR 104.
     */
    public void accept(E event) {
        Objects.requireNonNull(event, "event cannot be null");
        registeredProjection().catchupFeed().accept(event);
    }

    /**
     * Feed a live domain event to the registered projection together with the {@link EventMetadata} the source knows
     * about it, so a projection keyed on the stream id, version or position works on the live path and not only during
     * the catch-up replay. Use this when the broker message carries those values and your listener can read them.
     * Otherwise call {@link #accept(Object)}, which folds with no metadata.
     *
     * @throws IllegalStateException if no projection is registered on this feed, for the reason
     *                               {@link #accept(Object)} gives.
     */
    public void accept(EventMetadata metadata, E event) {
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(event, "event cannot be null");
        registeredProjection().catchupFeed().accept(metadata, event);
    }

    /**
     * Feed a live event as a {@link CloudEvent} rather than an already-decoded domain event. This matches it against
     * the {@link Filter} {@link #register} was called with, decodes it with this feed's {@link CloudEventConverter}
     * only if it matches, delivers it, and reports which {@link RoutingOutcome} happened. Call this from a broker
     * listener that has a CloudEvent to rebuild rather than a domain event and an {@link EventMetadata} already in
     * hand. Acknowledge on {@link RoutingOutcome#DELIVERED}, once this has returned normally, and on
     * {@link RoutingOutcome#FILTERED}, where redelivering would loop forever against this same registration, since
     * the event is not this projection's under the {@link Filter} currently registered. Redeliver instead on
     * {@link RoutingOutcome#DEFERRED}, safe arbitrarily many times, and never acknowledge it. Named distinctly from
     * {@link #accept(Object)} rather than overloaded
     * onto it, since a {@code DomainEventFeed<CloudEvent>} would otherwise let the compiler silently pick between
     * two overloads with different behavior for the same argument.
     * <p>
     * A non-matching event is never decoded, so a converter that only knows how to decode this projection's own
     * event types never sees one it was not built for. {@link EventMetadata} for a matched event comes from
     * {@link EventMetadata#from(CloudEvent)} on {@code cloudEvent} itself, the same way a replayed delivery's does.
     * <p>
     * This feed holds no filter of its own beyond the one {@link #register} was called with. The live match is
     * always evaluated against that same filter, so the replay and the live path can never disagree about which
     * events are this projection's. That matcher is built from the registered {@link Filter} the first time this
     * method is called rather than at {@link #register}, so a {@code data} payload condition this feed has no
     * {@link DataFieldReader} for is refused here instead of blocking {@link #register} for a caller that never
     * calls this method at all.
     * <p>
     * Whichever of the two this first call produces, a working matcher or the refusal below, is then cached and
     * reused for every call after that against the same registration, rather than rebuilt each time.
     * <p>
     * Reports {@link RoutingOutcome#DEFERRED} rather than {@link RoutingOutcome#DELIVERED} for a matching event
     * that only reaches the buffer, not the view, whatever the reason. One evaluation decides both the live check
     * and the accept together, under the same lock the underlying handover already holds for that decision, so
     * there is no window between "is it live" and "hand it over" for a concurrent {@link #goLive(String)} to land
     * in. That covers an event fed before {@link #catchUpAll()}/{@link #catchUp(String)} or
     * {@link #goLive(String)} has been called at all, one fed while a replay is still running and buffering ahead
     * of its own drain, and one fed after {@link #stopCatchUp()} interrupted a replay in flight. Redelivering a
     * {@link RoutingOutcome#DEFERRED} event is always safe: this feed's own de-dup key absorbs a repeat that
     * already landed, and a caller with nowhere else to redeliver from, {@link #goLive(String)} exists precisely
     * for a registration whose events are not in the local store, gets the same outcome either way, since this
     * feed has no way to know in advance which kind of registration it is fielding an event for.
     *
     * @throws IllegalStateException if no projection is registered on this feed, for the reason
     *                               {@link #accept(Object)} gives. Refused rather than reported as
     *                               {@link RoutingOutcome#NOT_DELIVERABLE}, since this feed, unlike a push
     *                               subscription model, has no write path to protect and ADR 104 already refuses
     *                               here for {@link #accept(Object)} and {@link #accept(EventMetadata, Object)}.
     *                               Also thrown, unwrapped, as {@code BlockingHandover.PreDispatchRefusalException}
     *                               (an {@code IllegalStateException} subtype, in
     *                               {@code org.occurrent.subscription.api.blocking.internal}) once this
     *                               registration's catch-up-then-live handover has permanently failed. That
     *                               failure never clears, so every later call on the same registration throws the
     *                               same way.
     * @throws UnreadableLiveFilterException the first time this is called on a registration whose {@link Filter}
     *                               references a {@code data} field this feed's {@link DataFieldReader} cannot
     *                               read, and again with the exact same exception instance on every later call on
     *                               that registration. This is a permanent configuration error, not a transient
     *                               one, a caller catching it must stop or park the registration rather than
     *                               retry, and must never acknowledge and redeliver the event that triggered it
     *                               expecting a different answer. Register a new {@code DomainEventFeed} with a
     *                               {@link Filter} that does not reference the field, or with a
     *                               {@link DataFieldReader} that can read it.
     * @throws RuntimeException whatever {@link CloudEventConverter#toDomainEvent(CloudEvent)} throws for a matching
     *                               event it cannot decode, uncaught. Reachable for a filter no more selective than
     *                               the event type, {@link Filter#all()} in particular, against a source carrying
     *                               event types this feed's converter was never built to decode.
     */
    public RoutingOutcome acceptCloudEvent(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Registered<E> registered = registeredProjection();
        LiveMatcher liveMatcher = liveMatcherFor(registered);
        if (liveMatcher.refusal() != null) {
            throw liveMatcher.refusal();
        }
        if (!liveMatcher.matcher().test(cloudEvent)) {
            return RoutingOutcome.FILTERED;
        }
        E event = converter.toDomainEvent(cloudEvent);
        CatchupProjectionFeed<E> catchupFeed = registered.catchupFeed();
        boolean delivered = catchupFeed.acceptIfLive(EventMetadata.from(cloudEvent), event);
        return delivered ? RoutingOutcome.DELIVERED : RoutingOutcome.DEFERRED;
    }

    // The one place the "nothing registered" refusal is spelled, so every accept overload and catchUpAll cannot
    // drift apart on it.
    private Registered<E> registeredProjection() {
        Registered<E> registered = feed.get();
        if (registered == null) {
            throw new IllegalStateException(SingleConsumerMessages.noConsumerRegistered("DomainEventFeed", "projection"));
        }
        return registered;
    }

    // Builds the live matcher for a registration on the first acceptCloudEvent call, then reuses it, whether that
    // build succeeded or refused. A benign race between two concurrent first calls can build it twice, but
    // matcherFor is pure for the same Filter and DataFieldReader, so the loser's copy, success or refusal, is just
    // discarded work: every caller ends up with the same winner, which is what makes a cached refusal always the
    // same exception instance rather than a fresh one built per call.
    private LiveMatcher liveMatcherFor(Registered<E> registered) {
        LiveMatcher cached = registered.liveMatcher.get();
        if (cached != null) {
            return cached;
        }
        LiveMatcher built;
        try {
            built = LiveMatcher.matched(SubscriptionFilterMatcher.matcherFor(AgnosticSubscriptionFilter.filter(registered.replayFilter), dataFieldReader));
        } catch (UnsupportedOperationException e) {
            String id = registered.catchupFeed().id();
            built = LiveMatcher.refused(new UnreadableLiveFilterException("The Filter registered for '" + id
                    + "' references a data field this feed's DataFieldReader cannot read, so its live CloudEvent "
                    + "match can never succeed. Register with a Filter that does not reference a data field, or "
                    + "build this feed with a DataFieldReader that can read it.", e));
        }
        registered.liveMatcher.compareAndSet(null, built);
        return registered.liveMatcher.get();
    }

    // The outcome of the first acceptCloudEvent call for one registration: either a working matcher, or the
    // permanent refusal every later call on that same registration replays instead of rebuilding.
    private static final class LiveMatcher {
        private final @Nullable Predicate<CloudEvent> matcher;
        private final @Nullable UnreadableLiveFilterException refusal;

        private LiveMatcher(@Nullable Predicate<CloudEvent> matcher, @Nullable UnreadableLiveFilterException refusal) {
            this.matcher = matcher;
            this.refusal = refusal;
        }

        private static LiveMatcher matched(Predicate<CloudEvent> matcher) {
            return new LiveMatcher(matcher, null);
        }

        private static LiveMatcher refused(UnreadableLiveFilterException refusal) {
            return new LiveMatcher(null, refusal);
        }

        private Predicate<CloudEvent> matcher() {
            return matcher;
        }

        private @Nullable UnreadableLiveFilterException refusal() {
            return refusal;
        }
    }

    // Pairs a registration with the Filter it was registered with, so acceptCloudEvent always matches against the
    // one filter this feed was actually given. See the register(String, MaterializedView, Filter) javadoc for why
    // there is only ever one filter here, and its own javadoc for why it is not turned into a matcher until then.
    // liveMatcher caches the outcome of that first build, one cache per registration since a new register() call
    // creates a new Registered with a fresh, empty cache.
    private static final class Registered<E> {
        private final CatchupProjectionFeed<E> catchupFeed;
        private final Filter replayFilter;
        private final AtomicReference<@Nullable LiveMatcher> liveMatcher = new AtomicReference<>();

        private Registered(CatchupProjectionFeed<E> catchupFeed, Filter replayFilter) {
            this.catchupFeed = catchupFeed;
            this.replayFilter = replayFilter;
        }

        private CatchupProjectionFeed<E> catchupFeed() {
            return catchupFeed;
        }
    }

    /**
     * Run the one-time catch-up of the registered projection (replay history, then go live). Call once, after the
     * projection is registered and the live feed is wired.
     * <p>
     * A failure here is terminal for this feed, so let it reach the caller and do not start the application. The
     * projection rejects every later event afterwards. Unlike a subscription model, the feed does not drop it: the
     * application asked for this projection, so running on without it is worse than not running. Fix the cause and
     * build a new feed.
     * <p>
     * Named for when a feed could carry several projections. It carries one, so this and {@link #catchUp(String)} do
     * the same thing whenever the id matches.
     *
     * @throws IllegalStateException if no projection is registered on this feed. It used to be a no-op, which meant a
     *                               feed nobody registered on caught up "successfully" and then silently fed nothing.
     */
    public void catchUpAll() {
        registeredProjection().catchupFeed().catchUp();
    }

    /**
     * Run the one-time catch-up of the projection registered under {@code id}. Use this over {@link #catchUpAll()}
     * when the caller knows which projection it means and wants a mismatch to fail rather than pass silently.
     *
     * @throws IllegalArgumentException if no projection with that id is registered on this feed
     */
    public void catchUp(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        Registered<E> registered = feed.get();
        if (registered == null || !registered.catchupFeed().id().equals(id)) {
            throw new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed.");
        }
        registered.catchupFeed().catchUp();
    }

    /**
     * Go live without a catch-up: skip straight past the one-time replay for the projection registered under
     * {@code id}, draining whatever live events it already buffered. Use this over {@link #catchUp(String)} when this
     * feed's events are not in the local event store, so there is nothing to replay. No completion marker is written,
     * so a later {@link #catchUp(String)} on the same projection still replays the full history.
     *
     * @throws IllegalArgumentException if no projection with that id is registered on this feed
     */
    public void goLive(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        Registered<E> registered = feed.get();
        if (registered == null || !registered.catchupFeed().id().equals(id)) {
            throw new IllegalArgumentException("No projection with id '" + id + "' is registered on this feed.");
        }
        registered.catchupFeed().goLive();
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
        Registered<E> registered = feed.get();
        if (registered != null) {
            registered.catchupFeed().stopCatchUp();
        }
    }
}
