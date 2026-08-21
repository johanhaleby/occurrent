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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionKeys;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The public factory for assembling blocking projection runners (the Kotlin {@code project} extensions and the Java
 * {@code *ProjectionRunner} facades): turns a {@link Projection} plus a {@link ViewStateRepository} into a
 * {@link MaterializedView} that skips events whose id resolves to {@code null}. The plain-{@code Filter} derivation
 * lives in {@code org.occurrent.dsl.projection.internal.ProjectionFilters}, shared with the reactor stack.
 */
@NullMarked
public final class Projections {

    private Projections() {
    }

    /**
     * A {@link MaterializedView} for a keyed projection: loads, evolves, and saves the view state through
     * {@code repository}, keyed by the projection's {@link Projection#id() id}. An event whose id resolves to
     * {@code null} is skipped (no load or save), so a projection can safely see events that map to no keyed instance.
     * Throws if the projection is single-instance (it has no id function). Use
     * {@link #materializedView(Projection, ViewStateRepository, String)} with the single-instance key for those.
     * <p>
     * This does a plain read, fold, and save with no retry of its own, so two threads folding two events for the same
     * instance can both read the same state before either saves. What happens to the losing save then depends on the
     * store. A store with no conflict detection lets the second save overwrite the first with no signal at all. A store
     * that does detect the conflict (optimistic locking, a unique-key violation) throws instead, and that failure is
     * still a lost update on this overload, since nothing here retries it. That is reachable wherever a live push sink
     * is fed by more than one thread. Use {@link #materializedView(Projection, ViewStateRepository, RetryStrategy)} to
     * recover from it, and read what it says about what retry can and cannot fix.
     * <p>
     * During a catch-up replay this view coalesces its reads and writes per key rather than paying one of each per
     * event; see {@link MaterializedViewOptions} and {@link #materializedView(Projection, ViewStateRepository, RetryStrategy, MaterializedViewOptions)}
     * for the setting and what it changes.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return materializedView(projection, repository, RetryStrategy.none(), MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #materializedView(Projection, ViewStateRepository)}, retrying the whole read, fold, and save with
     * {@code retryStrategy} when it fails.
     * <p>
     * <strong>This recovers a lost update only when your store reports the conflict.</strong> A store that does
     * optimistic locking (a Spring Data {@code @Version} field, a conditional update, a unique index on an insert)
     * throws on the losing write, and the retry then re-reads the state the winner saved, folds the event onto it, and
     * saves again, so both transitions survive. A store that detects nothing never throws, so the second save
     * overwrites the first whatever strategy you pass here. Retry is the recovery, conflict detection in the store is
     * what makes it one.
     * <p>
     * Scope the strategy to the conflict, for example
     * {@code RetryStrategy.exponentialBackoff(..).maxAttempts(5).retryIf(OptimisticLockingFailureException.class::isInstance)}.
     * A strategy that retries everything also retries a fold that fails for a deterministic reason, which keeps the
     * failure away from the subscription model's error listener and from your broker's redelivery.
     * <p>
     * {@code retryStrategy} also reaches this view's catch-up replay flush. Passing anything other than
     * {@link RetryStrategy#none()} makes a flush re-read and re-write one key at a time instead of
     * {@link ViewStateRepository#findAllById(java.util.Collection)} and {@link ViewStateRepository#saveAll(java.util.Map)}
     * in one call each, because a repository that overrides those for a real bulk round trip reports no per-key outcome
     * to retry against. See {@link #materializedView(Projection, ViewStateRepository, RetryStrategy, MaterializedViewOptions)}
     * for the batching itself.
     * <p>
     * Hand the result to whichever sink you drive through its {@link MaterializedView} overload:
     * {@code CatchupProjectionFeed.create(id, view, replayFilter, ..)},
     * {@code DomainEventFeed.register(id, view, replayFilter)}, or
     * {@code project(subscriptionId, projection, view, startAt)}. On Kotlin with Spring Data MongoDB the view DSL's
     * {@code materialized(..)} builds an equivalent view with its own duplicate-key and optimistic-locking policy.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, RetryStrategy retryStrategy) {
        return materializedView(projection, repository, retryStrategy, MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #materializedView(Projection, ViewStateRepository, RetryStrategy)}, with explicit {@code options} for
     * how the view batches its store calls during a catch-up replay
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
     * <p>
     * During a replay this view buffers events per key instead of writing through immediately, and flushes the buffer
     * (read each buffered key's state once, fold its buffered events onto it in arrival order, write once) whenever
     * {@link MaterializedViewOptions#batchSize()} events have buffered across every key, and once more when the replay
     * finishes. The live path (no replay in progress) is never batched. Pass {@code new MaterializedViewOptions(1)} to
     * write through per event during a replay too, the same as before this behaviour existed.
     * <p>
     * {@code retryStrategy} still recovers a lost update the same way it does outside a replay, described above, but a
     * batch flush only retries the affected key rather than the batch it belongs to: passing a {@code retryStrategy}
     * other than {@link RetryStrategy#none()} makes a flush re-read and re-write one key at a time instead of using
     * {@link ViewStateRepository#findAllById(java.util.Collection)} and {@link ViewStateRepository#saveAll(Map)} in one
     * call each, because a repository that overrides those for a real bulk round trip reports no per-key outcome to
     * retry against.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, RetryStrategy retryStrategy, MaterializedViewOptions options) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        requireNonNull(retryStrategy, "retryStrategy cannot be null");
        requireNonNull(options, "options cannot be null");
        BiFunction<EventMetadata, E, @Nullable ID> id = projection.idWithMetadata();
        requireNonNull(id, "projection is single-instance; use materializedView(projection, repository, singletonKey)");
        View<S, E> view = projection.view();
        BiFunction<EventMetadata, E, @Nullable ID> resolveId = (metadata, event) -> {
            @Nullable ID instanceId = id.apply(metadata, event);
            if (instanceId == null) {
                ProjectionKeys.failIfKeyNeededMetadata(projection.metadataKeyed(), metadata);
            }
            return instanceId;
        };
        return new CoalescingMaterializedView<>(view, repository, retryStrategy, options.batchSize(), resolveId);
    }

    /**
     * A {@link MaterializedView} for a keyed or single-instance projection. A keyed projection behaves like
     * {@link #materializedView(Projection, ViewStateRepository)}. A single-instance projection (no id function) updates
     * one slot keyed by {@code singletonKey}, the projection's runtime identity (the subscription id, or the
     * {@code @Projection} id).
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return materializedView(projection, repository, singletonKey, RetryStrategy.none(), MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #materializedView(Projection, ViewStateRepository, String)}, retrying the whole read, fold, and save
     * with {@code retryStrategy} when it fails. See
     * {@link #materializedView(Projection, ViewStateRepository, RetryStrategy)} for what retry does and does not
     * recover.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey, RetryStrategy retryStrategy) {
        return materializedView(projection, repository, singletonKey, retryStrategy, MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #materializedView(Projection, ViewStateRepository, String, RetryStrategy)}, with explicit
     * {@code options} for how the view batches its store calls during a catch-up replay. See
     * {@link #materializedView(Projection, ViewStateRepository, RetryStrategy, MaterializedViewOptions)} for what
     * batching does and how it interacts with {@code retryStrategy}.
     */
    @SuppressWarnings("unchecked")
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey, RetryStrategy retryStrategy, MaterializedViewOptions options) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        requireNonNull(retryStrategy, "retryStrategy cannot be null");
        requireNonNull(options, "options cannot be null");
        if (projection.id() != null) {
            return materializedView(projection, repository, retryStrategy, options);
        }
        requireNonNull(singletonKey, "singletonKey cannot be null");
        View<S, E> view = projection.view();
        ID key = (ID) singletonKey; // a single-instance projection is Projection<S, E, String>
        BiFunction<EventMetadata, E, @Nullable ID> resolveId = (metadata, event) -> key;
        return new CoalescingMaterializedView<>(view, repository, retryStrategy, options.batchSize(), resolveId);
    }

    /**
     * A sink that folds a <strong>domain event</strong> straight into {@code repository} for a keyed projection, with no
     * CloudEvent conversion. Use it to drive a projection from a source that already hands you domain events (a RabbitMQ
     * or Kafka listener with its own message converter), so a live event is folded directly rather than round-tripped
     * through {@code toCloudEvent}/{@code toDomainEvent}. An event whose id resolves to {@code null} is skipped, and the
     * fold no-ops on an event type the projection does not handle. This is the live-tail feed only, with no catch-up:
     * backfill a new or rebuilt projection from the event store first.
     * <p>
     * Returns the {@link MaterializedView} rather than a bare {@code Consumer<E>} so the metadata channel survives. Call
     * {@link MaterializedView#update(Object)} when the source gives you only the event, and
     * {@link MaterializedView#update(EventMetadata, Object)} when it also gives you the stream id, version or position,
     * which is what a projection keyed on metadata needs to work on the live path.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return materializedView(projection, repository);
    }

    /**
     * As {@link #domainEventFeed(Projection, ViewStateRepository)}, retrying the whole read, fold, and save with
     * {@code retryStrategy} when it fails. See
     * {@link #materializedView(Projection, ViewStateRepository, RetryStrategy)} for what retry does and does not
     * recover.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, RetryStrategy retryStrategy) {
        return materializedView(projection, repository, retryStrategy);
    }

    /**
     * A domain-event feed for a keyed or single-instance projection, folding directly into {@code repository} with no
     * CloudEvent conversion. A single-instance projection (no id function) updates one slot keyed by
     * {@code singletonKey}. See {@link #domainEventFeed(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return materializedView(projection, repository, singletonKey);
    }

    /**
     * As {@link #domainEventFeed(Projection, ViewStateRepository, String)}, retrying the whole read, fold, and save
     * with {@code retryStrategy} when it fails. See
     * {@link #materializedView(Projection, ViewStateRepository, RetryStrategy)} for what retry does and does not
     * recover.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey, RetryStrategy retryStrategy) {
        return materializedView(projection, repository, singletonKey, retryStrategy);
    }

    /**
     * Folds the events {@code projection} selects, read on demand, into its view state and returns it: the
     * strongly-consistent, query-driven counterpart to the subscription-fed {@code project(subscriptionId, projection,
     * ...)} runners. Uses the projection's explicit {@link Projection#filter() filter} if set, else its handled
     * {@link Projection#eventTypes() event types} (empty means "all events"). Only valid for a single-instance
     * (singleton) projection. A keyed projection throws, since folding every instance into one blended state on demand
     * would produce a nonsense result. Use {@link #project(Projection, DomainEventQueries, Object)} with an
     * {@code instanceId} for a keyed projection.
     */
    public static <S extends @Nullable Object, E, ID> S project(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        if (projection.id() != null) {
            throw new IllegalArgumentException("projection is keyed; folding every instance into one blended state on demand is not supported, "
                    + "use project(projection, queries, instanceId) to read a single instance, or a singleton projection for one shared state");
        }
        Stream<E> events = selectEvents(projection, queries);
        return projection.view().evolve(events);
    }

    /**
     * Folds the events {@code projection} selects for {@code instanceId}, read on demand, into that instance's view
     * state and returns it: the strongly-consistent, query-driven, single-instance counterpart to the unqualified
     * {@link #project(Projection, DomainEventQueries)}. Uses the same filter or handled event types as the unqualified
     * {@code project} to read candidate events, then keeps only the ones whose {@link Projection#id()} resolves to
     * {@code instanceId} before folding. A singleton projection (no id function) has a single instance regardless of
     * {@code instanceId}, so this folds all selected events, same as the unqualified {@code project}.
     */
    public static <S extends @Nullable Object, E, ID> S project(Projection<S, E, ID> projection, DomainEventQueries<E> queries, ID instanceId) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        requireNonNull(instanceId, "instanceId cannot be null");
        Stream<E> events = selectEvents(projection, queries);
        Function<E, @Nullable ID> id = projection.id();
        Stream<E> scopedEvents = id == null ? events : events.filter(event -> instanceId.equals(id.apply(event)));
        return projection.view().evolve(scopedEvents);
    }

    private static <S extends @Nullable Object, E, ID> Stream<E> selectEvents(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
        Filter explicitFilter = projection.filter();
        if (explicitFilter != null) {
            return queries.query(explicitFilter);
        } else if (projection.eventTypes().isEmpty()) {
            return queries.all();
        } else {
            return queries.query(projection.eventTypes());
        }
    }

    /**
     * Folds the events matching {@code dcbProjection}'s DCB criteria, read on demand, into its view state and returns
     * it: the strongly-consistent, query-driven counterpart to the subscription-fed {@code DcbProjectionRunner}. This is
     * the shape of a single-instance DCB projection such as "is this username claimed?", where the criteria itself
     * already scopes the read, so there is no keyed/singleton ambiguity to guard against.
     */
    public static <S extends @Nullable Object, E, ID> S project(DcbProjection<S, E, ID> dcbProjection, DcbDomainEventQueries<E> queries) {
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        return dcbProjection.projection().view().evolve(queries.query(dcbProjection.criteria()));
    }

    /**
     * Wraps {@code view} so every live update it applies is also recorded into {@code store} as an applied append,
     * letting a caller later ask whether {@code projectionId} has applied a particular append
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
     * This is what {@code @Projection(recordAppliedAppends = true)} builds on the blocking stack; call it directly
     * when composing a projection programmatically instead of through the annotation.
     * <p>
     * If {@code view} is itself {@link org.occurrent.dsl.view.ReplayAware}, wrap the delegate (not the result of
     * this call) with your own replay-aware behaviour first, since the returned view forwards to whatever
     * {@code view} was when this was called.
     * <p>
     * The returned view is a {@link CatchupListener}, and nothing signals it unless you arrange that. Register it
     * on the subscription model this projection runs on, before subscribing.
     * <pre>{@code
     * RecordingMaterializedView<MyEvent> recording = Projections.recordingAppliedAppends(view, projectionId, store);
     * ReplayAwareSubscriptions.findIn(subscriptionModel)
     *         .ifPresent(model -> model.listenForCatchup(projectionId, recording));
     * }</pre>
     * <p>
     * {@code listenForCatchup} answers {@code false} for a model that cannot say when its catch-ups begin and end.
     * Poll that one instead: call {@link CatchupListener#catchupStarted(Object)} with a fresh object when
     * {@code isCatchingUp(projectionId)} turns true, and {@link CatchupListener#historyRead(Object)} with that same
     * object when it turns false again. A view that is never signalled records straight through a replay and never
     * clears, which is the untruth this recording exists to prevent.
     * <p>
     * The Spring Boot starter's own scheduled poll (ADR 132 decision 7) is what retries a clear that keeps failing.
     * Calling this factory directly does not install it. Call {@link AppliedAppendRecorder#pollForClear()} on the
     * returned view on a schedule, or accept that a clear a catch-up left owed only retries once another delivery
     * reaches this projection.
     */
    public static <E> RecordingMaterializedView<E> recordingAppliedAppends(MaterializedView<E> view, String projectionId, AppliedAppendStore store) {
        requireNonNull(view, "view cannot be null");
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(store, "store cannot be null");
        return new RecordingMaterializedView<>(view, projectionId, store);
    }
}
