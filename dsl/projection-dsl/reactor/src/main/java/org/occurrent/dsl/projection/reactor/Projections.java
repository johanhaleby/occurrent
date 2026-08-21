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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.MaterializedViewOptions;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionKeys;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Internal helpers shared by the reactor projection runners (the Kotlin {@code project} extensions and the Java
 * {@code Reactive*ProjectionRunner} facades): adapting a blocking view store into the reactive {@code (E) -> Mono<Void>}
 * update the runners consume. The plain-{@code Filter} derivation lives in
 * {@code org.occurrent.dsl.projection.internal.ProjectionFilters}, shared with the blocking stack.
 * <p>
 * There is no reactive {@link MaterializedView}/{@link ViewStateRepository} in the view DSL, so a genuinely reactive
 * store should be driven through a caller-supplied {@code (E) -> Mono<Void>} update (the runners' primitive overload).
 * The helpers here bridge the <em>blocking</em> view store into a reactive pipeline by scheduling its blocking work on
 * {@link Schedulers#boundedElastic()}. A reactive-store convenience (a reactive {@code ViewStateRepository}) is a
 * follow-up. Note that the {@code boundedElastic} bridge runs on a different thread than the writer, so it is <em>not</em>
 * suitable for synchronous, in-transaction (read-your-writes) dispatch. Use the reactive-primitive update there.
 */
@NullMarked
public final class Projections {

    private Projections() {
    }

    /**
     * A reactive {@code (E) -> Mono<Void>} update that loads, evolves, and saves the projection's view state through the
     * blocking {@code repository}, keyed by the projection's {@link Projection#id() id}, with the blocking work scheduled
     * on {@link Schedulers#boundedElastic()}. An event whose id resolves to {@code null} is skipped. This does a plain
     * read, fold, and save with no optimistic-locking retry of its own. A failed update is still retried by the
     * subscription model's retry strategy, which redelivers the event, but for concurrent writers to the same instance
     * supply a {@link MaterializedView} that re-reads and reapplies on conflict through
     * {@link #reactiveUpdate(MaterializedView)}.
     */
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> reactiveUpdate(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        BiFunction<EventMetadata, E, Mono<Void>> update = reactiveUpdateWithMetadata(projection, repository);
        return event -> update.apply(EventMetadata.empty(), event);
    }

    /**
     * The metadata-aware form of {@link #reactiveUpdate(Projection, ViewStateRepository)}: keys the view instance with
     * the projection's {@link Projection#idWithMetadata() metadata-aware id} and folds through
     * {@link View#evolve(Object, EventMetadata, Object)}, so a projection can be keyed by metadata such as the stream id.
     * <p>
     * During a catch-up replay this update coalesces its reads and writes per key rather than paying one of each per
     * event; see {@link MaterializedViewOptions} and
     * {@link #reactiveUpdateWithMetadata(Projection, ViewStateRepository, MaterializedViewOptions)} for the setting and
     * what it changes.
     */
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> reactiveUpdateWithMetadata(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return reactiveUpdateWithMetadata(projection, repository, MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #reactiveUpdateWithMetadata(Projection, ViewStateRepository)}, with explicit {@code options} for how
     * the update batches its store calls during a catch-up replay
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0110-a-replay-tells-the-view-where-it-begins-and-ends.md">ADR 110</a>).
     * <p>
     * During a replay this update buffers events per key instead of writing through immediately, and flushes the
     * buffer (read each buffered key's state once, fold its buffered events onto it in arrival order, write once)
     * whenever {@link MaterializedViewOptions#batchSize()} events have buffered across every key, and once more when
     * the replay finishes. The live path (no replay in progress) is never batched. Pass
     * {@code new MaterializedViewOptions(1)} to write through per event during a replay too, the same as before this
     * behaviour existed.
     */
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> reactiveUpdateWithMetadata(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, MaterializedViewOptions options) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        requireNonNull(options, "options cannot be null");
        BiFunction<EventMetadata, E, @Nullable ID> id = projection.idWithMetadata();
        requireNonNull(id, "projection is single-instance, pass a singletonKey: reactiveUpdate(projection, repository, singletonKey) or reactiveUpdateWithMetadata(projection, repository, singletonKey)");
        View<S, E> view = projection.view();
        BiFunction<EventMetadata, E, @Nullable ID> resolveId = (metadata, event) -> {
            @Nullable ID instanceId = id.apply(metadata, event);
            if (instanceId == null) {
                ProjectionKeys.failIfKeyNeededMetadata(projection.metadataKeyed(), metadata);
            }
            return instanceId;
        };
        return new CoalescingMaterializedUpdate<>(view, repository, options.batchSize(), resolveId);
    }

    /**
     * A reactive update for a keyed or single-instance projection. A single-instance projection (no id function) updates
     * one slot keyed by {@code singletonKey}, the projection's runtime identity (the subscription id).
     */
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> reactiveUpdate(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        BiFunction<EventMetadata, E, Mono<Void>> update = reactiveUpdateWithMetadata(projection, repository, singletonKey);
        return event -> update.apply(EventMetadata.empty(), event);
    }

    /**
     * The metadata-aware form of {@link #reactiveUpdate(Projection, ViewStateRepository, String)}.
     */
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> reactiveUpdateWithMetadata(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return reactiveUpdateWithMetadata(projection, repository, singletonKey, MaterializedViewOptions.defaults());
    }

    /**
     * As {@link #reactiveUpdateWithMetadata(Projection, ViewStateRepository, String)}, with explicit {@code options}
     * for how the update batches its store calls during a catch-up replay. See
     * {@link #reactiveUpdateWithMetadata(Projection, ViewStateRepository, MaterializedViewOptions)} for what batching
     * does.
     */
    @SuppressWarnings("unchecked")
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> reactiveUpdateWithMetadata(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey, MaterializedViewOptions options) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        requireNonNull(options, "options cannot be null");
        if (projection.id() != null) {
            return reactiveUpdateWithMetadata(projection, repository, options);
        }
        requireNonNull(singletonKey, "singletonKey cannot be null");
        View<S, E> view = projection.view();
        ID key = (ID) singletonKey; // a single-instance projection is Projection<S, E, String>
        BiFunction<EventMetadata, E, @Nullable ID> resolveId = (metadata, event) -> key;
        return new CoalescingMaterializedUpdate<>(view, repository, options.batchSize(), resolveId);
    }

    /**
     * A reactive {@code (E) -> Mono<Void>} update that calls the blocking {@code materializedView.update(event)} on
     * {@link Schedulers#boundedElastic()}. Use this to drive a blocking {@link MaterializedView} (for example the one the
     * view DSL's {@code materialized(...)} builds, with its own retry/locking policy) from a reactive pipeline. When
     * {@code materializedView} implements the blocking view DSL's replay-aware capability, the returned update forwards
     * the replay lifecycle to it too, so a batching view keeps batching instead of writing through per event. See
     * {@link #reactiveUpdateWithMetadata(MaterializedView)}.
     */
    public static <E> Function<E, Mono<Void>> reactiveUpdate(MaterializedView<E> materializedView) {
        BiFunction<EventMetadata, E, Mono<Void>> update = reactiveUpdateWithMetadata(materializedView);
        return event -> update.apply(EventMetadata.empty(), event);
    }

    /**
     * The metadata-aware form of {@link #reactiveUpdate(MaterializedView)}: calls the blocking
     * {@code materializedView.update(metadata, event)} on {@link Schedulers#boundedElastic()}. The returned update
     * always implements {@link ReactiveReplayAware}, and forwards
     * {@code replayStarted}/{@code replayCompleted}/{@code replayAbandoned} to {@code materializedView} whenever it
     * implements the blocking view DSL's {@code ReplayAware} capability, so a replay driven through a
     * {@code CatchupProjectionFeed} still reaches a batching view wrapped through this bridge.
     */
    public static <E> BiFunction<EventMetadata, E, Mono<Void>> reactiveUpdateWithMetadata(MaterializedView<E> materializedView) {
        requireNonNull(materializedView, "materializedView cannot be null");
        return new BlockingMaterializedViewUpdate<>(materializedView);
    }

    /**
     * A reactive {@code (EventMetadata, E) -> Mono<Void>} feed that folds a <strong>domain event</strong> straight into
     * the blocking {@code repository} for a keyed projection (on {@link Schedulers#boundedElastic()}), with no CloudEvent
     * conversion. Use it to drive a projection from a source that already hands you domain events (a RabbitMQ or Kafka
     * listener with its own message converter), so a live event is folded directly rather than round-tripped through
     * {@code toCloudEvent}/{@code toDomainEvent}. This is the live-tail feed only, with no catch-up.
     * <p>
     * Metadata-carrying on purpose, so a projection keyed on the stream id or position works on the live path too. Pass
     * {@link EventMetadata#empty()} when the source gives you only the event. The same shape as
     * {@link #reactiveUpdateWithMetadata(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return reactiveUpdateWithMetadata(projection, repository);
    }

    /**
     * A reactive domain-event feed for a keyed or single-instance projection, folding directly into {@code repository}
     * with no CloudEvent conversion. See {@link #domainEventFeed(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> BiFunction<EventMetadata, E, Mono<Void>> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return reactiveUpdateWithMetadata(projection, repository, singletonKey);
    }

    /**
     * Folds the events {@code projection} selects, read on demand, into its view state: the strongly-consistent,
     * query-driven counterpart to the subscription-fed {@code project(subscriptionId, projection, ...)} runners. The
     * returned {@link Mono} emits the folded state, or completes empty when that state is {@code null}. Only valid for
     * a single-instance (singleton) projection. A keyed projection errors, since folding every instance into one
     * blended state on demand would produce a nonsense result. Use {@link #project(Projection, DomainEventQueries, Object)}
     * with an {@code instanceId} for a keyed projection.
     */
    public static <S extends @Nullable Object, E, ID> Mono<S> project(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        if (projection.id() != null) {
            return Mono.error(new IllegalArgumentException("projection is keyed; folding every instance into one blended state on demand is not supported, "
                    + "use project(projection, queries, instanceId) to read a single instance, or a singleton projection for one shared state"));
        }
        Flux<E> events = selectEvents(projection, queries);
        return foldIncrementally(projection.view(), events);
    }

    /**
     * Folds the events {@code projection} selects for {@code instanceId}, read on demand, into that instance's view
     * state: the strongly-consistent, query-driven, single-instance counterpart to the unqualified
     * {@link #project(Projection, DomainEventQueries)}. Uses the same filter or handled event types as the unqualified
     * {@code project} to read candidate events, then keeps only the ones whose {@link Projection#id()} resolves to
     * {@code instanceId} before folding. A singleton projection (no id function) has a single instance regardless of
     * {@code instanceId}, so this folds all selected events, same as the unqualified {@code project}. The returned
     * {@link Mono} emits the folded state, or completes empty when that state is {@code null}.
     */
    public static <S extends @Nullable Object, E, ID> Mono<S> project(Projection<S, E, ID> projection, DomainEventQueries<E> queries, ID instanceId) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        requireNonNull(instanceId, "instanceId cannot be null");
        Flux<E> events = selectEvents(projection, queries);
        Function<E, @Nullable ID> id = projection.id();
        Flux<E> scopedEvents = id == null ? events : events.filter(event -> instanceId.equals(id.apply(event)));
        return foldIncrementally(projection.view(), scopedEvents);
    }

    // A record wrapper because Reactor's reduce cannot carry a null accumulator, but View.evolve is free to return a
    // null state (a projection can model "not yet created" or "deleted" as null).
    private record StateBox<S extends @Nullable Object>(S state) {
    }

    private static <S extends @Nullable Object, E> Mono<S> foldIncrementally(View<S, E> view, Flux<E> events) {
        return events.reduce(new StateBox<>(view.initialState()), (box, event) -> new StateBox<>(view.evolve(box.state(), event)))
                .flatMap(box -> Mono.justOrEmpty(box.state()));
    }

    private static <S extends @Nullable Object, E, ID> Flux<E> selectEvents(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
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
     * Folds the events matching {@code dcbProjection}'s DCB criteria, read on demand, into its view state: the
     * strongly-consistent, query-driven counterpart to the subscription-fed {@code ReactiveDcbProjectionRunner}, and the
     * shape of a single-instance DCB projection such as "is this username claimed?". The returned {@link Mono} emits the
     * folded state, or completes empty when that state is {@code null}. The criteria itself already scopes the read, so
     * there is no keyed/singleton ambiguity to guard against.
     */
    public static <S extends @Nullable Object, E, ID> Mono<S> project(DcbProjection<S, E, ID> dcbProjection, DcbDomainEventQueries<E> queries) {
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        return foldIncrementally(dcbProjection.projection().view(), queries.query(dcbProjection.criteria()));
    }

    /**
     * Wraps {@code update} so every live event it applies is also recorded into {@code store} as an applied append,
     * letting a caller later ask whether {@code projectionId} has applied a particular append
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
     * This is what {@code @Projection(recordAppliedAppends = true)} builds on the reactor stack; call it directly
     * when composing a projection programmatically instead of through the annotation.
     * <p>
     * The returned view is a {@link org.occurrent.dsl.projection.CatchupListener}, and nothing signals it unless you
     * arrange that. Register it on the subscription model this projection runs on, with
     * {@code ReplayAwareSubscriptions.listenForCatchup(projectionId, view)}, before subscribing. A model that
     * answers {@code false} there cannot say when its catch-ups begin and end, so poll it instead: call
     * {@link org.occurrent.dsl.projection.CatchupListener#catchupStarted(Object)} with a fresh object when
     * {@code isCatchingUp(projectionId)} turns true, and
     * {@link org.occurrent.dsl.projection.CatchupListener#historyRead(Object)} with the same object when it turns
     * false again. A view that is never signalled records straight through a replay and never clears, which is the
     * untruth this recording exists to prevent.
     * <p>
     * If {@code view} is itself {@link org.occurrent.dsl.view.ReplayAware}, wrap the delegate (not the result of
     * this call) with your own replay-aware behaviour first, since the returned view forwards to whatever
     * {@code view} was when this was called.
     * <p>
     * The Spring Boot starter's own scheduled poll (ADR 132 decision 7) is what retries a clear that keeps failing.
     * Calling this factory directly does not install it. Call
     * {@link AppliedAppendRecorder#pollForClear()} on the returned view on a schedule, or accept that a clear a
     * catch-up left owed only retries once another delivery reaches this projection.
     */    public static <E> RecordingReactiveUpdate<E> recordingAppliedAppends(BiFunction<EventMetadata, E, Mono<Void>> update, String projectionId, AppliedAppendStore store) {
        requireNonNull(update, "update cannot be null");
        requireNonNull(projectionId, "projectionId cannot be null");
        requireNonNull(store, "store cannot be null");
        return new RecordingReactiveUpdate<>(update, projectionId, store);
    }
}
