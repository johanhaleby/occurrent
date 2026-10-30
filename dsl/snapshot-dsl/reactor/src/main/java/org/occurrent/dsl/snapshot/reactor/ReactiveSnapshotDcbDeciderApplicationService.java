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

package org.occurrent.dsl.snapshot.reactor;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbExecuteOptions;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.internal.SkippedSaves;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The DCB counterpart to {@link ReactiveSnapshotDeciderApplicationService}: runs a {@link DcbDecider} but resumes from a
 * snapshot instead of folding the whole DCB boundary. Construct it once around a reactive {@link DcbApplicationService}
 * and reuse it for every aggregate. Each execute takes a {@link ReactiveSnapshotDcbDecider}, the per-aggregate spec that
 * bundles the decider with its {@link ReactiveSnapshotStore}, {@link SnapshotOptions}, and snapshot-key function.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is a canonical,
 * order-insensitive rendering of the {@link DcbCriteria} that {@link DcbDecider#criteriaFor(List)} resolves for the
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}), the spec's key function overrides it. The snapshot is
 * saved at the marker versions the append reported ({@link DcbAppendResult#markerVersions()}), and the next execute
 * reads only the events appended after them. The resume read still captures the whole boundary's consistency token, so
 * the append condition is unaffected. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 * <p>
 * The snapshot also keeps the position of the append. An event that reserved a position below it can commit after the
 * snapshot was saved, and folding that event last could give another state than a read in position order, so the
 * execute treats it as a refused read.
 * <p>
 * A snapshot is saved only when the store reported marker versions for the append and confirms that the append is
 * committed ({@link DcbApplicationService#isAppendConfirmed(DcbAppendResult)}). Otherwise the save is skipped, and the
 * first skip for each reason is logged as a warning. When the store refuses to read after the saved versions, for
 * example because a marker was pruned, the execute reads the whole boundary from the initial state instead. A snapshot
 * that 0.34 saved at a DCB position is never used, so after an upgrade an execute reads the whole boundary until one
 * can save over it.
 * <p>
 * An empty result Mono from {@link #execute} means the domain function produced no new events, so nothing is appended and
 * no snapshot is written. {@link #executeAndReturnDecision} still emits the decided state for a no-op, nullable or not.
 * {@link #executeAndReturnState} emits it too, whether or not anything was appended, except a null state, which errors
 * before anything is appended.
 * <p>
 * Deliberate asymmetry with the stream executor: this executor only advances the base when the decision actually
 * appended events, since a no-op decision has no {@link DcbAppendResult} to key the save on.
 * {@link ReactiveSnapshotDeciderApplicationService} instead saves unconditionally after every execute, because a
 * stream write always has a {@code WriteResult} to advance the base from, whether or not new events were appended.
 * Either way a missed save only costs a longer replay on the next execute. It is never a correctness issue.
 *
 * @param <E> the event type
 */
@NullMarked
public final class ReactiveSnapshotDcbDeciderApplicationService<E> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSnapshotDcbDeciderApplicationService.class);

    private final DcbApplicationService<E> applicationService;
    private final SkippedSaves skippedSaves = new SkippedSaves(log);

    public ReactiveSnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(C command, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return execute(List.of(command), snapshotDcbDecider);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider).flatMap(executed -> Mono.justOrEmpty(executed.appendResult()));
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(C command, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(List.of(command), snapshotDcbDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider).map(Executed::decision);
    }

    /**
     * Execute a single command and emit the folded state after the decision (even when nothing was appended). The state type
     * {@code S} may be nullable, but a {@link Mono} cannot carry a null value, so a decider that folds to null is refused
     * with a {@link NullPointerException} before anything is written. The blocking
     * {@code org.occurrent.dsl.snapshot.blocking.SnapshotDcbDeciderApplicationService} returns a null state instead. Use
     * {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(C command, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecuteAndReturnState(List.of(command), snapshotDcbDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision (even when nothing was appended). The
     * commands are decided as one unit and appended once, so only the state after the last command is checked. If that
     * state is null, nothing is written, not even the events the earlier commands decided. See
     * {@link #executeAndReturnState(Object, ReactiveSnapshotDcbDecider)}.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecuteAndReturnState(commands, snapshotDcbDecider);
    }

    private <C, S extends @Nullable Object> Mono<@NonNull S> doExecuteAndReturnState(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider, true).map(executed -> executed.decision().state());
    }

    // A Mono cannot carry null, so a null folded state fails fast with guidance instead of a bare NPE from Reactor.
    private static <S, E> S requireNonNullState(Decider.Decision<S, E> decision) {
        return Objects.requireNonNull(decision.state(), "The decider produced a null state, but a Mono cannot carry null. Use executeAndReturnDecision for a nullable state.");
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(C command, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(command, snapshotDcbDecider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(commands, snapshotDcbDecider).map(Decider.Decision::events);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider, false);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider, boolean refuseNullState) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(snapshotDcbDecider, "snapshotDcbDecider cannot be null");

        return Mono.defer(() -> {
            DcbDecider<C, S, E> dcbDecider = snapshotDcbDecider.dcbDecider();
            ReactiveSnapshotStore<S> store = snapshotDcbDecider.store();
            SnapshotOptions<S, E> options = snapshotDcbDecider.options();
            Function<DcbCriteria, String> keyFunction = snapshotDcbDecider.keyFunction();

            DcbCriteria criteria = dcbDecider.criteriaFor(commands);
            String key = Objects.requireNonNull(keyFunction.apply(criteria), "snapshot key cannot be null");
            Decider<C, S, E> decider = dcbDecider.decider();

            return ReactiveSnapshotSupport.resolveDcbBase(store, key, options.schemaVersion(), decider::initialState)
                    .flatMap(base -> {
                        AtomicBoolean decided = new AtomicBoolean();
                        return executeFrom(base, criteria, commands, dcbDecider, refuseNullState, decided)
                            .onErrorResume(DcbSnapshotInvalidException.class, e -> {
                                // After a decision the append may have committed, and reading in full would run the
                                // command twice. A refusal marked as before the append comes from a read, so the
                                // attempt that threw it appended nothing
                                if (base.markerVersions().isEmpty() || (decided.get() && !e.isBeforeAppend())) {
                                    return Mono.error(e);
                                }
                                log.debug("Reading the whole boundary of snapshot '{}' since the store can't read after its marker versions: {}", key, e.getMessage());
                                return executeFrom(SnapshotSupport.DcbBase.initial(decider::initialState), criteria, commands, dcbDecider, refuseNullState, new AtomicBoolean());
                            });
                    })
                    .flatMap(executed -> {
                        Optional<DcbAppendResult> appendResult = executed.appendResult();
                        if (appendResult.isEmpty()) {
                            return Mono.just(executed);
                        }
                        DcbAppendResult result = appendResult.get();
                        Decider.Decision<S, E> decision = executed.decision();
                        return ReactiveSnapshotSupport.maybeSaveBestEffort(store, key, options.policy(),
                                        () -> new SnapshotDecision<>(decision.state(), decision.events(), 0, executed.tailSize() + decision.events().size()),
                                        snapshotDecision -> {
                                            if (result.markerVersions().isEmpty()) {
                                                skippedSaves.skip(key, SkippedSaves.Reason.NO_MARKER_VERSIONS);
                                                return Mono.empty();
                                            }
                                            return applicationService.isAppendConfirmed(result).flatMap(confirmed -> {
                                                if (!confirmed) {
                                                    skippedSaves.skip(key, SkippedSaves.Reason.UNCONFIRMED);
                                                    return Mono.empty();
                                                }
                                                return Mono.just(Snapshot.atMarkerVersions(snapshotDecision.newState(), result.markerVersions().get(), result.lastSequencePosition(), options.schemaVersion()));
                                            });
                                        })
                                .thenReturn(executed);
                    });
        });
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> executeFrom(SnapshotSupport.DcbBase<S> base, DcbCriteria criteria, List<C> commands,
                                                                            DcbDecider<C, S, E> dcbDecider, boolean refuseNullState, AtomicBoolean decided) {
        return Mono.defer(() -> {
            Decider<C, S, E> decider = dcbDecider.decider();
            DcbExecuteOptions<E> withTags = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
            DcbExecuteOptions<E> executeOptions = base.markerVersions().map(markerVersions -> withTags.fromVersions(markerVersions, base.highestFoldedPosition())).orElseGet(withTags::withVersions);
            AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
            AtomicInteger tailSize = new AtomicInteger();
            return applicationService.execute(criteria, executeOptions,
                            tail -> {
                                tailSize.set(tail.size());
                                S current = decider.evolve(base.state(), tail);
                                Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                                // The decision is known before functionThatCallsDomainModel returns, which is before
                                // the application service appends anything, so refusing a null state here (rather
                                // than after the append) keeps a rejected command from ever being committed.
                                if (refuseNullState) {
                                    requireNonNullState(decision);
                                }
                                decisionRef.set(decision);
                                decided.set(true);
                                return decision.events();
                            })
                    .map(result -> new Executed<>(Optional.of(result), requireDecision(decisionRef), tailSize.get()))
                    // The domain function produced no events, so nothing was appended, but the decider still folded a
                    // decision, so return it for executeAndReturnState/Decision (execute filters it back to empty).
                    .switchIfEmpty(Mono.fromSupplier(() -> new Executed<>(Optional.empty(), requireDecision(decisionRef), tailSize.get())));
        });
    }

    private static <S extends @Nullable Object, E> Decider.Decision<S, E> requireDecision(AtomicReference<Decider.Decision<S, E>> decisionRef) {
        return Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
    }

    private record Executed<S extends @Nullable Object, E>(Optional<DcbAppendResult> appendResult, Decider.Decision<S, E> decision, int tailSize) {
    }
}
