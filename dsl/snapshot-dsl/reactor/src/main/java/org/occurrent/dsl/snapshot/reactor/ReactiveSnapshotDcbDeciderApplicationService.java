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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbExecuteOptions;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}), the spec's key function overrides it. The snapshot's
 * version is the global DCB position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and the
 * resume read still captures the whole boundary's consistency token, so the append condition is unaffected and a stale
 * snapshot only lengthens the tail. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 * <p>
 * An empty result Mono from {@link #execute} means the domain function produced no new events, so nothing is appended and
 * no snapshot is written. {@link #executeAndReturnDecision}/{@link #executeAndReturnState} still emit the decided state
 * even for a no-op.
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

    private final DcbApplicationService<E> applicationService;

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
     * Execute a single command and emit the folded state after the decision (even when nothing was appended). A {@link Mono}
     * cannot carry a null value, so the snapshot state {@code S} must be non-null here, use
     * {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C, S extends @Nullable Object> Mono<S> executeAndReturnState(C command, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(command, snapshotDcbDecider).map(ReactiveSnapshotDcbDeciderApplicationService::requireNonNullState);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Mono<S> executeAndReturnState(List<C> commands, ReactiveSnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(commands, snapshotDcbDecider).map(ReactiveSnapshotDcbDeciderApplicationService::requireNonNullState);
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

            return ReactiveSnapshotSupport.resolveBase(store, key, options.schemaVersion(), decider::initialState).flatMap(base -> {
                AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
                AtomicInteger tailSize = new AtomicInteger();
                return applicationService.execute(criteria,
                                DcbExecuteOptions.<E>options().fromPosition(base.version()).tagGenerator(dcbDecider.tags()),
                                tail -> {
                                    tailSize.set(tail.size());
                                    S current = decider.evolve(base.state(), tail);
                                    Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                                    decisionRef.set(decision);
                                    return decision.events();
                                })
                        .flatMap(result -> {
                            Decider.Decision<S, E> decision = requireDecision(decisionRef);
                            // DCB positions are global and monotonic, they never reset, so a snapshot can never be ahead
                            // of the head: no head guard or self-heal is needed here, and eventsSinceSnapshot
                            // (tail + events) cannot go negative. The Supplier-based best-effort is used only for
                            // symmetry with the stream executor.
                            return ReactiveSnapshotSupport.maybeSaveBestEffort(store, key, options.schemaVersion(), options.policy(), () -> {
                                        int eventsSinceSnapshot = tailSize.get() + decision.events().size();
                                        return new SnapshotDecision<>(decision.state(), decision.events(), result.lastSequencePosition(), eventsSinceSnapshot);
                                    })
                                    .thenReturn(new Executed<>(Optional.of(result), decision));
                        })
                        // The domain function produced no events, so nothing was appended, but the decider still folded a
                        // decision, so return it for executeAndReturnState/Decision (execute filters it back to empty).
                        .switchIfEmpty(Mono.fromSupplier(() -> new Executed<>(Optional.empty(), requireDecision(decisionRef))));
            });
        });
    }

    private static <S extends @Nullable Object, E> Decider.Decision<S, E> requireDecision(AtomicReference<Decider.Decision<S, E>> decisionRef) {
        return Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
    }

    private record Executed<S extends @Nullable Object, E>(Optional<DcbAppendResult> appendResult, Decider.Decision<S, E> decision) {
    }
}
