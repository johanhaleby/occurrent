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
 * together with the {@link ReactiveSnapshotStore} and {@link SnapshotOptions} for the state it snapshots, then call it with
 * command(s) and a decider.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is a canonical,
 * order-insensitive rendering of the {@link DcbCriteria} that {@link DcbDecider#criteriaFor(List)} resolves for the
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}); pass a key function to the constructor to override it. The
 * snapshot's version is the global DCB position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and
 * the resume read still captures the whole boundary's consistency token, so the append condition is unaffected and a stale
 * snapshot only lengthens the tail. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 * <p>
 * An empty result Mono from {@link #execute} means the domain function produced no new events, so nothing is appended and
 * no snapshot is written. {@link #executeAndReturnDecision}/{@link #executeAndReturnState} still emit the decided state
 * even for a no-op.
 *
 * @param <S> the snapshot state type
 * @param <E> the event type
 */
@NullMarked
public final class ReactiveSnapshotDcbDeciderApplicationService<S extends @Nullable Object, E> {

    private final DcbApplicationService<E> applicationService;
    private final ReactiveSnapshotStore<S> store;
    private final SnapshotOptions<S, E> options;
    private final Function<DcbCriteria, String> keyFunction;

    public ReactiveSnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        this(applicationService, store, options, DcbSnapshotKeys::canonicalKey);
    }

    public ReactiveSnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction cannot be null");
    }

    /**
     * Execute a single command, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C> Mono<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider) {
        return execute(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return doExecute(commands, dcbDecider).flatMap(executed -> Mono.justOrEmpty(executed.appendResult()));
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return doExecute(commands, dcbDecider).map(Executed::decision);
    }

    /**
     * Execute a single command and emit the folded state after the decision (even when nothing was appended). A {@link Mono}
     * cannot carry a null value, so the snapshot state {@code S} must be non-null here, use
     * {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C> Mono<S> executeAndReturnState(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).map(ReactiveSnapshotDcbDeciderApplicationService::requireNonNullState);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision (even when nothing was appended).
     */
    public <C> Mono<S> executeAndReturnState(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).map(ReactiveSnapshotDcbDeciderApplicationService::requireNonNullState);
    }

    // A Mono cannot carry null, so a null folded state fails fast with guidance instead of a bare NPE from Reactor.
    private static <S, E> S requireNonNullState(Decider.Decision<S, E> decision) {
        return Objects.requireNonNull(decision.state(), "The decider produced a null state, but a Mono cannot carry null. Use executeAndReturnDecision for a nullable state.");
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).map(Decider.Decision::events);
    }

    private <C> Mono<Executed<S, E>> doExecute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(dcbDecider, "dcbDecider cannot be null");

        return Mono.defer(() -> {
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
                            int eventsSinceSnapshot = tailSize.get() + decision.events().size();
                            return ReactiveSnapshotSupport.maybeSaveBestEffort(store, key, options.schemaVersion(), options.policy(),
                                            new SnapshotDecision<>(decision.state(), decision.events(), result.lastSequencePosition(), eventsSinceSnapshot))
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
