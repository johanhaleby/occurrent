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
 * snapshot instead of folding the whole DCB boundary. It wraps the lower-level reactive {@link DcbApplicationService} so it
 * can pass {@link DcbExecuteOptions#fromPosition(long)} and the decider's tags.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is a canonical,
 * order-insensitive rendering of the {@link DcbCriteria} that {@link DcbDecider#criteriaFor(List)} resolves for the
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}); pass a key function to override it. The snapshot's version
 * is the global DCB position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and the resume read
 * still captures the whole boundary's consistency token, so the append condition is unaffected and a stale snapshot only
 * lengthens the tail. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 * <p>
 * An empty result Mono from {@link #execute} means the domain function produced no new events, so nothing is appended and
 * no snapshot is written. {@link #executeAndReturnDecision}/{@link #executeAndReturnState} still emit the decided state
 * even for a no-op.
 */
@NullMarked
public final class ReactiveSnapshotDcbDeciderApplicationService<E> {

    private final DcbApplicationService<E> applicationService;

    public ReactiveSnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command, resuming from the snapshot in {@code store} keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(List.of(command), dcbDecider, store, options);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot in {@code store} keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(commands, dcbDecider, store, options, DcbSnapshotKeys::canonicalKey);
    }

    /**
     * Execute {@code commands}, deriving the snapshot key from the resolved {@link DcbCriteria} with {@code keyFunction}.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        return doExecute(commands, dcbDecider, store, options, keyFunction).flatMap(executed -> Mono.justOrEmpty(executed.appendResult()));
    }

    /**
     * Execute {@code command} and return the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(C command, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return doExecute(List.of(command), dcbDecider, store, options, DcbSnapshotKeys::canonicalKey).map(Executed::decision);
    }

    /**
     * Execute {@code command} and return the folded state after the decision (even when nothing was appended). The
     * state is bound to a non-null type because a {@link Mono} cannot carry a null value, use
     * {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C, S> Mono<S> executeAndReturnState(C command, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return executeAndReturnDecision(command, dcbDecider, store, options).map(Decider.Decision::state);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(List<C> commands, DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(dcbDecider, "dcbDecider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(keyFunction, "keyFunction cannot be null");

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
