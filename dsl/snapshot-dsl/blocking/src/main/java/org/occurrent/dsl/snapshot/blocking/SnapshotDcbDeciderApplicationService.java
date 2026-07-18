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

package org.occurrent.dsl.snapshot.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The DCB counterpart to {@link SnapshotDeciderApplicationService}: runs a {@link DcbDecider} but resumes from a snapshot
 * instead of folding the whole DCB boundary. Construct it once around a {@link DcbApplicationService} together with the
 * {@link SnapshotStore} and {@link SnapshotOptions} for the state it snapshots, then call it with command(s) and a decider.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is a canonical,
 * order-insensitive rendering of the {@link DcbCriteria} that {@link DcbDecider#criteriaFor(List)} resolves for the
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}); pass a key function to the constructor to override it. The
 * snapshot's version is the global DCB position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and
 * the resume read still captures the whole boundary's consistency token, so the append condition is unaffected and a stale
 * snapshot only lengthens the tail. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 *
 * @param <S> the snapshot state type
 * @param <E> the event type
 */
@NullMarked
public final class SnapshotDcbDeciderApplicationService<S extends @Nullable Object, E> {

    private final DcbApplicationService<E> applicationService;
    private final SnapshotStore<S> store;
    private final SnapshotOptions<S, E> options;
    private final Function<DcbCriteria, String> keyFunction;

    public SnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        this(applicationService, store, options, DcbSnapshotKeys::canonicalKey);
    }

    public SnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService, SnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction cannot be null");
    }

    /**
     * Execute a single command, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C> Optional<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider) {
        return execute(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C> Optional<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return doExecute(commands, dcbDecider).appendResult();
    }

    /**
     * Execute a single command and return the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C> Decider.Decision<S, E> executeAndReturnDecision(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} and return the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C> Decider.Decision<S, E> executeAndReturnDecision(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return doExecute(commands, dcbDecider).decision();
    }

    /**
     * Execute a single command and return the folded state after the decision (even when nothing was appended).
     */
    public <C> S executeAndReturnState(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision (even when nothing was appended).
     */
    public <C> S executeAndReturnState(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).state();
    }

    /**
     * Execute a single command and return the new events that were decided.
     */
    public <C> List<E> executeAndReturnEvents(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).events();
    }

    /**
     * Execute {@code commands} and return the new events that were decided.
     */
    public <C> List<E> executeAndReturnEvents(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).events();
    }

    private <C> Executed<S, E> doExecute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(dcbDecider, "dcbDecider cannot be null");

        DcbCriteria criteria = dcbDecider.criteriaFor(commands);
        String key = Objects.requireNonNull(keyFunction.apply(criteria), "snapshot key cannot be null");
        Decider<C, S, E> decider = dcbDecider.decider();

        SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(store.findLatest(key), options.schemaVersion(), decider::initialState);

        AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
        AtomicInteger tailSize = new AtomicInteger();
        Optional<DcbAppendResult> appendResult = applicationService.execute(criteria,
                DcbExecuteOptions.<E>options().fromPosition(base.version()).tagGenerator(dcbDecider.tags()),
                tail -> {
                    tailSize.set(tail.size());
                    S current = decider.evolve(base.state(), tail);
                    Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                    decisionRef.set(decision);
                    return decision.events();
                });

        Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
        appendResult.ifPresent(result -> {
            int eventsSinceSnapshot = tailSize.get() + decision.events().size();
            SnapshotSupport.maybeSaveBestEffort(store, key, options.schemaVersion(), options.policy(),
                    new SnapshotDecision<>(decision.state(), decision.events(), result.lastSequencePosition(), eventsSinceSnapshot));
        });
        return new Executed<>(appendResult, decision);
    }

    private record Executed<S extends @Nullable Object, E>(Optional<DcbAppendResult> appendResult, Decider.Decision<S, E> decision) {
    }
}
