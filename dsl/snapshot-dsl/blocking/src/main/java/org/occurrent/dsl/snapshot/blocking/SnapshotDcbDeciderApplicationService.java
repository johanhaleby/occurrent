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
 * instead of folding the whole DCB boundary. Construct it once around a {@link DcbApplicationService} and reuse it for
 * every aggregate. Each execute takes a {@link SnapshotDcbDecider}, the per-aggregate spec that bundles the decider with
 * its {@link SnapshotStore}, {@link SnapshotOptions}, and snapshot-key function.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is a canonical,
 * order-insensitive rendering of the {@link DcbCriteria} that {@link DcbDecider#criteriaFor(List)} resolves for the
 * commands ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}); the spec's key function overrides it. The snapshot's
 * version is the global DCB position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and the
 * resume read still captures the whole boundary's consistency token, so the append condition is unaffected and a stale
 * snapshot only lengthens the tail. It loads one snapshot per execute, and costs nothing when no snapshot is used.
 * <p>
 * Deliberate asymmetry with the stream executor: this executor only advances the base (calls
 * {@code maybeSaveBestEffort}) when the decision actually appended events, since a no-op decision returns no
 * {@link DcbAppendResult} to key the save on. {@link SnapshotDeciderApplicationService} instead saves unconditionally
 * after every execute, because a stream write always has a {@code WriteResult} to advance the base from, whether or
 * not new events were appended. Either way a missed save only costs a longer replay on the next execute; it is never
 * a correctness issue.
 *
 * @param <E> the event type
 */
@NullMarked
public final class SnapshotDcbDeciderApplicationService<E> {

    private final DcbApplicationService<E> applicationService;

    public SnapshotDcbDeciderApplicationService(DcbApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Optional<DcbAppendResult> execute(C command, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return execute(List.of(command), snapshotDcbDecider);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Optional<DcbAppendResult> execute(List<C> commands, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider).appendResult();
    }

    /**
     * Execute a single command and return the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(C command, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(List.of(command), snapshotDcbDecider);
    }

    /**
     * Execute {@code commands} and return the folded state plus the events that were decided (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(List<C> commands, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return doExecute(commands, snapshotDcbDecider).decision();
    }

    /**
     * Execute a single command and return the folded state after the decision (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(C command, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(command, snapshotDcbDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision (even when nothing was appended).
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(List<C> commands, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(commands, snapshotDcbDecider).state();
    }

    /**
     * Execute a single command and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(C command, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(command, snapshotDcbDecider).events();
    }

    /**
     * Execute {@code commands} and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(List<C> commands, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(commands, snapshotDcbDecider).events();
    }

    private <C, S extends @Nullable Object> Executed<S, E> doExecute(List<C> commands, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(snapshotDcbDecider, "snapshotDcbDecider cannot be null");

        DcbDecider<C, S, E> dcbDecider = snapshotDcbDecider.dcbDecider();
        SnapshotStore<S> store = snapshotDcbDecider.store();
        SnapshotOptions<S, E> options = snapshotDcbDecider.options();
        Function<DcbCriteria, String> keyFunction = snapshotDcbDecider.keyFunction();

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
        // DCB positions are global and monotonic, they never reset, so a snapshot can never be ahead of the head: no
        // head guard or self-heal is needed here, and eventsSinceSnapshot (tail + events) cannot go negative. The
        // Supplier-based best-effort is used only for symmetry with the stream executor.
        appendResult.ifPresent(result ->
                SnapshotSupport.maybeSaveBestEffort(store, key, options.schemaVersion(), options.policy(), () -> {
                    int eventsSinceSnapshot = tailSize.get() + decision.events().size();
                    return new SnapshotDecision<>(decision.state(), decision.events(), result.lastSequencePosition(), eventsSinceSnapshot);
                }));
        return new Executed<>(appendResult, decision);
    }

    private record Executed<S extends @Nullable Object, E>(Optional<DcbAppendResult> appendResult, Decider.Decision<S, E> decision) {
    }
}
