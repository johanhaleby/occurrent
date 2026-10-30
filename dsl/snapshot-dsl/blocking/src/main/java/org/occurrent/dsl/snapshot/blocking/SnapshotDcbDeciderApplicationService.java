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
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.blocking.internal.SnapshotStoreSupport;
import org.occurrent.dsl.snapshot.internal.SkippedSaves;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * that 0.34 saved at a DCB position is never used, so the first execute after an upgrade reads the whole boundary and
 * saves over it.
 * <p>
 * Deliberate asymmetry with the stream executor: this executor only advances the base (calls
 * {@code maybeSaveBestEffort}) when the decision actually appended events, since a no-op decision returns no
 * {@link DcbAppendResult} to key the save on. {@link SnapshotDeciderApplicationService} instead saves unconditionally
 * after every execute, because a stream write always has a {@code WriteResult} to advance the base from, whether or
 * not new events were appended. Either way a missed save only costs a longer replay on the next execute. It is never
 * a correctness issue.
 *
 * @param <E> the event type
 */
@NullMarked
public final class SnapshotDcbDeciderApplicationService<E> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotDcbDeciderApplicationService.class);

    private final DcbApplicationService<E> applicationService;
    private final SkippedSaves skippedSaves = new SkippedSaves(log);

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
     * A null state is returned as it is, unlike the reactive services, which refuse it because a {@code Mono} cannot
     * hold null.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(C command, SnapshotDcbDecider<C, S, E> snapshotDcbDecider) {
        return executeAndReturnDecision(command, snapshotDcbDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision (even when nothing was appended).
     * A null state is returned as it is, unlike the reactive services, which refuse it because a {@code Mono} cannot
     * hold null.
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

        SnapshotSupport.DcbBase<S> base = SnapshotSupport.resolveDcbBase(store.findLatest(key), options.schemaVersion(), decider::initialState);
        Executed<S, E> executed;
        AtomicBoolean decided = new AtomicBoolean();
        try {
            executed = executeFrom(base, criteria, commands, dcbDecider, decided);
        } catch (DcbSnapshotInvalidException e) {
            // After a decision the append may have committed, for example when a synchronous subscription throws this
            // without a transaction, and reading in full would run the command twice. A refusal marked as before the
            // append is the read of a retry, and the default retry strategy retries only a failed append condition
            if (base.markerVersions().isEmpty() || (decided.get() && !e.isBeforeAppend())) {
                throw e;
            }
            log.debug("Reading the whole boundary of snapshot '{}' since the store can't read after its marker versions: {}", key, e.getMessage());
            executed = executeFrom(SnapshotSupport.DcbBase.initial(decider::initialState), criteria, commands, dcbDecider, new AtomicBoolean());
        }

        Decider.Decision<S, E> decision = executed.decision();
        int tailSize = executed.tailSize();
        executed.appendResult().ifPresent(result ->
                SnapshotStoreSupport.maybeSaveBestEffort(store, key, options.schemaVersion(), options.policy(),
                        () -> new SnapshotDecision<>(decision.state(), decision.events(), 0, tailSize + decision.events().size()),
                        snapshotDecision -> {
                            if (result.markerVersions().isEmpty()) {
                                skippedSaves.skip(key, SkippedSaves.Reason.NO_MARKER_VERSIONS);
                                return Optional.empty();
                            } else if (!applicationService.isAppendConfirmed(result)) {
                                skippedSaves.skip(key, SkippedSaves.Reason.UNCONFIRMED);
                                return Optional.empty();
                            }
                            return Optional.of(Snapshot.atMarkerVersions(snapshotDecision.newState(), result.markerVersions().get(), result.lastSequencePosition(), options.schemaVersion()));
                        }));
        return executed;
    }

    private <C, S extends @Nullable Object> Executed<S, E> executeFrom(SnapshotSupport.DcbBase<S> base, DcbCriteria criteria, List<C> commands, DcbDecider<C, S, E> dcbDecider, AtomicBoolean decided) {
        Decider<C, S, E> decider = dcbDecider.decider();
        DcbExecuteOptions<E> withTags = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
        DcbExecuteOptions<E> executeOptions = base.markerVersions().map(markerVersions -> withTags.fromVersions(markerVersions, base.highestFoldedPosition())).orElseGet(withTags::withVersions);
        AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
        AtomicInteger tailSize = new AtomicInteger();
        Optional<DcbAppendResult> appendResult = applicationService.execute(criteria, executeOptions,
                tail -> {
                    tailSize.set(tail.size());
                    S current = decider.evolve(base.state(), tail);
                    Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                    decisionRef.set(decision);
                    decided.set(true);
                    return decision.events();
                });
        Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
        return new Executed<>(appendResult, decision, tailSize.get());
    }

    private record Executed<S extends @Nullable Object, E>(Optional<DcbAppendResult> appendResult, Decider.Decision<S, E> decision, int tailSize) {
    }
}
