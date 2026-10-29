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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ExecuteOptions;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reactive {@link ApplicationService} facade that runs a {@link Decider} but resumes from a snapshot instead of
 * replaying the whole stream, the reactive counterpart to {@link org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService}.
 * Construct it once around an application service and reuse it for every aggregate. Each execute takes a
 * {@link ReactiveSnapshotDecider}, the per-aggregate spec that bundles the decider with its {@link ReactiveSnapshotStore}
 * and {@link SnapshotOptions}.
 * <p>
 * On each execute it loads the latest {@link Snapshot} for the stream, reads only the events written after it (via
 * {@link ExecuteOptions#fromStreamVersion(long)}), folds those onto the snapshot state with {@link Decider#evolve(Object, List)},
 * decides, writes, and then writes a fresh snapshot when the {@link org.occurrent.dsl.snapshot.SnapshotPolicy} in the
 * spec's {@link SnapshotOptions} fires. The optimistic write still happens at the stream's true current version, so
 * concurrency control is unchanged and a stale snapshot only means a longer tail to fold, never a wrong result. It loads
 * one snapshot and reads only the events after it per execute, and a plain application service without a snapshot store
 * pays nothing.
 * <p>
 * Snapshots are a discardable optimization: a loaded snapshot whose schema version does not match the one in the spec's
 * {@link SnapshotOptions} is ignored and the state is rebuilt from scratch. The snapshot write is best-effort (this facade
 * writes it after the command's own write, and a save failure is logged rather than failing the committed command).
 *
 * @param <E> the event type
 */
@NullMarked
public final class ReactiveSnapshotDeciderApplicationService<E> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSnapshotDeciderApplicationService.class);

    private final ApplicationService<E> applicationService;

    public ReactiveSnapshotDeciderApplicationService(ApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(String streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId, List.of(command), snapshotDecider);
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(UUID streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId.toString(), command, snapshotDecider);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider).map(Executed::writeResult);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(UUID streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId.toString(), commands, snapshotDecider);
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(String streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, List.of(command), snapshotDecider).map(Executed::decision);
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(UUID streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId.toString(), command, snapshotDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider).map(Executed::decision);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(UUID streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId.toString(), commands, snapshotDecider);
    }

    /**
     * Execute a single command and emit the folded state after the decision. The state type {@code S} may be nullable,
     * but a {@link Mono} cannot carry a null value, so when a decider folds to null, none of the command's events are
     * written and the {@code Mono} fails with a {@link NullPointerException}. The blocking
     * {@link org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService} returns a null state instead. Use
     * {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(String streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecuteAndReturnState(streamId, List.of(command), snapshotDecider);
    }

    /**
     * Execute a single command and emit the folded state after the decision. If the decider folds to a null state, none
     * of the command's events are written and the {@code Mono} fails. See
     * {@link #executeAndReturnState(String, Object, ReactiveSnapshotDecider)}.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(UUID streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecuteAndReturnState(streamId.toString(), List.of(command), snapshotDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision. The commands are decided as one unit and
     * appended once, so only the state after the last command is checked. If that state is null, no events are
     * written, not even the ones the earlier commands decided. See
     * {@link #executeAndReturnState(String, Object, ReactiveSnapshotDecider)}.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecuteAndReturnState(streamId, commands, snapshotDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision. The commands are decided as one unit and
     * appended once, so only the state after the last command is checked. If that state is null, no events are
     * written, not even the ones the earlier commands decided. See
     * {@link #executeAndReturnState(String, List, ReactiveSnapshotDecider)}.
     */
    public <C, S extends @Nullable Object> Mono<@NonNull S> executeAndReturnState(UUID streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecuteAndReturnState(streamId.toString(), commands, snapshotDecider);
    }

    private <C, S extends @Nullable Object> Mono<@NonNull S> doExecuteAndReturnState(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider, true).map(executed -> executed.decision().state());
    }

    // A Mono cannot carry null, so a null folded state fails fast with guidance instead of a bare NPE from Reactor.
    private static NullPointerException nullStateRefusal() {
        return new NullPointerException("The decider produced a null state, but a Mono cannot carry null. Use executeAndReturnDecision for a nullable state.");
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(String streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).map(Decider.Decision::events);
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(UUID streamId, C command, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(UUID streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).map(Decider.Decision::events);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider, false);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(String streamId, List<C> commands, ReactiveSnapshotDecider<C, S, E> snapshotDecider, boolean refuseNullState) {
        Objects.requireNonNull(streamId, "streamId cannot be null");
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(snapshotDecider, "snapshotDecider cannot be null");

        Decider<C, S, E> decider = snapshotDecider.decider();
        ReactiveSnapshotStore<S> store = snapshotDecider.store();
        SnapshotOptions<S, E> options = snapshotDecider.options();

        // Defer so the snapshot load and everything after it are cold: nothing runs until subscribed, and each subscription
        // loads a fresh base. The base does not change between the app service's optimistic-retry attempts.
        return Mono.defer(() -> ReactiveSnapshotSupport.resolveBase(store, streamId, options.schemaVersion(), decider::initialState).flatMap(base -> {
            AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
            return applicationService.execute(streamId, ExecuteOptions.<E>empty().fromStreamVersion(base.version()), tail -> {
                S current = decider.evolve(base.state(), tail);
                Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                decisionRef.set(decision);
                // The decision is known before functionThatCallsDomainModel returns, which is before the application
                // service writes anything. A refused null state writes no events, so the rejected command is never
                // committed, and the empty write still reports the stream's head for the self-heal below.
                return refuseNullState && decision.state() == null ? List.of() : decision.events();
            }).flatMap(writeResult -> {
                Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
                boolean refused = refuseNullState && decision.state() == null;
                Mono<Executed<S, E>> outcome = refused ? Mono.error(nullStateRefusal()) : Mono.just(new Executed<>(writeResult, decision));
                if (base.version() > writeResult.oldStreamVersion()) {
                    // Self-heal: the snapshot's version is ahead of the stream's true head before this write, so the
                    // stream was reset (truncated) below the snapshot after it was written. There is nothing valid to
                    // persist, and the stale snapshot must go, otherwise the next command resumes from state the reset
                    // stream no longer holds. Deleting it makes the next resolveBase fold fresh. This is misuse-only, a
                    // reset that did not pair with SnapshotStore.delete. The delete is best-effort because its failure
                    // must not change the outcome, a committed command or a refused one that wrote nothing.
                    log.warn("Snapshot for stream '{}' is at version {} but the stream's head was {} before this write, so the stream was reset below the snapshot. Deleting the stale snapshot; the next command folds fresh from the stream. Pair a stream reset with SnapshotStore.delete to avoid this.",
                            streamId, base.version(), writeResult.oldStreamVersion());
                    // Deferred so a store that throws before returning its Mono reaches onErrorResume too
                    return ReactiveSnapshotSupport.<Void>deferCatchingEverything(() -> store.delete(streamId))
                            .onErrorResume(e -> {
                                log.warn("Failed to delete the stale snapshot for stream '{}' after detecting a reset. It will be discarded again by the head guard on the next command.", streamId, e);
                                return Mono.empty();
                            })
                            .then(outcome);
                }
                if (refused) {
                    // Nothing was written, so there is no new state to snapshot
                    return outcome;
                }
                // Build the decision (including the eventsSinceSnapshot narrowing) inside the best-effort boundary so
                // nothing after the commit can surface the committed command as a failure.
                return ReactiveSnapshotSupport.maybeSaveBestEffort(store, streamId, options.schemaVersion(), options.policy(), () -> {
                            long newVersion = writeResult.newStreamVersion();
                            int eventsSinceSnapshot = SnapshotSupport.requireInt(newVersion - base.version(), "the number of events since the snapshot");
                            return new SnapshotDecision<>(decision.state(), decision.events(), newVersion, eventsSinceSnapshot);
                        })
                        .thenReturn(new Executed<>(writeResult, decision));
            });
        }));
    }

    private record Executed<S extends @Nullable Object, E>(WriteResult writeResult, Decider.Decision<S, E> decision) {
    }
}
