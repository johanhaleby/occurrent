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
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.application.service.reactor.ExecuteOptions;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.WriteResult;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reactive {@link ApplicationService} facade that runs a {@link Decider} but resumes from a snapshot instead of
 * replaying the whole stream, the reactive counterpart to {@link org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService}.
 * Construct it once around an application service together with the {@link ReactiveSnapshotStore} and {@link SnapshotOptions}
 * for the state it snapshots, then call it with a stream id, command(s), and a decider.
 * <p>
 * On each execute it loads the latest {@link Snapshot} for the stream, reads only the events written after it (via
 * {@link ExecuteOptions#fromStreamVersion(long)}), folds those onto the snapshot state with {@link Decider#evolve(Object, List)},
 * decides, writes, and then writes a fresh snapshot when the {@link org.occurrent.dsl.snapshot.SnapshotPolicy} in the
 * {@link SnapshotOptions} fires. The optimistic write still happens at the stream's true current version, so concurrency
 * control is unchanged and a stale snapshot only means a longer tail to fold, never a wrong result. It loads one snapshot
 * and reads only the events after it per execute, and a plain application service without a snapshot store pays nothing.
 * <p>
 * Snapshots are a discardable optimization: a loaded snapshot whose schema version does not match the one in
 * {@link SnapshotOptions} is ignored and the state is rebuilt from scratch. The snapshot write is best-effort (this facade
 * writes it after the command's own write, and a save failure is logged rather than failing the committed command).
 *
 * @param <S> the snapshot state type
 * @param <E> the event type
 */
@NullMarked
public final class ReactiveSnapshotDeciderApplicationService<S extends @Nullable Object, E> {

    private final ApplicationService<E> applicationService;
    private final ReactiveSnapshotStore<S> store;
    private final SnapshotOptions<S, E> options;

    public ReactiveSnapshotDeciderApplicationService(ApplicationService<E> applicationService, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
        this.store = Objects.requireNonNull(store, "store cannot be null");
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C> Mono<WriteResult> execute(String streamId, C command, Decider<C, S, E> decider) {
        return execute(streamId, List.of(command), decider);
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C> Mono<WriteResult> execute(UUID streamId, C command, Decider<C, S, E> decider) {
        return execute(streamId.toString(), command, decider);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C> Mono<WriteResult> execute(String streamId, List<C> commands, Decider<C, S, E> decider) {
        return doExecute(streamId, commands, decider).map(Executed::writeResult);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C> Mono<WriteResult> execute(UUID streamId, List<C> commands, Decider<C, S, E> decider) {
        return execute(streamId.toString(), commands, decider);
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided.
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(String streamId, C command, Decider<C, S, E> decider) {
        return doExecute(streamId, List.of(command), decider).map(Executed::decision);
    }

    /**
     * Execute a single command and emit the folded state plus the events that were decided.
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(UUID streamId, C command, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId.toString(), command, decider);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided.
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(String streamId, List<C> commands, Decider<C, S, E> decider) {
        return doExecute(streamId, commands, decider).map(Executed::decision);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the events that were decided.
     */
    public <C> Mono<Decider.Decision<S, E>> executeAndReturnDecision(UUID streamId, List<C> commands, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId.toString(), commands, decider);
    }

    /**
     * Execute a single command and emit the folded state after the decision. A {@link Mono} cannot carry a null value, so
     * the snapshot state {@code S} must be non-null here, use {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C> Mono<S> executeAndReturnState(String streamId, C command, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, command, decider).map(ReactiveSnapshotDeciderApplicationService::requireNonNullState);
    }

    /**
     * Execute a single command and emit the folded state after the decision.
     */
    public <C> Mono<S> executeAndReturnState(UUID streamId, C command, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, command, decider).map(ReactiveSnapshotDeciderApplicationService::requireNonNullState);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision.
     */
    public <C> Mono<S> executeAndReturnState(String streamId, List<C> commands, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, commands, decider).map(ReactiveSnapshotDeciderApplicationService::requireNonNullState);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision.
     */
    public <C> Mono<S> executeAndReturnState(UUID streamId, List<C> commands, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, commands, decider).map(ReactiveSnapshotDeciderApplicationService::requireNonNullState);
    }

    // A Mono cannot carry null, so a null folded state fails fast with guidance instead of a bare NPE from Reactor.
    private static <S, E> S requireNonNullState(Decider.Decision<S, E> decision) {
        return Objects.requireNonNull(decision.state(), "The decider produced a null state, but a Mono cannot carry null. Use executeAndReturnDecision for a nullable state.");
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(String streamId, C command, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, command, decider).map(Decider.Decision::events);
    }

    /**
     * Execute a single command and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(UUID streamId, C command, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, command, decider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(String streamId, List<C> commands, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, commands, decider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events that were decided.
     */
    public <C> Mono<List<E>> executeAndReturnEvents(UUID streamId, List<C> commands, Decider<C, S, E> decider) {
        return executeAndReturnDecision(streamId, commands, decider).map(Decider.Decision::events);
    }

    private <C> Mono<Executed<S, E>> doExecute(String streamId, List<C> commands, Decider<C, S, E> decider) {
        Objects.requireNonNull(streamId, "streamId cannot be null");
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(decider, "decider cannot be null");

        // Defer so the snapshot load and everything after it are cold: nothing runs until subscribed, and each subscription
        // loads a fresh base. The base does not change between the app service's optimistic-retry attempts.
        return Mono.defer(() -> ReactiveSnapshotSupport.resolveBase(store, streamId, options.schemaVersion(), decider::initialState).flatMap(base -> {
            AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
            return applicationService.execute(streamId, ExecuteOptions.<E>empty().fromStreamVersion(base.version()), tail -> {
                S current = decider.evolve(base.state(), tail);
                Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                decisionRef.set(decision);
                return decision.events();
            }).flatMap(writeResult -> {
                Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
                long newVersion = writeResult.newStreamVersion();
                int eventsSinceSnapshot = SnapshotSupport.requireInt(newVersion - base.version(), "the number of events since the snapshot");
                return ReactiveSnapshotSupport.maybeSaveBestEffort(store, streamId, options.schemaVersion(), options.policy(),
                                new SnapshotDecision<>(decision.state(), decision.events(), newVersion, eventsSinceSnapshot))
                        .thenReturn(new Executed<>(writeResult, decision));
            });
        }));
    }

    private record Executed<S extends @Nullable Object, E>(WriteResult writeResult, Decider.Decision<S, E> decision) {
    }
}
