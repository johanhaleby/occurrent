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
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotSupport;
import org.occurrent.eventstore.api.WriteResult;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reactive {@link ApplicationService} facade that runs a {@link Decider} but resumes from a snapshot instead of
 * replaying the whole stream, the reactive counterpart to {@link org.occurrent.dsl.snapshot.blocking.SnapshotDeciderApplicationService}.
 * <p>
 * On each execute it loads the latest {@link Snapshot} for the stream, reads only the events written after it (via
 * {@link ExecuteOptions#fromStreamVersion(long)}), folds those onto the snapshot state with {@link Decider#evolve(Object, List)},
 * decides, writes, and then writes a fresh snapshot when the {@link org.occurrent.dsl.snapshot.SnapshotPolicy} in the
 * {@link SnapshotOptions} fires. The optimistic write still happens at the stream's true current version, so concurrency
 * control is unchanged and a stale snapshot only means a longer tail to fold, never a wrong result.
 * <p>
 * Snapshots are a discardable optimization: a loaded snapshot whose schema version does not match the one in
 * {@link SnapshotOptions} is ignored and the state is rebuilt from scratch. The snapshot write is best-effort (this facade
 * writes it after the command's own write).
 * <p>
 * {@link SnapshotStore} is a blocking interface, so its {@code findLatest}/{@code save} run inline on the subscribing
 * thread, the same way the reactive projection DSL bridges its blocking view store. Keep the store fast or supply one that
 * does its own scheduling.
 */
@NullMarked
public final class ReactiveSnapshotDeciderApplicationService<E> {

    private final ApplicationService<E> applicationService;

    public ReactiveSnapshotDeciderApplicationService(ApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot in {@code store}.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(String streamId, C command, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(streamId, List.of(command), decider, store, options);
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot in {@code store}.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(UUID streamId, C command, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(streamId.toString(), command, decider, store, options);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot in {@code store}.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(String streamId, List<C> commands, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return doExecute(streamId, commands, decider, store, options).map(Executed::writeResult);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot in {@code store}.
     */
    public <C, S extends @Nullable Object> Mono<WriteResult> execute(UUID streamId, List<C> commands, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(streamId.toString(), commands, decider, store, options);
    }

    /**
     * Execute {@code command} and return the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(String streamId, C command, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return doExecute(streamId, List.of(command), decider, store, options).map(Executed::decision);
    }

    /**
     * Execute {@code command} and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> Mono<S> executeAndReturnState(String streamId, C command, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return executeAndReturnDecision(streamId, command, decider, store, options).map(Decider.Decision::state);
    }

    private <C, S extends @Nullable Object> Mono<Executed<S, E>> doExecute(String streamId, List<C> commands, Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        Objects.requireNonNull(streamId, "streamId cannot be null");
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(decider, "decider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");

        // Defer so the snapshot load and everything after it are cold: nothing runs until subscribed, and each subscription
        // loads a fresh base. The base does not change between the app service's optimistic-retry attempts.
        return Mono.defer(() -> {
            SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(store.findLatest(streamId), options.schemaVersion(), decider::initialState);
            AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
            return applicationService.execute(streamId, ExecuteOptions.<E>empty().fromStreamVersion(base.version()), tail -> {
                S current = decider.evolve(base.state(), tail);
                Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
                decisionRef.set(decision);
                return decision.events();
            }).map(writeResult -> {
                Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
                long newVersion = writeResult.newStreamVersion();
                int eventsSinceSnapshot = Math.toIntExact(newVersion - base.version());
                SnapshotSupport.maybeSaveBestEffort(store, streamId, options.schemaVersion(), options.policy(),
                        new SnapshotDecision<>(decision.state(), decision.events(), newVersion, base.version(), eventsSinceSnapshot));
                return new Executed<>(writeResult, decision);
            });
        });
    }

    private record Executed<S extends @Nullable Object, E>(WriteResult writeResult, Decider.Decision<S, E> decision) {
    }
}
