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
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.ExecuteOptions;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.eventstore.api.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A blocking {@link ApplicationService} facade that runs a {@link Decider} but resumes from a snapshot instead of
 * replaying the whole stream, the snapshot counterpart to {@link org.occurrent.dsl.decider.DeciderApplicationService}.
 * Construct it once around an application service and reuse it for every aggregate, exactly like the plain facade. Each
 * execute takes a {@link SnapshotDecider}, the per-aggregate spec that bundles the decider with its {@link SnapshotStore}
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
 * {@link SnapshotOptions} is ignored and the state is rebuilt from scratch. The snapshot write is best-effort: it happens
 * after the command's events commit and a save failure is logged rather than failing the committed command. For a snapshot
 * kept consistent on the write path, maintain it with {@code @Snapshot(mode = SYNCHRONOUS)} or a synchronous subscription.
 * <p>
 * The decider's event type must equal the application service's event type {@code E}. Widen a narrower decider with
 * {@link Decider#adapt(Decider, Class, Class)} first.
 *
 * @param <E> the event type
 */
@NullMarked
public final class SnapshotDeciderApplicationService<E> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotDeciderApplicationService.class);

    private final ApplicationService<E> applicationService;

    public SnapshotDeciderApplicationService(ApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> WriteResult execute(String streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId, List.of(command), snapshotDecider);
    }

    /**
     * Execute a single command against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> WriteResult execute(UUID streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId.toString(), command, snapshotDecider);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> WriteResult execute(String streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider).writeResult();
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, resuming from the snapshot.
     */
    public <C, S extends @Nullable Object> WriteResult execute(UUID streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return execute(streamId.toString(), commands, snapshotDecider);
    }

    /**
     * Execute a single command and return the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(String streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, List.of(command), snapshotDecider).decision();
    }

    /**
     * Execute a single command and return the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(UUID streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId.toString(), command, snapshotDecider);
    }

    /**
     * Execute {@code commands} and return the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(String streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return doExecute(streamId, commands, snapshotDecider).decision();
    }

    /**
     * Execute {@code commands} and return the folded state plus the events that were decided.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(UUID streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId.toString(), commands, snapshotDecider);
    }

    /**
     * Execute a single command and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(String streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).state();
    }

    /**
     * Execute a single command and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(UUID streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(String streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(UUID streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).state();
    }

    /**
     * Execute a single command and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(String streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).events();
    }

    /**
     * Execute a single command and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(UUID streamId, C command, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, command, snapshotDecider).events();
    }

    /**
     * Execute {@code commands} and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(String streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).events();
    }

    /**
     * Execute {@code commands} and return the new events that were decided.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(UUID streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        return executeAndReturnDecision(streamId, commands, snapshotDecider).events();
    }

    private <C, S extends @Nullable Object> Executed<S, E> doExecute(String streamId, List<C> commands, SnapshotDecider<C, S, E> snapshotDecider) {
        Objects.requireNonNull(streamId, "streamId cannot be null");
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(snapshotDecider, "snapshotDecider cannot be null");

        Decider<C, S, E> decider = snapshotDecider.decider();
        SnapshotStore<S> store = snapshotDecider.store();
        SnapshotOptions<S, E> options = snapshotDecider.options();

        // Load once, outside the app service's optimistic-retry loop: the snapshot base does not change between attempts,
        // a conflict just re-reads a longer tail and folds it onto the same base again.
        SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(store.findLatest(streamId), options.schemaVersion(), decider::initialState);

        AtomicReference<Decider.Decision<S, E>> decisionRef = new AtomicReference<>();
        WriteResult writeResult = applicationService.execute(streamId, ExecuteOptions.<E>empty().fromStreamVersion(base.version()), tail -> {
            S current = decider.evolve(base.state(), tail);
            Decider.Decision<S, E> decision = decider.decideOnState(current, commands);
            decisionRef.set(decision);
            return decision.events();
        });

        Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
        if (base.version() > writeResult.oldStreamVersion()) {
            // Self-heal: the snapshot's version is ahead of the stream's true head before this write, so the stream was
            // reset (truncated) below the snapshot after it was written. There is nothing valid to persist, and the
            // stale snapshot must go, otherwise the next command resumes from state the reset stream no longer holds.
            // Deleting it makes the next resolveBase fold fresh from the reset stream. This is misuse-only (a reset that
            // did not pair with SnapshotStore.delete); the delete is best-effort because the command already committed.
            log.warn("Snapshot for stream '{}' is at version {} but the stream's head was {} before this write, so the stream was reset below the snapshot. Deleting the stale snapshot; the next command folds fresh from the stream. Pair a stream reset with SnapshotStore.delete to avoid this.",
                    streamId, base.version(), writeResult.oldStreamVersion());
            try {
                store.delete(streamId);
            } catch (RuntimeException e) {
                log.warn("Failed to delete the stale snapshot for stream '{}' after detecting a reset. It will be discarded again by the head guard on the next command.", streamId, e);
            }
            return new Executed<>(writeResult, decision);
        }
        // Build the decision (including the eventsSinceSnapshot narrowing) inside the best-effort boundary so nothing
        // after the commit can surface the committed command as a failure.
        SnapshotSupport.maybeSaveBestEffort(store, streamId, options.schemaVersion(), options.policy(), () -> {
            long newVersion = writeResult.newStreamVersion();
            int eventsSinceSnapshot = SnapshotSupport.requireInt(newVersion - base.version(), "the number of events since the snapshot");
            return new SnapshotDecision<>(decision.state(), decision.events(), newVersion, eventsSinceSnapshot);
        });
        return new Executed<>(writeResult, decision);
    }

    private record Executed<S extends @Nullable Object, E>(WriteResult writeResult, Decider.Decision<S, E> decision) {
    }
}
