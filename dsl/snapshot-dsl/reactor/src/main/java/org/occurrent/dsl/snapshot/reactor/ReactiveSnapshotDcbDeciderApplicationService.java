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
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.SnapshotSupport;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * The DCB counterpart to {@link ReactiveSnapshotDeciderApplicationService}: runs a {@link DcbDecider} but resumes from a
 * snapshot instead of folding the whole DCB boundary. It wraps the lower-level reactive {@link DcbApplicationService} so it
 * can pass {@link DcbExecuteOptions#fromPosition(long)} and the decider's tags.
 * <p>
 * Because DCB has no stream id, the snapshot is keyed by the decider's read boundary. By default the key is the
 * {@link DcbCriteria#toString()} that {@link DcbDecider#criteriaFor(List)} resolves for the commands; pass a key function
 * to override this when a criteria's string form is not stable across processes. The snapshot's version is the global DCB
 * position the append landed at ({@link DcbAppendResult#lastSequencePosition()}), and the resume read still captures the
 * whole boundary's consistency token, so the append condition is unaffected and a stale snapshot only lengthens the tail.
 * <p>
 * An empty result Mono means the domain function produced no new events, so nothing is appended and no snapshot is written.
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
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(List.of(command), dcbDecider, store, options);
    }

    /**
     * Execute {@code commands} in order, resuming from the snapshot in {@code store} keyed by the decider's criteria.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return execute(commands, dcbDecider, store, options, DcbCriteria::toString);
    }

    /**
     * Execute {@code commands}, deriving the snapshot key from the resolved {@link DcbCriteria} with {@code keyFunction}.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider, SnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        Objects.requireNonNull(commands, "commands cannot be null");
        Objects.requireNonNull(dcbDecider, "dcbDecider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(keyFunction, "keyFunction cannot be null");

        return Mono.defer(() -> {
            DcbCriteria criteria = dcbDecider.criteriaFor(commands);
            String key = Objects.requireNonNull(keyFunction.apply(criteria), "snapshot key cannot be null");
            Decider<C, S, E> decider = dcbDecider.decider();

            SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(store.findLatest(key), options.schemaVersion(), decider::initialState);
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
                    }).map(result -> {
                Decider.Decision<S, E> decision = Objects.requireNonNull(decisionRef.get(), "The decider produced no decision");
                int eventsSinceSnapshot = tailSize.get() + decision.events().size();
                SnapshotSupport.maybeSave(store, key, options.schemaVersion(), options.policy(),
                        new SnapshotDecision<>(decision.state(), decision.events(), result.lastSequencePosition(), base.version(), eventsSinceSnapshot));
                return result;
            });
        });
    }
}
