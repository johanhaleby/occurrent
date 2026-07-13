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

package org.occurrent.dsl.dcb.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.reactor.dcb.DcbApplicationService;
import org.occurrent.application.service.reactor.dcb.DcbExecuteOptions;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thin facade over a reactive {@link DcbApplicationService} that runs a {@link DcbDecider}, the Java counterpart to
 * the Kotlin {@code execute(command, dcbDecider)} extension in the reactor
 * {@code DcbApplicationServiceDeciderExtensions.kt}. Construct it once around an existing DCB application service and
 * call {@link #execute} with a command and a decider.
 * <p>
 * The {@code DcbDecider} carries all three pieces DCB execution needs: the decision function, the {@link DcbCriteria}
 * read boundary derived from the command, and the {@link org.occurrent.application.service.dcb.TagGenerator} for the
 * events it writes. The decision itself is synchronous, only the read and append are reactive.
 * <p>
 * The decider's event type must be the same as the application service's event type {@code E}. A feature decider whose
 * event type is narrower than {@code E} should first be widened with {@link DcbDecider#adapt(DcbDecider, Class, Class)}
 * (or combined with {@link DcbDecider#compose}, which already yields a decider over {@code E}).
 */
@NullMarked
public final class DcbDeciderApplicationService<E> {

    private final DcbApplicationService<E> applicationService;

    public DcbDeciderApplicationService(DcbApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command using {@code dcbDecider}. Emits the {@link DcbAppendResult}, or completes empty when the
     * decider produced no new events. Fails with {@link IllegalArgumentException} when the command is not recognized.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider) {
        return execute(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} in order using {@code dcbDecider}. All commands must resolve to the same read boundary
     * since they are appended atomically under one condition.
     */
    public <C, S extends @Nullable Object> Mono<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        // Deferred so boundary resolution (and the IllegalArgumentException it may throw) happens per subscription
        // rather than eagerly when the Mono is built.
        return Mono.defer(() -> {
            DcbCriteria criteria = dcbDecider.criteriaFor(commands);
            DcbExecuteOptions<E> options = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
            return applicationService.execute(criteria, options, events -> dcbDecider.decider().decideOnEventsAndReturnEvents(events, commands));
        });
    }

    /**
     * Execute a single command and emit the folded state plus the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} and emit the folded state plus the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Mono<Decider.Decision<S, E>> executeAndReturnDecision(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        // Deferred so the AtomicReference is created per subscription. A shared reference would let concurrent or
        // repeat subscribers see each other's decision.
        return Mono.defer(() -> {
            DcbCriteria criteria = dcbDecider.criteriaFor(commands);
            DcbExecuteOptions<E> options = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
            AtomicReference<Decider.Decision<S, E>> decision = new AtomicReference<>();
            return applicationService.execute(criteria, options, events -> {
                Decider.Decision<S, E> result = dcbDecider.decider().decideOnEvents(events, commands);
                decision.set(result);
                return result.events();
            }).then(Mono.fromCallable(() -> Objects.requireNonNull(decision.get(), "The decider produced no decision")));
        });
    }

    /**
     * Execute a single command and emit the folded state after the decision. The state is bound to a non-null type
     * because a {@link Mono} cannot carry a null value, use {@link #executeAndReturnDecision} for a nullable state.
     */
    public <C, S> Mono<S> executeAndReturnState(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).map(Decider.Decision::state);
    }

    /**
     * Execute {@code commands} and emit the folded state after the decision.
     */
    public <C, S> Mono<S> executeAndReturnState(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).map(Decider.Decision::state);
    }

    /**
     * Execute a single command and emit the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).map(Decider.Decision::events);
    }

    /**
     * Execute {@code commands} and emit the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Mono<List<E>> executeAndReturnEvents(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).map(Decider.Decision::events);
    }
}
