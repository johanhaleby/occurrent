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

package org.occurrent.dsl.dcb.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thin facade over a blocking {@link DcbApplicationService} that runs a {@link DcbDecider}, the Java counterpart to
 * the Kotlin {@code execute(command, dcbDecider)} extension in {@code DcbApplicationServiceDeciderExtensions.kt}.
 * Construct it once around an existing DCB application service and call {@link #execute} with a command and a decider.
 * <p>
 * The {@code DcbDecider} carries all three pieces DCB execution needs: the decision function, the {@link DcbCriteria}
 * read boundary derived from the command, and the {@link org.occurrent.application.service.dcb.TagGenerator} for the
 * events it writes. This facade resolves the boundary, routes the decider's tags through {@link DcbExecuteOptions}, and
 * runs the decision, so a Java caller does not have to wire those by hand.
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
     * Execute a single command using {@code dcbDecider} to resolve the read boundary, decide, and tag the new events.
     * Returns the {@link DcbAppendResult}, or {@link Optional#empty()} when the decider produced no new events. Throws
     * {@link IllegalArgumentException} when the command is not recognized by the decider.
     */
    public <C, S extends @Nullable Object> Optional<DcbAppendResult> execute(C command, DcbDecider<C, S, E> dcbDecider) {
        return execute(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} in order using {@code dcbDecider}. All commands must resolve to the same read boundary
     * since they are appended atomically under one condition. Returns the {@link DcbAppendResult}, or
     * {@link Optional#empty()} when the decider produced no new events.
     */
    public <C, S extends @Nullable Object> Optional<DcbAppendResult> execute(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return execute(dcbDecider.criteriaFor(commands), commands, dcbDecider);
    }

    /**
     * Execute {@code commands} in order under the boundary {@code criteria}, using {@code dcbDecider} to decide and tag
     * the new events. For a caller that has already resolved the boundary and would otherwise make the decider derive
     * it a second time, such as a dispatcher that grouped a batch by boundary before executing each group.
     * <p>
     * {@code criteria} is expected to be the boundary {@code dcbDecider} resolves for these commands. This is not
     * enforced here, unlike {@link #execute(List, DcbDecider)}, which derives the boundary itself and rejects a batch
     * whose commands disagree. Passing a boundary the commands do not resolve to reads the wrong events and appends
     * under the wrong condition, which can under-scope the DCB append condition without anything raising an error.
     * Prefer {@link #execute(List, DcbDecider)} unless the boundary came from {@code dcbDecider} for these same
     * commands.
     *
     * @param criteria    the read boundary to read under and append against
     * @param commands    the commands to execute in order
     * @param dcbDecider  the decider that decides and tags the new events
     */
    public <C, S extends @Nullable Object> Optional<DcbAppendResult> execute(DcbCriteria criteria, List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        DcbExecuteOptions<E> options = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
        return applicationService.execute(criteria, options, events -> dcbDecider.decider().decideOnEventsAndReturnEvents(events, commands));
    }

    /**
     * Execute a single command and return the folded state plus the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(List.of(command), dcbDecider);
    }

    /**
     * Execute {@code commands} and return the folded state plus the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> Decider.Decision<S, E> executeAndReturnDecision(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        DcbCriteria criteria = dcbDecider.criteriaFor(commands);
        DcbExecuteOptions<E> options = DcbExecuteOptions.<E>options().tagGenerator(dcbDecider.tags());
        AtomicReference<Decider.Decision<S, E>> decision = new AtomicReference<>();
        applicationService.execute(criteria, options, events -> {
            Decider.Decision<S, E> result = dcbDecider.decider().decideOnEvents(events, commands);
            decision.set(result);
            return result.events();
        });
        return Objects.requireNonNull(decision.get(), "The decider produced no decision");
    }

    /**
     * Execute a single command and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).state();
    }

    /**
     * Execute {@code commands} and return the folded state after the decision.
     */
    public <C, S extends @Nullable Object> S executeAndReturnState(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).state();
    }

    /**
     * Execute a single command and return the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(C command, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(command, dcbDecider).events();
    }

    /**
     * Execute {@code commands} and return the new events decided by {@code dcbDecider}.
     */
    public <C, S extends @Nullable Object> List<E> executeAndReturnEvents(List<C> commands, DcbDecider<C, S, E> dcbDecider) {
        return executeAndReturnDecision(commands, dcbDecider).events();
    }
}
