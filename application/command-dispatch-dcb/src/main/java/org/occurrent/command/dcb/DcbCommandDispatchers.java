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

package org.occurrent.command.dcb;

import org.occurrent.application.service.blocking.dcb.DcbApplicationService;
import org.occurrent.application.service.blocking.dcb.DcbExecuteOptions;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.command.internal.CommandGrouping;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.dcb.blocking.DcbDeciderApplicationService;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Ready-made {@link CommandDispatcher}s, the DCB twin of {@code org.occurrent.command.CommandDispatchers}. A {@link DcbDecider}
 * already carries its own {@code DcbCriteria} read boundary and {@code TagGenerator}, so unlike the stream-based
 * {@code CommandDispatchers.decider(...)} there is no separate stream id resolver to supply.
 */
public final class DcbCommandDispatchers {

    private DcbCommandDispatchers() {
    }

    /**
     * A dispatcher that runs a command through {@code dcbDecider} via {@code applicationService}. The decider
     * re-reads the boundary it derives from the command before deciding, so a decider whose rules are idempotent
     * turns a duplicated or stale command into no new events. At-least-once dispatch is therefore safe only to the
     * extent the decider's own rules make it so.
     * <p>
     * {@link CommandDispatcher#dispatchAll(List)} folds a run of <i>consecutive</i> commands resolving to the same
     * {@link DcbCriteria} into a single {@code execute}, so a reaction issuing three commands inside one boundary is
     * one append rather than three. The decider sees them in order and each one decides against what the ones before
     * it decided. Order is preserved, so two commands in one boundary separated by one in a different boundary stay
     * three separate appends.
     * <p>
     * Unlike the stream twin, the boundary is derived from the command by the decider rather than by a resolver, so a
     * command this decider does not recognise fails the whole batch before anything is appended. That holds as long as
     * the decider's criteria function answers the same for the same command, which DCB already requires of it, since
     * the boundary is both the read query and the append condition. Grouping derives it once per command and
     * {@code execute} derives it again for the run it is given, so a function that answered differently the second time
     * could fail a later run after an earlier one had been written.
     * <p>
     * Boundaries are compared by value, and one built from the same criteria in a different order counts as a
     * different boundary, which costs an extra append and never merges two that differ.
     *
     * @param applicationService the DCB decider-backed application service to execute against
     * @param dcbDecider         the decider handling the commands, including its read boundary and tags
     * @param <C>                the command type
     * @param <E>                the event type of the application service and decider
     */
    public static <C, E> CommandDispatcher<C> decider(DcbDeciderApplicationService<E> applicationService,
                                                      DcbDecider<C, ?, E> dcbDecider) {
        requireNonNull(applicationService, "applicationService cannot be null");
        requireNonNull(dcbDecider, "dcbDecider cannot be null");
        return new CommandDispatcher<>() {
            @Override
            public void dispatch(C command) {
                requireNonNull(command, "command cannot be null");
                applicationService.execute(command, dcbDecider);
            }

            @Override
            public void dispatchAll(List<C> commands) {
                CommandGrouping.forEachRun(commands, dcbDecider::criteriaFor,
                        (criteria, group) -> applicationService.execute(group, dcbDecider));
            }
        };
    }

    /**
     * A dispatcher for commands that carry their own handling logic, the DCB twin of
     * {@code CommandDispatchers.invocation(...)}. Each {@link DcbInvocation} names a read boundary and a domain
     * function, and this runs that function through {@code applicationService} inside that boundary.
     * <p>
     * This is the one dispatcher that does not fold a batch. {@link CommandDispatcher#dispatchAll(List)} is left at its
     * default here, because invocations sharing a {@link DcbCriteria} may carry different {@link TagGenerator}s and one
     * append can only be tagged one way. {@link #decider} has no such problem, since a {@link DcbDecider} carries one
     * tag generator for every command it handles.
     *
     * @param applicationService the DCB application service to execute each invocation's decision against
     * @param <E>                the event type of the write model
     */
    public static <E> CommandDispatcher<DcbInvocation<E>> invocation(DcbApplicationService<E> applicationService) {
        requireNonNull(applicationService, "applicationService cannot be null");
        return invocation -> {
            requireNonNull(invocation, "invocation cannot be null");
            TagGenerator<E> tagGenerator = invocation.tagGenerator();
            DcbExecuteOptions<E> options = tagGenerator == null
                    ? DcbExecuteOptions.empty()
                    : DcbExecuteOptions.<E>options().tagGenerator(tagGenerator);
            applicationService.execute(invocation.criteria(), options, invocation.decision());
        };
    }
}
