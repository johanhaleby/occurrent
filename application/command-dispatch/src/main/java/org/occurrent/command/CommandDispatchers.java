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

package org.occurrent.command;

import org.occurrent.application.composition.command.ListCommandComposition;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Ready-made {@link CommandDispatcher}s. A dispatcher is usually just a lambda over an {@code ApplicationService}, with or
 * without a decider, so these are conveniences, not the only way. The {@link #decider} adapter bridges a saga's commands
 * into the existing decider machinery, and {@link #invocation} covers the decider-free path where the command carries the
 * domain function itself.
 * <p>
 * Both factories need types from modules that {@code occurrent-command-dispatch} declares optional, so a consumer using
 * only {@link CommandDispatcher} and {@link StreamIdResolver} is unaffected but a caller of these methods must have the
 * matching module on the classpath: {@code occurrent-decider} for {@link #decider}, and
 * {@code occurrent-application-service-blocking} for {@link #invocation}. Without it the call fails with
 * {@code NoClassDefFoundError}.
 */
public final class CommandDispatchers {

    private CommandDispatchers() {
    }

    /**
     * A dispatcher that runs each command through {@code decider} on the stream {@code streamIdOf} derives from the
     * command, via {@code applicationService}. Because the decider re-folds the authoritative stream, a duplicated or
     * stale command is rejected by the decider's own rules, which is what makes the executor's at-least-once dispatch
     * safe.
     *
     * @param applicationService the decider-backed application service to execute against
     * @param decider            the decider handling the saga's commands
     * @param streamIdOf         derives the target stream id from a command
     * @param <C>                the command type
     * @param <E>                the event type of the application service and decider
     */
    public static <C, E> CommandDispatcher<C> decider(DeciderApplicationService<E> applicationService,
                                                      Decider<C, ?, E> decider,
                                                      StreamIdResolver<C> streamIdOf) {
        requireNonNull(applicationService, "applicationService cannot be null");
        requireNonNull(decider, "decider cannot be null");
        requireNonNull(streamIdOf, "streamIdOf cannot be null");
        return command -> applicationService.execute(streamIdOf.streamId(command), command, decider);
    }

    /**
     * A dispatcher for commands that carry their own handling logic. Each {@link Invocation} names a stream and a domain
     * function, and this runs that function through {@code applicationService} against that stream. Use it when the
     * domain model is plain functions rather than command objects and deciders, so nothing has to invent a command
     * record or {@code switch} over one.
     * <p>
     * Because the application service re-reads the stream before the function decides, a duplicated or stale invocation
     * is rejected by the domain's own rules, which is what makes the executor's at-least-once dispatch safe. The same
     * property the {@link #decider} path relies on.
     * <p>
     * {@link CommandDispatcher#dispatchAll(List)} folds <i>consecutive</i> invocations targeting the same stream into a
     * single {@code execute}, using {@link ListCommandComposition#composeCommands(List)} so each function sees the
     * events the ones before it decided. One reaction issuing two invocations to one stream therefore produces one
     * atomic append rather than two, which is what the batch seam exists for. Order is never rearranged to make groups
     * larger, since dispatch is contractually in order, so two invocations separated by one to a different stream stay
     * three separate appends.
     *
     * @param applicationService the application service to execute each invocation's decision against
     * @param <E>                the event type of the streams being written to
     */
    public static <E> CommandDispatcher<Invocation<E>> invocation(ApplicationService<E> applicationService) {
        requireNonNull(applicationService, "applicationService cannot be null");
        return new CommandDispatcher<>() {
            @Override
            public void dispatch(Invocation<E> invocation) {
                requireNonNull(invocation, "invocation cannot be null");
                applicationService.execute(invocation.streamId(), invocation.decision());
            }

            @Override
            public void dispatchAll(List<Invocation<E>> invocations) {
                requireNonNull(invocations, "invocations cannot be null");
                int groupStart = 0;
                while (groupStart < invocations.size()) {
                    String streamId = invocations.get(groupStart).streamId();
                    int groupEnd = groupStart + 1;
                    while (groupEnd < invocations.size() && invocations.get(groupEnd).streamId().equals(streamId)) {
                        groupEnd++;
                    }
                    applicationService.execute(streamId, decisionFor(invocations.subList(groupStart, groupEnd)));
                    groupStart = groupEnd;
                }
            }

            private Function<List<E>, List<E>> decisionFor(List<Invocation<E>> group) {
                if (group.size() == 1) {
                    return group.get(0).decision();
                }
                List<Function<List<E>, List<E>>> decisions = new ArrayList<>(group.size());
                for (Invocation<E> invocation : group) {
                    decisions.add(invocation.decision());
                }
                return ListCommandComposition.composeCommands(decisions);
            }
        };
    }
}
