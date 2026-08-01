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
import org.occurrent.command.internal.CommandGrouping;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Ready-made {@link CommandDispatcher}s. A dispatcher is usually just a lambda over an {@code ApplicationService}, with or
 * without a decider, so these are conveniences, not the only way. The {@link #decider} adapter bridges a saga's commands
 * into the existing decider machinery, and {@link #invocation} covers the decider-free path where the command carries the
 * domain function itself.
 * <p>
 * A consumer using only {@link CommandDispatcher} and {@link StreamIdResolver} needs nothing extra. Calling either
 * factory needs one module that {@code occurrent-command-dispatch} declares optional, {@code occurrent-decider} for
 * {@link #decider} and {@code occurrent-application-service-blocking} for {@link #invocation}, and without it the call
 * fails with {@code NoClassDefFoundError}. {@code occurrent-command-composition}, which {@link #invocation} folds a
 * batch with, is a required dependency and is always present.
 */
public final class CommandDispatchers {

    private CommandDispatchers() {
    }

    /**
     * A dispatcher that runs a command through {@code decider} on the stream {@code streamIdOf} derives from the
     * command, via {@code applicationService}. Because the decider re-folds the authoritative stream, a duplicated or
     * stale command is rejected by the decider's own rules, which is what makes the executor's at-least-once dispatch
     * safe.
     * <p>
     * {@link CommandDispatcher#dispatchAll(List)} folds a run of <i>consecutive</i> commands targeting the same stream
     * into a single {@code execute}, so a reaction issuing three commands against one stream is one append rather than
     * three. The decider sees them in order and each one decides against what the ones before it decided. Order is
     * preserved, so two commands to one stream separated by one to a different stream stay three separate appends.
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
        return new CommandDispatcher<>() {
            @Override
            public void dispatch(C command) {
                requireNonNull(command, "command cannot be null");
                applicationService.execute(streamIdOf.streamId(command), command, decider);
            }

            @Override
            public void dispatchAll(List<C> commands) {
                CommandGrouping.forEachRun(commands, streamIdOf::streamId,
                        (streamId, group) -> applicationService.execute(streamId, group, decider));
            }
        };
    }

    /**
     * A dispatcher for commands that carry their own handling logic. Each {@link Invocation} names a stream and a
     * domain function, and this runs that function through {@code applicationService} against that stream.
     * <p>
     * Like {@link #decider}, this is safe under at-least-once dispatch, because the application service re-reads the
     * stream before the function decides.
     * <p>
     * {@link CommandDispatcher#dispatchAll(List)} folds <i>consecutive</i> invocations targeting the same stream into a
     * single {@code execute}, using {@link ListCommandComposition#composeCommands(List)} so each function sees the
     * events the ones before it decided. Order is preserved, so two invocations to one stream separated by one to a
     * different stream stay three separate appends.
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
                CommandGrouping.forEachRun(invocations, Invocation::streamId, (streamId, group) ->
                        applicationService.execute(streamId, ListCommandComposition.composeCommands(group.stream().map(Invocation::decision).toList())));
            }
        };
    }
}
