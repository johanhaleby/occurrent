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

package org.occurrent.dsl.saga.blocking;

import org.occurrent.command.CommandDispatcher;
import org.occurrent.command.StreamIdResolver;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;

import static java.util.Objects.requireNonNull;

/**
 * Ready-made {@link CommandDispatcher}s. A dispatcher is usually just a lambda over an {@code ApplicationService}, with or
 * without a decider, so these are conveniences, not the only way. The {@link #decider} adapter bridges a saga's commands
 * into the existing decider machinery. The non-decider path is a plain lambda the caller writes directly.
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
}
