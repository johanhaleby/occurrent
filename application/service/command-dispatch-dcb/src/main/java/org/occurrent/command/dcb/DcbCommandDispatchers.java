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

import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.dcb.blocking.DcbDeciderApplicationService;

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
     * A dispatcher that runs each command through {@code dcbDecider} via {@code applicationService}. The decider
     * re-reads the boundary it derives from the command before deciding, so a decider whose rules are idempotent
     * turns a duplicated or stale command into no new events. At-least-once dispatch is therefore safe only to the
     * extent the decider's own rules make it so.
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
        return command -> applicationService.execute(List.of(command), dcbDecider);
    }
}
