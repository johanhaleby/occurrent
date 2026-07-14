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

package org.occurrent.dsl.decider;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.eventstore.api.WriteResult;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * A thin facade over a blocking {@link ApplicationService} that runs a {@link Decider}, the Java counterpart to the
 * Kotlin {@code execute(streamId, command, decider)} extension in {@code ApplicationServiceDeciderExtensions.kt}.
 * Construct it once around an existing application service and call {@link #execute} with a command and a decider.
 * <p>
 * The decider's event type must be the same as the application service's event type {@code E}. A feature decider whose
 * event type is narrower than {@code E} should first be widened with {@link Decider#adapt(Decider, Class, Class)} (or
 * combined with {@link Decider#compose}, which already yields a decider over {@code E}).
 */
@NullMarked
public final class DeciderApplicationService<E> {

    private final ApplicationService<E> applicationService;

    public DeciderApplicationService(ApplicationService<E> applicationService) {
        this.applicationService = Objects.requireNonNull(applicationService, "applicationService cannot be null");
    }

    /**
     * Execute a single command against {@code streamId}, using {@code decider} to decide the new events.
     */
    public <C, S extends @Nullable Object> WriteResult execute(String streamId, C command, Decider<C, S, E> decider) {
        return execute(streamId, List.of(command), decider);
    }

    /**
     * Execute a single command against {@code streamId}, using {@code decider} to decide the new events.
     */
    public <C, S extends @Nullable Object> WriteResult execute(UUID streamId, C command, Decider<C, S, E> decider) {
        return execute(streamId.toString(), command, decider);
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, using {@code decider} to decide the new events.
     */
    public <C, S extends @Nullable Object> WriteResult execute(String streamId, List<C> commands, Decider<C, S, E> decider) {
        return applicationService.execute(streamId, events -> decider.decideOnEventsAndReturnEvents(events, commands));
    }

    /**
     * Execute {@code commands} in order against {@code streamId}, using {@code decider} to decide the new events.
     */
    public <C, S extends @Nullable Object> WriteResult execute(UUID streamId, List<C> commands, Decider<C, S, E> decider) {
        return execute(streamId.toString(), commands, decider);
    }
}
