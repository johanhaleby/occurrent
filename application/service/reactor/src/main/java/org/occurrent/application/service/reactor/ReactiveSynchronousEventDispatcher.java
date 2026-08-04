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

package org.occurrent.application.service.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * The reactive application-service seam for invoking synchronous subscriptions after a write.
 * <p>
 * The reactive counterpart of the blocking {@code SynchronousEventDispatcher}: the reactive synchronous subscription
 * model implements it, and the reactive application service composes {@link #dispatch(List)} into its chain before it
 * emits the {@code WriteResult}, so matching handlers complete before {@code execute} completes.
 */
@NullMarked
public interface ReactiveSynchronousEventDispatcher {

    /**
     * Dispatch the just-written cloud events to every matching synchronous subscription. The returned {@link Mono}
     * completes when all matching handlers have completed, and errors if any handler errors.
     * <p>
     * {@code transactional} says whether the caller opened a transaction around the write and this dispatch, which
     * decides what a handler failure costs the handlers behind it. When it is {@code true} the failure rolls the write
     * back, so nothing is folded by anyone and stopping at the first one loses nothing. When it is {@code false} the
     * write has already committed, so give every handler the event and report the failures together: a synchronous
     * subscription has no replay, and a handler skipped because a sibling errored would never see that event.
     *
     * @param writtenCloudEvents The cloud events written by the current command, enriched with stream metadata.
     * @param transactional      Whether the caller wrapped this dispatch in a transaction.
     * @return A {@link Mono} that completes when dispatch is done.
     */
    Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional);

    /**
     * @return {@code true} if at least one synchronous subscription is registered. When {@code false} the reactive
     * application service does no synchronous-dispatch work at all for a write.
     */
    boolean hasSubscriptions();
}
