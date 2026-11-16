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
 * model implements it, and the reactive application service composes {@link #dispatch(List, boolean)} into its chain
 * before it emits the {@code WriteResult}, so matching handlers complete before {@code execute} completes.
 */
@NullMarked
public interface ReactiveSynchronousEventDispatcher {

    /**
     * Dispatch the just-written cloud events to every matching synchronous subscription. The returned {@link Mono}
     * completes when all matching handlers have completed, and errors if any handler errors.
     * <p>
     * {@code transactional} says whether the write can't commit after a handler fails, because the application service
     * opened the transaction around the write and this dispatch, or joined one that its executor says a failure rolls
     * back. When it is {@code true} the dispatch stops at the first failure and the handlers behind it don't get the
     * event. A store that doesn't join that transaction can still keep the write, and ADR 165 lists where. When it is
     * {@code false} the write may commit, so give the other handlers the event and report the failures together. A
     * synchronous subscription has no replay, and a handler skipped because a sibling errored would never see that event.
     *
     * @param writtenCloudEvents The cloud events written by the current command, enriched with stream metadata.
     * @param transactional      Whether the write can't commit after a handler fails.
     * @return A {@link Mono} that completes when dispatch is done.
     */
    Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional);

    /**
     * @return {@code true} if at least one synchronous subscription is registered. When {@code false} the reactive
     * application service does no synchronous-dispatch work at all for a write.
     */
    boolean hasSubscriptions();

    /**
     * Asked each time the {@link Mono} that {@code execute} returns is subscribed, before anything is read and before
     * {@link #hasSubscriptions()}. It throws a {@link org.occurrent.application.service.WriteRefusedException} when no
     * write may go through now, and the {@link Mono} fails with it. The {@link WriteCheck} it returns is asked inside
     * the chain with the events the command decided on, before they are written, and throws one when a handler that
     * would never see them matches them. Nothing is written when either throws. A refusal is an error signal, so a
     * caller that subscribes without an error consumer stores nothing and gets no exception. The application service
     * closes the check when the {@link Mono} terminates or is cancelled, and a pause or a stop that came after this
     * call waits for that, or applies then when it can't block.
     * <p>
     * The default refuses nothing.
     *
     * @return The check for the events of this write.
     */
    default WriteCheck beforeWrite() {
        return WriteCheck.NONE;
    }

    /**
     * The check {@link #beforeWrite()} returns for the events of one write. It must not block, since it runs inside
     * the reactive chain.
     */
    @FunctionalInterface
    interface WriteCheck extends AutoCloseable {
        /**
         * Refuses nothing.
         */
        WriteCheck NONE = newEvents -> {
        };

        /**
         * @param newEvents The cloud events the command decided on, before the event store has written them
         */
        void check(List<CloudEvent> newEvents);

        /**
         * Called once the write and its dispatch are done, have failed or were cancelled, on whatever thread that
         * happened. It must not block. The default does nothing.
         */
        @Override
        default void close() {
        }
    }
}
