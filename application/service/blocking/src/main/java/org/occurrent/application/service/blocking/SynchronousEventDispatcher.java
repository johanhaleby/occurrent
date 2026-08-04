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

package org.occurrent.application.service.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

import java.util.List;

/**
 * The application-service seam for invoking synchronous subscriptions after a write.
 * <p>
 * This tiny interface lives in the application-service layer so the application service can dispatch the
 * events it just wrote to synchronous subscription handlers without depending on the subscription modules
 * (which sit above it). The synchronous subscription model implements it.
 * <p>
 * {@link #hasSubscriptions()} lets the application service skip the extra work of preparing enriched events
 * entirely when nothing is registered, so an application that wires a dispatcher but has registered no
 * synchronous subscriptions pays no per-write cost.
 */
@NullMarked
public interface SynchronousEventDispatcher {

    /**
     * Dispatch the just-written cloud events to every matching synchronous subscription, synchronously, on the
     * calling thread. A handler exception propagates to the caller.
     *
     * @param writtenCloudEvents The cloud events written by the current command, enriched with stream metadata.
     */
    void dispatch(List<CloudEvent> writtenCloudEvents);

    /**
     * Dispatch as {@link #dispatch(List)} does, told whether the write and the handlers are running inside a
     * transaction. This is the overload the application service calls.
     * <p>
     * When {@code transactional} is {@code true} a handler failure rolls the write back, so stopping at the first one
     * loses nothing. When it is {@code false} the write has already committed, so every handler should be given the
     * event and the failures reported together, because a synchronous subscription has no replay and a handler skipped
     * because a sibling threw would never see that event.
     * <p>
     * <strong>Override this if you fan out to several handlers.</strong> The default ignores the flag and delegates to
     * {@link #dispatch(List)}, so an implementation that does not override it keeps stopping at the first failure even
     * when there is no transaction to roll the write back. That is source-compatible but it is the behaviour the
     * 2026-08-04 amendment to ADR 57 exists to correct, and Occurrent cannot correct it on your behalf, since your
     * implementation owns the dispatch loop.
     *
     * @param writtenCloudEvents The cloud events written by the current command, enriched with stream metadata.
     * @param transactional      Whether the caller wrapped this dispatch in a transaction.
     */
    default void dispatch(List<CloudEvent> writtenCloudEvents, boolean transactional) {
        dispatch(writtenCloudEvents);
    }

    /**
     * @return {@code true} if at least one synchronous subscription is registered. When {@code false} the
     * application service does no synchronous-dispatch work at all for a write.
     */
    boolean hasSubscriptions();
}
