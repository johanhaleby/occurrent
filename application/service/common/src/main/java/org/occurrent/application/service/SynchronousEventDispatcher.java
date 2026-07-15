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

package org.occurrent.application.service;

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
     * @return {@code true} if at least one synchronous subscription is registered. When {@code false} the
     * application service does no synchronous-dispatch work at all for a write.
     */
    boolean hasSubscriptions();
}
