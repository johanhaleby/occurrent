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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.DuplicateSubscriptionIdException;

import java.util.*;

/**
 * Holds the startup work a {@code source = PUSH} registration would otherwise have run at boot, withheld because
 * {@code occurrent.subscription.mode} is {@code manual}. That covers a {@code @Projection(source = PUSH)} and a
 * {@code @Saga(source = PUSH)} alike. Both are fed by a {@code PushSubscriptionModel} or {@code DomainEventFeed} bean
 * the application supplies, not by the framework's own {@code SubscriptionModel}, so the withholding that mode applies
 * to that bean never reaches them. This registry is what withholds them instead. Inject it and bring one up with
 * {@link #start(String)}, or every withheld one with {@link #startAll()}, once the application is ready to run them.
 * <p>
 * One registry rather than one per annotation, because the reason a registration lands here is the push feed and not
 * what is on the other end of it, and because an application bringing its push sources up behind a leader election
 * wants one {@link #startAll()} rather than one per kind. Ids are unique across both, since a {@code @Projection} and a
 * {@code @Saga} already cannot share a subscription id.
 * <p>
 * Starting an id a second time, or one that was never withheld (for example because {@code occurrent.subscription.mode}
 * is {@code auto} and it already ran at boot), is a no-op rather than an error, so a caller does not need to track what
 * it already started.
 */
@NullMarked
public final class ManualStartPushSources {

    private final Map<String, Runnable> pending = new LinkedHashMap<>();

    /**
     * Record the startup work for {@code id}, to run once {@link #start(String)} or {@link #startAll()} is called.
     * Called by the annotation processor while registering a withheld push source, not normally by application code.
     *
     * @throws DuplicateSubscriptionIdException if {@code id} is already registered
     */
    void register(String id, Runnable startup) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(startup, "startup cannot be null");
        synchronized (pending) {
            if (pending.containsKey(id)) {
                throw new DuplicateSubscriptionIdException(id, "A push source with id '" + id + "' is already registered for manual start.");
            }
            pending.put(id, startup);
        }
    }

    /**
     * Start the push source registered under {@code id}. Does nothing if it was already started, or if nothing is
     * withheld under that id.
     */
    public void start(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        startAndReport(id);
    }

    /**
     * Start every push source still withheld, in the order each was registered.
     *
     * @return the ids this call started, in that order, empty if none were withheld. An id another caller claimed
     * first is left out, so the list says what happened rather than what was pending when the call began
     */
    public List<String> startAll() {
        List<String> started = new ArrayList<>();
        for (String id : pendingIds()) {
            if (startAndReport(id)) {
                started.add(id);
            }
        }
        return List.copyOf(started);
    }

    // True when this call was the one that claimed the id, false when it was already started or never withheld.
    private boolean startAndReport(String id) {
        final Runnable startup;
        synchronized (pending) {
            startup = pending.remove(id);
        }
        if (startup == null) {
            return false;
        }
        startup.run();
        return true;
    }

    /**
     * The ids still withheld, awaiting {@link #start(String)}, in registration order.
     */
    public List<String> pendingIds() {
        synchronized (pending) {
            return List.copyOf(pending.keySet());
        }
    }
}
