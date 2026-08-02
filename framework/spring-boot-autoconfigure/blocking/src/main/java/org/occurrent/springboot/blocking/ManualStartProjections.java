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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds the startup work a {@code @Projection(source = PUSH)} would otherwise have run at boot, withheld because
 * {@code occurrent.subscription.mode} is {@code manual}. Such a projection is fed by a
 * {@code PushSubscriptionModel} or {@code DomainEventFeed} bean the application supplies, not by the framework's own
 * {@code SubscriptionModel}, so the withholding that mode applies to that bean never reaches it. This registry is
 * what withholds it instead: inject it and bring one projection up with {@link #start(String)}, or every withheld one
 * with {@link #startAll()}, once the application is ready to run them.
 * <p>
 * Starting an id a second time, or one that was never withheld (for example because {@code occurrent.subscription.mode}
 * is {@code auto} and the projection already ran at boot), is a no-op rather than an error, so a caller does not need
 * to track what it already started.
 */
@NullMarked
public final class ManualStartProjections {

    private final Map<String, Runnable> pending = new LinkedHashMap<>();

    /**
     * Record the startup work for {@code id}, to run once {@link #start(String)} or {@link #startAll()} is called.
     * Called by the annotation processor while registering a withheld projection, not normally by application code.
     *
     * @throws IllegalArgumentException if {@code id} is already registered
     */
    void register(String id, Runnable startup) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(startup, "startup cannot be null");
        synchronized (pending) {
            if (pending.containsKey(id)) {
                throw new IllegalArgumentException("A projection with id '" + id + "' is already registered for manual start.");
            }
            pending.put(id, startup);
        }
    }

    /**
     * Start the projection registered under {@code id}. Does nothing if it was already started, or if no projection
     * is withheld under that id.
     */
    public void start(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        Runnable startup;
        synchronized (pending) {
            startup = pending.remove(id);
        }
        if (startup != null) {
            startup.run();
        }
    }

    /**
     * Start every projection still withheld, in the order each was registered.
     *
     * @return the ids started, in that order, empty if none were withheld
     */
    public List<String> startAll() {
        List<String> ids = pendingIds();
        ids.forEach(this::start);
        return ids;
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
