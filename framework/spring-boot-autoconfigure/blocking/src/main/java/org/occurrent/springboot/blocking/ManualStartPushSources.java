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
import java.util.function.BooleanSupplier;

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
 * <p>
 * A push source refused because the application context has begun closing is left out of {@link #startAll()} and is
 * gone from {@link #pendingIds()} too. It is dropped rather than put back for a later {@link #start(String)}, since
 * such a context never reopens and the withheld work may already have subscribed or registered before it read the
 * flag, so a retry would repeat that work rather than resume it.
 */
@NullMarked
public final class ManualStartPushSources {

    private final Map<String, BooleanSupplier> pending = new LinkedHashMap<>();

    /**
     * Record the startup work for {@code id}, to run once {@link #start(String)} or {@link #startAll()} is called.
     * The work reports whether it brought the push source up, and answers false when it refused because the
     * application context has begun closing. Called by the annotation processor while registering a withheld push
     * source, not normally by application code.
     *
     * @throws DuplicateSubscriptionIdException if {@code id} is already registered
     */
    void register(String id, BooleanSupplier startup) {
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
     * first is left out, as is one refused because the application context has begun closing, so the list says what
     * happened rather than what was pending when the call began
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

    // True when this call claimed the id and the work it ran brought the push source up. False when the id was
    // already started, was never withheld, or the work refused because the context is closing.
    private boolean startAndReport(String id) {
        final BooleanSupplier startup;
        synchronized (pending) {
            startup = pending.remove(id);
        }
        return startup != null && startup.getAsBoolean();
    }

    /**
     * The ids still withheld, awaiting {@link #start(String)}, in registration order. One refused because the
     * application context has begun closing is not among them, since a retry would be refused too.
     */
    public List<String> pendingIds() {
        synchronized (pending) {
            return List.copyOf(pending.keySet());
        }
    }
}
