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

package org.occurrent.springboot.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Holds the startup work a {@code @Projection(source = PUSH)} would otherwise have run at boot, withheld because
 * {@code occurrent.subscription.mode} is {@code manual}. Such a projection is fed by a
 * {@code PushSubscriptionModel} or {@code DomainEventFeed} bean the application supplies, not by the framework's own
 * subscription model, so the withholding that mode applies to that bean never reaches it. This registry is what
 * withholds it instead. Inject it and bring one projection up with {@link #start(String)}, or every withheld one with
 * {@link #startAll()}, once the application is ready to run them.
 * <p>
 * Starting an id a second time, or one that was never withheld (for example because {@code occurrent.subscription.mode}
 * is {@code auto} and the projection already ran at boot), completes without doing anything rather than failing, so a
 * caller does not need to track what it already started.
 * <p>
 * The reactor twin of the blocking {@code ManualStartPushSources}, differing in that the startup work runs when the
 * returned {@link Mono} is subscribed rather than when the method is called. The blocking one also withholds a
 * {@code @Saga(source = PUSH)}, which this one has no equivalent of because {@code @Saga} is blocking-only. The name is
 * kept in step with it anyway, so the two stacks do not diverge over a difference that is not theirs.
 */
@NullMarked
public final class ManualStartPushSources {

    private final Map<String, Supplier<Mono<Void>>> pending = new LinkedHashMap<>();

    /**
     * Record the startup work for {@code id}, to run once {@link #start(String)} or {@link #startAll()} is called and
     * subscribed. Called by the annotation processor while registering a withheld projection, not normally by
     * application code.
     *
     * @throws DuplicateSubscriptionIdException if {@code id} is already registered
     */
    void register(String id, Supplier<Mono<Void>> startup) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(startup, "startup cannot be null");
        synchronized (pending) {
            if (pending.containsKey(id)) {
                throw new DuplicateSubscriptionIdException(id, "A projection with id '" + id + "' is already registered for manual start.");
            }
            pending.put(id, startup);
        }
    }

    /**
     * Start the projection registered under {@code id}. The returned {@link Mono} completes once it has caught up and
     * gone live, and completes without doing anything if it was already started or if no projection is withheld under
     * that id.
     */
    public Mono<Void> start(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        return startAndReport(id).then();
    }

    /**
     * Start every projection still withheld, one after another, in the order each was registered.
     *
     * @return The ids this call started, in that order, empty if none were withheld. An id another caller claimed
     * first is left out, so the list says what happened rather than what was pending when the call began.
     */
    public Mono<List<String>> startAll() {
        return Flux.defer(() -> Flux.fromIterable(pendingIds()))
                .concatMap(this::startAndReport)
                .collectList();
    }

    // Emits the id when this call was the one that claimed it, and nothing when it was already started or was never
    // withheld. Claimed on subscribe rather than when the Mono is built, so one that is built and never subscribed
    // leaves the projection withheld instead of dropping its startup work.
    private Mono<String> startAndReport(String id) {
        return Mono.defer(() -> {
            final Supplier<Mono<Void>> startup;
            synchronized (pending) {
                startup = pending.remove(id);
            }
            return startup == null ? Mono.empty() : startup.get().then(Mono.just(id));
        });
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
