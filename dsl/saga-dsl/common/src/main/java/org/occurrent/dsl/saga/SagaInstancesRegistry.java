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

package org.occurrent.dsl.saga;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * The {@link SagaInstances} of every registered saga, keyed by saga id. This is what an application injects when it
 * observes more than one saga, or when it wants to enumerate the sagas that are running rather than hardcode their ids
 * (a progress dashboard, say).
 * <p>
 * On the Spring stack the {@code @Saga} registrar fills this in, and each saga's {@link SagaInstances} is also published
 * under its own bean name so a {@code getBean} or {@code @Qualifier} lookup reaches it directly. The two paths are
 * equivalent; this one is typed and injectable, the named singleton is convenient when the id is already known.
 *
 * <h2>It is empty until the sagas have been registered</h2>
 * A {@code @Saga} factory method can only run once the beans it collaborates with are wired, so the scan that populates
 * this registry happens <em>after</em> the application context has refreshed. A caller that reads the registry while
 * another bean is still being constructed therefore sees it empty, and that is inherent rather than a defect: the sagas
 * genuinely do not exist yet at that point.
 * <p>
 * In practice this is not a constraint, because anything that observes a saga instance runs in response to a request,
 * a schedule or a health check, all of which happen long after refresh. Injecting this registry into a constructor is
 * fine; <em>reading</em> it from a constructor is not.
 *
 * @see SagaInstances
 */
public final class SagaInstancesRegistry {

    private final ConcurrentMap<String, SagaInstances> instancesBySagaId = new ConcurrentHashMap<>();

    /**
     * The instances of the saga with {@code sagaId}, or empty when no such saga is registered. Prefer
     * {@link #get(String)} when the id is a constant in your code, so a typo fails loudly instead of looking like a
     * saga that has not started.
     */
    public Optional<SagaInstances> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(instancesBySagaId.get(sagaId));
    }

    /**
     * The instances of the saga with {@code sagaId}.
     * <p>
     * Both this and {@link #find(String)} exist because the two callers are genuinely different. Code holding a
     * constant id has a bug if that id is unknown, and should say so at the point of the mistake, which is why this
     * throws and names every id that <em>is</em> registered. Code resolving an id from a request or a configuration
     * value has no bug when it misses, and should use {@link #find(String)}.
     *
     * @throws IllegalArgumentException if no saga with that id is registered, listing the ids that are
     */
    public SagaInstances get(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        SagaInstances instances = instancesBySagaId.get(sagaId);
        if (instances == null) {
            throw new IllegalArgumentException("No saga is registered with id '%s'. Registered saga ids: %s. Note that sagas are registered after the application context has refreshed, so this is also empty when read while another bean is still being constructed.".formatted(sagaId, describeRegisteredIds()));
        }
        return instances;
    }

    /**
     * The ids of every registered saga, so a caller can enumerate sagas instead of hardcoding their ids. Unordered, and
     * a snapshot: it does not change as more sagas register.
     */
    public Set<String> sagaIds() {
        return Set.copyOf(instancesBySagaId.keySet());
    }

    /**
     * Registers {@code instances} under {@code sagaId}.
     * <p>
     * Called by the framework as it registers each saga, not by an application. It is public only because the
     * {@code @Saga} registrar lives in another module.
     *
     * @throws IllegalArgumentException if {@code sagaId} is already registered
     */
    public void register(String sagaId, SagaInstances instances) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(instances, "instances cannot be null");
        SagaInstances previous = instancesBySagaId.putIfAbsent(sagaId, instances);
        if (previous != null) {
            // The annotation registrar already rejects a duplicate id against its shared registry, so reaching this
            // means two registrations raced or bypassed that check. Either way, silently keeping one of them would make
            // a lookup return the wrong saga's instances.
            throw new IllegalArgumentException("A saga with id '%s' is already registered.".formatted(sagaId));
        }
    }

    private String describeRegisteredIds() {
        Set<String> ids = sagaIds();
        return ids.isEmpty() ? "none" : ids.stream().sorted().toList().toString();
    }
}
