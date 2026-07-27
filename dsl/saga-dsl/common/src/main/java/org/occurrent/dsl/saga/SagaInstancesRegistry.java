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

/**
 * The {@link SagaInstances} of every registered saga, keyed by saga id. This is what an application injects when it
 * observes more than one saga, or when it wants to enumerate the sagas that are running rather than hardcode their ids
 * (a progress dashboard, say).
 * <p>
 * On the Spring stack the {@code @Saga} registrar fills this in, and each saga's {@link SagaInstances} is also published
 * under its own bean name so a {@code getBean} or {@code @Qualifier} lookup reaches it directly. The two paths are
 * equivalent, this one is typed and injectable, the named singleton is convenient when the id is already known.
 * <p>
 * Like {@link SagaInstances}, this is read-only. Registering a saga is the framework's job and has no legitimate caller
 * in an application, so the method that does it is not on this interface at all: it lives on the implementation in
 * {@code org.occurrent.dsl.saga.internal}, which an application never references.
 *
 * <h2>It is empty until the sagas have been registered</h2>
 * A {@code @Saga} factory method can only run once the beans it collaborates with are wired, so the scan that populates
 * the registry happens <em>after</em> the application context has refreshed. A caller that reads it while another bean
 * is still being constructed therefore sees it empty, and that is inherent rather than a defect: the sagas genuinely do
 * not exist yet at that point.
 * <p>
 * In practice this is not a constraint, because anything that observes a saga instance runs in response to a request, a
 * schedule or a health check, all of which happen long after refresh. Injecting this registry into a constructor is
 * fine, <em>reading</em> it from a constructor is not.
 *
 * @see SagaInstances
 */
public interface SagaInstancesRegistry {

    /**
     * The instances of the saga with {@code sagaId}, or empty when no such saga is registered. Prefer
     * {@link #get(String)} when the id is a constant in your code, so a typo fails loudly instead of looking like a
     * saga that has not started.
     */
    Optional<SagaInstances> find(String sagaId);

    /**
     * The instances of the saga with {@code sagaId}.
     * <p>
     * Both this and {@link #find(String)} exist because the two callers are genuinely different. Code holding a constant
     * id has a bug if that id is unknown, and should say so at the point of the mistake, which is why this throws and
     * names every id that <em>is</em> registered. Code resolving an id from a request or a configuration value has no
     * bug when it misses, and should use {@link #find(String)}.
     *
     * @throws IllegalArgumentException if no saga with that id is registered, naming the ids that are registered
     */
    SagaInstances get(String sagaId);

    /**
     * The ids of every registered saga, so a caller can enumerate sagas instead of hardcoding their ids. Unordered, and
     * an immutable snapshot of the ids registered when the call was made. The returned set never changes, but a later
     * call can report more ids, because sagas register after the context has refreshed.
     */
    Set<String> sagaIds();
}
