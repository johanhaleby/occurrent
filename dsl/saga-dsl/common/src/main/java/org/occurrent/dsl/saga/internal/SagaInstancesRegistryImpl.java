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

package org.occurrent.dsl.saga.internal;

import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaInstancesRegistry;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * The mutable {@link SagaInstancesRegistry} the framework populates as it registers each saga.
 * <p>
 * This type is {@code public} only so that the annotation registrar, which lives in a starter module, can construct it
 * and call {@link #register(String, SagaInstances)}. It is not a user-facing API. An application injects
 * {@link SagaInstancesRegistry}, whose read-only surface is the whole point: there is no legitimate reason for
 * application code to add a saga to the registry, so that operation is not reachable from the interface at all rather
 * than being public with a comment asking callers not to use it.
 * <p>
 * Registration happens on the thread that finishes refreshing the application context, while reads happen later on
 * request threads, so the backing map is concurrent.
 */
public final class SagaInstancesRegistryImpl implements SagaInstancesRegistry {

    private final ConcurrentMap<String, SagaInstances> instancesBySagaId = new ConcurrentHashMap<>();

    @Override
    public Optional<SagaInstances> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(instancesBySagaId.get(sagaId));
    }

    @Override
    public SagaInstances get(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        SagaInstances instances = instancesBySagaId.get(sagaId);
        if (instances == null) {
            throw new IllegalArgumentException("No saga is registered with id '%s'. Registered saga ids: %s. Note that sagas are registered after the application context has refreshed, so this is also empty when read while another bean is still being constructed.".formatted(sagaId, describeRegisteredIds()));
        }
        return instances;
    }

    @Override
    public Set<String> sagaIds() {
        return Set.copyOf(instancesBySagaId.keySet());
    }

    /**
     * Registers {@code instances} under {@code sagaId}.
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
