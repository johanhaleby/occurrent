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

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Read-only access to one saga's instances, for answering operational questions: is this instance still running, which
 * step is it on, has it stopped moving. It hands back {@link SagaInstance}, so a caller sees the lifecycle without the
 * executor's delivery bookkeeping and without the saga's own state.
 * <p>
 * Obtain one from {@code SagaSubscription.instances()} when running a saga programmatically. On the Spring stack the
 * {@code @Saga} registrar publishes one per saga under the bean name {@code sagaInstances-<id>}.
 * <p>
 * This deliberately offers no way to write. Nothing here can start, advance, complete or delete an instance: the
 * executor owns those transitions, and a compare-and-set save from outside it would race the subscription and the timer
 * poller. Retention tooling that really has to remove an instance uses {@link SagaStateStore#delete(String)} directly.
 * <p>
 * It is not generic. Every accessor on {@link SagaInstance} is independent of the saga's state type, so parameterizing
 * this on {@code S} would only force callers to write a wildcard.
 */
public final class SagaInstances {

    private final SagaStateStore<?> stateStore;

    private SagaInstances(SagaStateStore<?> stateStore) {
        this.stateStore = requireNonNull(stateStore, "stateStore cannot be null");
    }

    /** Observes the instances held in {@code stateStore}. */
    public static SagaInstances of(SagaStateStore<?> stateStore) {
        return new SagaInstances(stateStore);
    }

    /** The instance with {@code sagaId}, or empty when the saga has never seen that correlation id. */
    public Optional<SagaInstance> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return stateStore.find(sagaId).map(SagaInstances::asInstance);
    }

    /**
     * Instances with {@code status} last updated before {@code updatedBefore}, stalest first, at most {@code limit}.
     * <p>
     * Pass {@code Instant.now()} to list everything in a status, or {@code Instant.now().minus(threshold)} to find the
     * instances that have gone quiet for longer than {@code threshold}. The full contract, including why {@code limit}
     * is a bound rather than a page, is on {@link SagaStateStore#findByStatus(SagaStatus, Instant, int)}.
     * <p>
     * <strong>This is not a cheap call for a flow saga, so do not poll it at subscription frequency.</strong> Instances
     * come back whole because {@link SagaInstance#currentStep()} is read off the state, and a flow saga's state carries
     * its received events. {@code limit} bounds the number of <em>instances</em>, not the work per instance: with
     * {@code limit} 100 against the default 100-event history window that is on the order of ten thousand CloudEvent
     * deserializations. Size it for a dashboard refresh or a periodic stuck-instance sweep.
     */
    public List<SagaInstance> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        requireNonNull(status, "status cannot be null");
        requireNonNull(updatedBefore, "updatedBefore cannot be null");
        return stateStore.findByStatus(status, updatedBefore, limit).stream()
                .map(SagaInstances::asInstance)
                .toList();
    }

    // Widens an envelope to the view an observing caller is handed. A method rather than a cast at each call site: in
    // findByStatus the map sits in qualifier position ahead of toList(), so the lambda's type is inferred standalone as
    // SagaEnvelope and the chain needs an explicit target type to compile at all. Naming the widening once keeps both
    // lookups reading the same way.
    private static SagaInstance asInstance(SagaEnvelope<?> envelope) {
        return envelope;
    }
}
