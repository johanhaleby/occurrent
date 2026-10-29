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

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Storage for the events a saga instance holds, implemented by the same class as its {@link SagaStateStore}. An instance
 * holds an event in two cases. A non-start event that arrives before the instance has started is held until the start
 * has applied, because acknowledging it would lose its reaction. A version that failed on a started instance and could
 * not be listed as pending on it is held too, so it is applied once the instance can take it.
 * <p>
 * {@code SagaRunner.run} refuses a store without this capability when the subscription model does not declare that it
 * delivers every event in commit order, since that is the feed that can deliver an event before its instance's start.
 * <p>
 * Every write here applies to whatever version the envelope is at. None of them is a compare-and-set, because a lost
 * compare-and-set on the failure path would need a retry loop that can lose an event when it gives up.
 *
 * @param <S> the user state type of the store's envelopes
 */
public interface SagaHeldEvents<S extends @Nullable Object> {

    /**
     * Hold {@code event} for {@code sagaId}.
     *
     * @return {@code false} when an event with the same key is already held for the instance, which is a redelivery of
     * one already held
     */
    boolean hold(String sagaId, HeldEvent event);

    /**
     * Mark the held event with {@code key} as outstanding, meaning it is known not to have been applied. Never inserts
     * and never grows the held document.
     *
     * @return {@code false} when no event with that key is held for the instance
     */
    boolean markOutstanding(String sagaId, String key);

    /**
     * Say that the instance has held events to drain, on an envelope that is active and started, whatever version it
     * is at. It unsets the envelope's {@code heldEventsChecked}, raises its version and, when {@code selfHeldKey} is
     * given, adds the key to its {@code selfHeldKeys}.
     *
     * @return the envelope as it was before the write, or empty when no active started envelope matched. A store may
     * leave the state out, as {@link SagaStateStore#findWithoutState(String)} may, and the caller never reads it
     */
    Optional<SagaEnvelope<S>> markHeldEvents(String sagaId, @Nullable String selfHeldKey);

    /** Remove {@code selfHeldKey} from the envelope's {@code selfHeldKeys} and raise its version, whatever version it is at. */
    void unmarkSelfHeld(String sagaId, String selfHeldKey);

    /** Every event held for {@code sagaId}, without the bodies, in no particular order. */
    List<HeldEventRef> heldIndex(String sagaId);

    /** The held events of {@code sagaId} with these keys. A key that is not held is left out of the answer. */
    List<HeldEvent> held(String sagaId, Collection<String> keys);

    /** Stop holding the event with {@code key} for {@code sagaId}. Releasing an event that is not held does nothing. */
    void release(String sagaId, String key);

    /**
     * Thrown by {@link #hold} and {@link #markHeldEvents} when the write would take a document past the store's size
     * limit, so it changed nothing. Retrying it cannot succeed, which is why a store does not retry it.
     */
    final class RefusedForSizeException extends RuntimeException {
        public RefusedForSizeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
