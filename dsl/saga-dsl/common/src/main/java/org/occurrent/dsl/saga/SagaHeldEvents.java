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

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * Storage for the events a saga instance holds, implemented by the same class as its {@link SagaStateStore}. An instance
 * holds an event in three cases. A non-start event that arrives before the instance has started is held until the start
 * has applied, because acknowledging it would lose its reaction. A non-start event on the stream that started the
 * instance, below the start's version, is held until a start event below it applies, or the instance completes. A version
 * that failed on a started instance and could not be listed as pending on it is held too, so it is applied once the
 * instance can take it.
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
     * Record that the held event with {@code key} first failed at {@code at}, unless it says so already, and never insert.
     * Quarantine measures the event's budget from it.
     */
    void failing(String sagaId, String key, Instant at);

    /** Every event held for {@code sagaId}, without the bodies, in no particular order. */
    List<HeldEventRef> heldIndex(String sagaId);

    /**
     * {@link #heldIndex} without the events that wait for a lower start: those on the stream of {@code start}, below its
     * version, that are not start events. An instance with a long history before its start holds one per earlier event,
     * and a store that can filter on the server should, because every attempt on the instance reads this.
     */
    default List<HeldEventRef> heldIndexWithoutWaiting(String sagaId, SagaEnvelope.StartMark start) {
        return heldIndex(sagaId).stream()
                .filter(ref -> ref.startEvent() || ref.version() == null || !start.streamId().equals(ref.streamId()) || ref.version() >= start.version())
                .toList();
    }

    /** The held events of {@code sagaId} with these keys. A key that is not held is left out of the answer. */
    List<HeldEvent> held(String sagaId, Collection<String> keys);

    /** Stop holding the event with {@code key} for {@code sagaId}. Releasing an event that is not held does nothing. */
    void release(String sagaId, String key);
}
