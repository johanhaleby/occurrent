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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

/**
 * An in-memory {@link SagaStateStore} backed by a {@link ConcurrentHashMap}, for tests and single-node use. The
 * compare-and-set save is atomic per instance via {@link ConcurrentMap#compute}, and so are the marks
 * {@link SagaHeldEvents} writes on an envelope. Held events live in a second map, keyed by instance and then by key.
 *
 * @param <S> the user state type
 */
public final class InMemorySagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S>, SagaHeldEvents<S> {

    private final ConcurrentMap<String, SagaEnvelope<S>> store = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<String, HeldEvent>> heldEvents = new ConcurrentHashMap<>();

    @Override
    public Optional<SagaEnvelope<S>> find(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        return Optional.ofNullable(store.get(sagaId));
    }

    @Override
    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(envelope, "envelope cannot be null");
        boolean[] saved = {false};
        store.compute(sagaId, (id, existing) -> {
            long current = existing == null ? 0 : existing.version();
            if (current != expectedVersion) {
                return existing;
            }
            saved[0] = true;
            return envelope;
        });
        return saved[0];
    }

    @Override
    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        requireNonNull(now, "now cannot be null");
        long nowMillis = now.toEpochMilli();
        List<SagaEnvelope<S>> due = new ArrayList<>();
        for (SagaEnvelope<S> envelope : store.values()) {
            // ACTIVE rather than "not completed", so a quarantined instance drops out of the poll and fires no timers.
            // Firing one would advance its state across the gap the quarantine exists to hold open.
            if (envelope.status() != SagaStatus.ACTIVE) {
                continue;
            }
            OptionalLong earliest = envelope.earliestTimerFiresAtEpochMilli();
            if (earliest.isPresent() && earliest.getAsLong() <= nowMillis) {
                due.add(envelope);
                if (due.size() >= limit) {
                    break;
                }
            }
        }
        return due;
    }

    @Override
    public List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        requireNonNull(status, "status cannot be null");
        requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        // Sort before truncating, so "the first limit" is the stalest instances the contract promises. Note that
        // findWithDueTimers above breaks at limit mid-iteration and so returns an arbitrary subset, which this method
        // may not do. That subset is also how an instance whose timer never fires can be returned every time while
        // others wait, which nothing here prevents, see #1003.
        return store.values().stream()
                .filter(envelope -> envelope.status() == status)
                .filter(envelope -> {
                    Instant updatedAt = envelope.updatedAt();
                    return updatedAt != null && updatedAt.isBefore(updatedBefore);
                })
                .sorted(Comparator.comparing(envelope -> requireNonNull(envelope.updatedAt())))
                .limit(limit)
                .toList();
    }

    @Override
    public void delete(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        // Held events first, so a delete that stops half way never keeps held events for an instance it removed.
        heldEvents.remove(sagaId);
        store.remove(sagaId);
    }

    @Override
    public boolean hold(String sagaId, HeldEvent event) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(event, "event cannot be null");
        // Every change to an instance's held events goes through compute on the outer map, so a release that removes the
        // emptied inner map cannot swallow a hold landing in it at the same moment.
        boolean[] inserted = {false};
        heldEvents.compute(sagaId, (id, held) -> {
            ConcurrentMap<String, HeldEvent> events = held == null ? new ConcurrentHashMap<>() : held;
            inserted[0] = events.putIfAbsent(event.key(), event) == null;
            return events;
        });
        return inserted[0];
    }

    @Override
    public void failing(String sagaId, String key, Instant at) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNull(at, "at cannot be null");
        heldEvents.computeIfPresent(sagaId, (id, held) -> {
            held.computeIfPresent(key, (k, event) -> {
                HeldEventRef ref = event.ref();
                return ref.failingSince() != null ? event : new HeldEvent(event.sagaId(), new HeldEventRef(ref.key(), ref.streamId(), ref.version(), ref.position(),
                        ref.type(), ref.heldAt(), ref.startEvent(), at), event.cloudEvent());
            });
            return held;
        });
    }

    @Override
    public List<HeldEventRef> heldIndex(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        ConcurrentMap<String, HeldEvent> held = heldEvents.get(sagaId);
        return held == null ? List.of() : held.values().stream().map(HeldEvent::ref).toList();
    }

    @Override
    public List<HeldEvent> held(String sagaId, Collection<String> keys) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(keys, "keys cannot be null");
        ConcurrentMap<String, HeldEvent> held = heldEvents.get(sagaId);
        if (held == null) {
            return List.of();
        }
        return keys.stream().map(held::get).filter(Objects::nonNull).toList();
    }

    @Override
    public void release(String sagaId, String key) {
        requireNonNull(sagaId, "sagaId cannot be null");
        requireNonNull(key, "key cannot be null");
        heldEvents.computeIfPresent(sagaId, (id, held) -> {
            held.remove(key);
            return held.isEmpty() ? null : held;
        });
    }
}
