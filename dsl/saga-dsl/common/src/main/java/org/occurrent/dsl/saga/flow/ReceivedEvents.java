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

package org.occurrent.dsl.saga.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The events a flow saga instance has received so far, in arrival order with the initiating event first. It is the only
 * "state" a flow saga has, and is what a reaction, a guard, or a not-fulfilled branch reads. Counts here span the whole
 * flow history (so a retry guard such as {@code count(PaymentFailed.class) < 3} works across a self-looping step); a
 * {@code join} step's own fulfilment is counted separately, over the events received since it was entered.
 *
 * @param <E> the domain event type
 */
public interface ReceivedEvents<E> {

    /** The event that started this saga instance. */
    E initiating();

    /** The initiating event cast to {@code type}. Throws {@link ClassCastException} if it is not of that type. */
    <T extends E> T initiating(Class<T> type);

    /** The first received event of {@code type}, if any. */
    <T extends E> Optional<T> first(Class<T> type);

    /** All received events of {@code type}, in arrival order. */
    <T extends E> List<T> all(Class<T> type);

    /** How many events of {@code type} have been received. */
    <T extends E> int count(Class<T> type);

    /** All received events, in arrival order, initiating first. */
    List<E> asList();

    /** A view over {@code events} (which must be non-empty; element 0 is the initiating event). */
    static <E> ReceivedEvents<E> of(List<E> events) {
        return new ReceivedEventsList<>(events);
    }
}

final class ReceivedEventsList<E> implements ReceivedEvents<E> {
    private final List<E> events;

    ReceivedEventsList(List<E> events) {
        requireNonNull(events, "events cannot be null");
        if (events.isEmpty()) {
            throw new IllegalArgumentException("received events cannot be empty; the initiating event is always present");
        }
        this.events = List.copyOf(events);
    }

    @Override
    public E initiating() {
        return events.get(0);
    }

    @Override
    public <T extends E> T initiating(Class<T> type) {
        return type.cast(initiating());
    }

    @Override
    public <T extends E> Optional<T> first(Class<T> type) {
        for (E event : events) {
            if (type.isInstance(event)) {
                return Optional.of(type.cast(event));
            }
        }
        return Optional.empty();
    }

    @Override
    public <T extends E> List<T> all(Class<T> type) {
        List<T> result = new ArrayList<>();
        for (E event : events) {
            if (type.isInstance(event)) {
                result.add(type.cast(event));
            }
        }
        return List.copyOf(result);
    }

    @Override
    public <T extends E> int count(Class<T> type) {
        int count = 0;
        for (E event : events) {
            if (type.isInstance(event)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public List<E> asList() {
        return events;
    }
}
