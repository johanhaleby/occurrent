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
 * The events a flow saga instance has received, in arrival order with the initiating event first. It is the only "state" a
 * flow saga has, and is what a reaction, a guard, or a not-fulfilled branch reads.
 * <p>
 * These are the events in the instance's <em>retained window</em>, not necessarily its whole history. A flow saga keeps a
 * bounded window (the initiating event, always present, plus the current step's events and a configurable carry-over of
 * earlier ones, see the flow builder's {@code historyWindow}), so counts and lookups here span that window. A retry guard
 * such as {@code count(PaymentFailed.class) < 3} works as long as its threshold fits inside the window, which the default
 * comfortably covers. A guard that must count far beyond it needs a wider {@code historyWindow}. {@link #initiating()} is
 * the exception. It always returns the start event even after the window has moved past it.
 * <p>
 * How much of that retained history a given callback reads depends on which callback it is. A guard
 * ({@code on(Class, onlyIf, ...)}) and a {@code timeout}'s {@code onExpiry} read all of it, which is what makes a count
 * spanning several steps possible. A window-condition reaction ({@code on(StepCondition, ...)}, and the deprecated
 * {@code join}) reads the step window its condition was evaluated over instead, the events received since the step it
 * fired from was entered, so a count it takes agrees with the count that fulfilled the condition. {@link #initiating()}
 * reaches past the window either way.
 *
 * @param <E> the domain event type
 */
public interface ReceivedEvents<E> {

    /** The event that started this saga instance. */
    E initiating();

    /**
     * The initiating event cast to {@code type}. Throws {@link ClassCastException} if it is not of that type.
     * <p>
     * Kotlin has a reified {@code received.initiating<OrderPlaced>()} for this. It is a top-level extension in this
     * package, so a caller in another package imports it by name, {@code import org.occurrent.dsl.saga.flow.initiating}.
     * Without that import the compiler reports "No type arguments expected" against the no-arg {@link #initiating()}
     * below rather than an unresolved reference, which makes a missing import look like the wrong method.
     */
    <T extends E> T initiating(Class<T> type);

    /** The first received event of {@code type}, if any. */
    <T extends E> Optional<T> first(Class<T> type);

    /** All received events of {@code type}, in arrival order. */
    <T extends E> List<T> all(Class<T> type);

    /** How many events of {@code type} have been received. */
    <T extends E> int count(Class<T> type);

    /**
     * Whether any event of {@code type} has been received.
     * <p>
     * Kotlin has a reified {@code received.any<Rejected>()} for this, a top-level extension in this package.
     */
    default <T extends E> boolean any(Class<T> type) {
        return first(type).isPresent();
    }

    /** Whether no event of {@code type} has been received. */
    default <T extends E> boolean none(Class<T> type) {
        return first(type).isEmpty();
    }

    /** The events in the window this view answers over, in arrival order. */
    List<E> asList();

    /** A view over all of {@code events} (which must be non-empty, element 0 is the initiating event). */
    static <E> ReceivedEvents<E> of(List<E> events) {
        return new ReceivedEventsList<>(events);
    }
}

final class ReceivedEventsList<E> implements ReceivedEvents<E> {
    private final List<E> events;
    // Where the window this view answers over begins. 0 is the whole retained list, what a guard and a timeout reaction
    // read. A window-condition reaction gets the index its own step's window starts at instead, so the events it counts are
    // the events the condition counted. initiating() ignores this and always answers element 0, which is why it keeps
    // working from a reaction whose window has long since moved past the start event.
    private final int windowStart;

    ReceivedEventsList(List<E> events) {
        this(events, 0);
    }

    ReceivedEventsList(List<E> events, int windowStart) {
        requireNonNull(events, "events cannot be null");
        if (events.isEmpty()) {
            throw new IllegalArgumentException("received events cannot be empty; the initiating event is always present");
        }
        if (windowStart < 0 || windowStart > events.size()) {
            throw new IllegalArgumentException("windowStart must be between 0 and the number of received events ("
                    + events.size() + "), was " + windowStart);
        }
        this.events = List.copyOf(events);
        this.windowStart = windowStart;
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
        for (E event : window()) {
            if (type.isInstance(event)) {
                return Optional.of(type.cast(event));
            }
        }
        return Optional.empty();
    }

    @Override
    public <T extends E> List<T> all(Class<T> type) {
        List<T> result = new ArrayList<>();
        for (E event : window()) {
            if (type.isInstance(event)) {
                result.add(type.cast(event));
            }
        }
        return List.copyOf(result);
    }

    @Override
    public <T extends E> int count(Class<T> type) {
        int count = 0;
        for (E event : window()) {
            if (type.isInstance(event)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public List<E> asList() {
        return window();
    }

    // events is already immutable, so a subList of it is an immutable view and needs no copy.
    private List<E> window() {
        return windowStart == 0 ? events : events.subList(windowStart, events.size());
    }
}
