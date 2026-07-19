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

import static java.util.Objects.requireNonNull;

/**
 * One requirement of a {@code join} step: how many events of a given type must arrive (since the step was entered) before
 * the join is fulfilled. The count is fixed at build time; a join with a runtime-varying count is a documented non-goal
 * of the flow layer (drop to the machine-core {@code Saga} for that).
 *
 * @param eventType the event type to wait for
 * @param count     how many are required (at least one)
 * @param <E>       the domain event type
 */
public record Expectation<E>(Class<? extends E> eventType, int count) {
    public Expectation {
        requireNonNull(eventType, "eventType cannot be null");
        if (count < 1) {
            throw new IllegalArgumentException("count must be at least 1, was " + count);
        }
    }

    /** Expect exactly one event of {@code eventType}. */
    public static <E> Expectation<E> of(Class<? extends E> eventType) {
        return new Expectation<>(eventType, 1);
    }

    /** Expect {@code count} events of {@code eventType}. */
    public static <E> Expectation<E> of(Class<? extends E> eventType, int count) {
        return new Expectation<>(eventType, count);
    }
}
