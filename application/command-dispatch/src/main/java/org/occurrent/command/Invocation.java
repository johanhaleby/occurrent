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

package org.occurrent.command;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A command that carries its own handling logic. It holds a stream id, and the function to run against that stream's
 * events. Dispatch it with {@link CommandDispatchers#invocation(org.occurrent.application.service.blocking.ApplicationService)}.
 * <p>
 * In a saga, the command type becomes {@code Invocation<E>} and no command records are needed at all:
 * <pre>{@code
 * .react(OrderPlaced.class, (state, e) -> List.of(
 *         SagaEffect.issue(Invocation.to(e.orderId(), events -> reservePayment(events, e.amount())))))
 * }</pre>
 * Kotlin callers get a two-argument {@code issue(streamId) { events -> ... }} from {@code occurrent-saga-dsl-blocking}.
 * <p>
 * {@code E} is the event type of the stream being written to, not the event type a saga subscribes to.
 * {@code Saga.adapt} cannot widen it, because Java generics are invariant, so type a feature saga on the module-wide
 * event type from the start.
 * <p>
 * Two invocations are equal only when they hold the same stream id and the very same function instance, since a lambda
 * has no value equality. Assert on what an invocation <i>does</i> instead, by applying {@link #decision()} to the events
 * the test cares about.
 *
 * @param streamId the id of the stream to read from and write the decided events to
 * @param decision a <i>pure</i> function from the stream's current events to the events to append
 * @param <E>      the event type of the stream being written to
 */
public record Invocation<E>(String streamId, Function<List<E>, List<E>> decision) {

    public Invocation {
        requireNonNull(streamId, "streamId cannot be null");
        requireNonNull(decision, "decision cannot be null");
        if (streamId.isBlank()) {
            throw new IllegalArgumentException("streamId cannot be blank");
        }
    }

    /** An invocation that runs {@code decision} against the stream {@code streamId}. */
    public static <E> Invocation<E> to(String streamId, Function<List<E>, List<E>> decision) {
        return new Invocation<>(streamId, decision);
    }

    /** Renders only the stream id, since the decision lambda has no useful {@code toString}. */
    @Override
    public String toString() {
        return "Invocation[streamId=" + streamId + "]";
    }
}
