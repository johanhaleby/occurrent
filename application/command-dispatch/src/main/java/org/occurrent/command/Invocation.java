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
 * A command whose handling logic <i>is</i> the command: a stream to write to, and the domain function to run against
 * that stream's events. Use it when your domain model is plain functions rather than command objects and deciders, so a
 * saga or a policy can invoke the domain directly instead of routing an invented command record through a
 * {@code switch}.
 * <p>
 * Dispatch it with {@link CommandDispatchers#invocation(org.occurrent.application.service.blocking.ApplicationService)},
 * which runs {@link #decision()} through {@code ApplicationService.execute(streamId, decision)}. Because that re-reads
 * the stream before deciding, a duplicated or stale invocation is rejected by the domain's own rules, which is what
 * makes at-least-once dispatch safe. It is also the reason this type carries a decision function rather than an
 * arbitrary {@code Runnable}: the only thing it can express is "fold this stream and return events to append".
 * <p>
 * In a saga, the command type becomes {@code Invocation<E>} and no command records are needed at all:
 * <pre>{@code
 * .react(OrderPlaced.class, (state, e) -> List.of(
 *         SagaEffect.issue(Invocation.to(e.orderId(), events -> reservePayment(events, e.amount())))))
 * }</pre>
 * Kotlin callers get a two-argument {@code issue(streamId) { events -> ... }} from {@code occurrent-saga-dsl-blocking}.
 * <p>
 * Two things to know before choosing this over command records:
 * <ul>
 *   <li><b>{@code E} is the event type of the stream being written to</b>, not the event type a saga subscribes to. A
 *       process commanding several write models needs a common event supertype and one {@code ApplicationService} over
 *       it.</li>
 *   <li><b>{@code Saga.adapt} cannot widen it.</b> {@code adapt} requires the narrower command type to be a subtype of
 *       the wider one, and Java generics are invariant, so {@code Invocation<PaymentEvent>} is not an
 *       {@code Invocation<DomainEvent>}. Type a feature saga on the module-wide event type from the start.</li>
 * </ul>
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

    /**
     * Only the stream id, because the decision is a lambda whose generated {@code toString} is a synthetic class name
     * that would otherwise fill every assertion failure and dispatch log line.
     */
    @Override
    public String toString() {
        return "Invocation[streamId=" + streamId + "]";
    }
}
