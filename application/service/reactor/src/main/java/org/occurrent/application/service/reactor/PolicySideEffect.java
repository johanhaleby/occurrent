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

package org.occurrent.application.service.reactor;

import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A reactive utility that makes it easier to run policies (a.k.a. triggers) as side-effects after events are written to
 * the event store. A policy takes a single domain event of a specific type and returns a {@code Mono<Void>} that
 * completes when the policy is done. This is the reactive counterpart of the blocking {@code PolicySideEffect}.
 *
 * @param <E> The type of your domain event.
 */
@NullMarked
public interface PolicySideEffect<E> extends Function<Stream<E>, Mono<Void>> {

    /**
     * Run a single policy for every event of {@code eventType} produced by the command, in order. Events that are not
     * assignable to {@code eventType} are ignored.
     */
    static <E, E_SPECIFIC extends E> PolicySideEffect<E> executePolicy(Class<E_SPECIFIC> eventType, Function<E_SPECIFIC, Mono<Void>> policy) {
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(policy, "Policy cannot be null");
        return stream -> Flux.fromStream(stream)
                .filter(e -> eventType.isAssignableFrom(e.getClass()))
                .map(eventType::cast)
                .concatMap(policy::apply)
                .then();
    }

    /**
     * Compose this policy with another one, running them in order against the same produced events.
     */
    default <E_SPECIFIC extends E> PolicySideEffect<E> andThenExecuteAnotherPolicy(Class<E_SPECIFIC> eventType, Function<E_SPECIFIC, Mono<Void>> policy) {
        PolicySideEffect<E> first = this;
        PolicySideEffect<E> second = executePolicy(eventType, policy);
        return stream -> {
            List<E> events = stream.toList();
            return first.apply(events.stream()).then(second.apply(events.stream()));
        };
    }
}
