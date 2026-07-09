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

/**
 * A reactive utility that makes it easier to run side-effects (a.k.a. triggers/policies) after events are written to
 * the event store. A side-effect takes a single domain event of a specific type and returns a {@code Mono<Void>} that
 * completes when the side-effect is done. This is the reactive counterpart of the blocking {@code SideEffect}.
 *
 * @param <E> The type of your domain event.
 */
@NullMarked
public interface SideEffect<E> extends Function<List<E>, Mono<Void>> {

    /**
     * Run a single side-effect for every event of {@code eventType} produced by the command, in order. Events that are not
     * assignable to {@code eventType} are ignored.
     */
    static <E, E_SPECIFIC extends E> SideEffect<E> executeSideEffect(Class<E_SPECIFIC> eventType, Function<E_SPECIFIC, Mono<Void>> sideEffect) {
        Objects.requireNonNull(eventType, "Event type cannot be null");
        Objects.requireNonNull(sideEffect, "Side-effect cannot be null");
        return events -> Flux.fromIterable(events)
                .filter(e -> eventType.isAssignableFrom(e.getClass()))
                .map(eventType::cast)
                .concatMap(sideEffect::apply)
                .then();
    }

    /**
     * Compose this side-effect with another one, running them in order against the same produced events.
     */
    default <E_SPECIFIC extends E> SideEffect<E> andThenExecuteAnotherSideEffect(Class<E_SPECIFIC> eventType, Function<E_SPECIFIC, Mono<Void>> sideEffect) {
        SideEffect<E> first = this;
        SideEffect<E> second = executeSideEffect(eventType, sideEffect);
        return events -> first.apply(events).then(second.apply(events));
    }
}
