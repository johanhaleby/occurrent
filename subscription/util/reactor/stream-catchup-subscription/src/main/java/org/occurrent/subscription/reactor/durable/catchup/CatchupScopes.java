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

package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * Opens the {@link CatchupScope} of a replay attempt.
 */
@NullMarked
@FunctionalInterface
interface CatchupScopes {

    /**
     * Subscribing to the returned {@link Mono} opens the scope.
     *
     * @param notBefore The live start an earlier attempt stored, which the reads must see everything up to, or null
     */
    Mono<CatchupScope> open(@Nullable String notBefore);

    /**
     * Returns {@code store} as the {@code spi} a catch-up reads through. A catch-up reads its live start and every
     * replay read from the event store in one session, so it can't read through a store that doesn't implement the SPI,
     * such as a decorator around one.
     *
     * @throws IllegalArgumentException if {@code store} doesn't implement {@code spi}
     */
    static <T> T requireCatchupReads(Object store, Class<T> spi) {
        if (!spi.isInstance(store)) {
            throw new IllegalArgumentException("Cannot run a catch-up subscription over " + store.getClass().getName() + ", because it does not implement " + spi.getSimpleName() + " (" + spi.getName()
                    + "). A catch-up reads its live start and every replay read from the event store in one session, which only a store that implements it can do. Hand the catch-up the event store itself and decorate around the subscription instead.");
        }
        return spi.cast(store);
    }
}
