/*
 *
 *  Copyright 2022 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.deadline.api.blocking;

import java.util.Objects;

/**
 * A consumer of deadline's. You can regard this is a callback that'll be invoked when a {@link Deadline} occur for a given {@code category}.
 * There can only be one {@code DeadlineConsumer} per {@code category}.
 */
@FunctionalInterface
public interface DeadlineConsumer<T> {

    /**
     * The action that should take place when a deadline matching the {@code category}
     *
     * @param id       The id of the deadline
     * @param category The deadline category
     * @param deadline The actual deadline (date/time)
     * @param data     The data associated with the deadline (if any)
     */
    void accept(String id, String category, Deadline deadline, T data);

    /**
     * Compose (chain) this deadline consumer with another one.
     *
     * @param after The deadline consumer to call after this one
     * @return A new deadline consumer that is a composition between the two
     */
    default DeadlineConsumer<T> andThen(DeadlineConsumer<? super T> after) {
        Objects.requireNonNull(after);
        return (a, b, c, d) -> {
            accept(a, b, c, d);
            after.accept(a, b, c, d);
        };
    }
}