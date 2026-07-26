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

package org.occurrent.application.service.blocking;

import org.jspecify.annotations.NullMarked;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs a unit of work, optionally inside a transaction, on behalf of the application service.
 * <p>
 * This is a deliberately tiny, storage-neutral abstraction so that the application service can span the
 * event-store write and any synchronous subscription handlers in a single transaction without the application
 * service module depending on Spring or on a specific event store. Implementations decide what a transaction
 * means: a Spring {@code TransactionTemplate}, a native MongoDB {@code ClientSession}, or nothing at all.
 * <p>
 * The default, {@link #noTransaction()}, simply runs the action with no transaction. With it, synchronous
 * subscriptions are best-effort: they run synchronously before the command returns, but the event write has
 * already committed by the time a handler runs, so a throwing handler does not roll the write back. Wire a
 * real implementation (for example a Spring-backed one) to make the write and the handlers commit atomically.
 */
@FunctionalInterface
@NullMarked
public interface TransactionExecutor {

    /**
     * Run {@code action} and return its result. Implementations that support transactions run the action
     * inside one, committing on normal return and rolling back if the action throws.
     *
     * @param action The unit of work to run.
     * @param <T>    The result type produced by the action.
     * @return The value returned by the action.
     * @implSpec An implementation that opens a transaction is responsible for retrying a write conflict, because the
     * event store joins that transaction and an aborted transaction can only be started again by whoever began it. An
     * implementation that joins a transaction someone else opened must run the action once and let the conflict reach
     * that owner. See ADR 0074.
     */
    <T> T inTransaction(Supplier<T> action);

    /**
     * A pass-through executor that runs the action with no transaction. This is the default used by the
     * application service and yields best-effort synchronous subscriptions (see the interface documentation).
     *
     * @return An executor that does not open a transaction.
     */
    static TransactionExecutor noTransaction() {
        return new TransactionExecutor() {
            @Override
            public <T> T inTransaction(Supplier<T> action) {
                return Objects.requireNonNull(action, "action cannot be null").get();
            }

            @Override
            public String toString() {
                return "TransactionExecutor.noTransaction()";
            }
        };
    }
}
