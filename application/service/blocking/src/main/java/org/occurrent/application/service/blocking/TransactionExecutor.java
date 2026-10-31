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
 * <p>
 * A failure after the write, in the re-read of the written events or in a handler, reaches the application service's
 * retry strategy only when {@link #isTransactional()} answers {@code false} before this executor is entered and
 * {@code true} inside it, which the service takes to mean this attempt opened a transaction that undoes the write. That
 * takes a store that joins the transaction and a rollback rule that covers the failure, and the service can't check
 * either. Otherwise it reaches the caller, since running the command again could write its events twice. The retry
 * strategy still sees a failure of the write itself. An implementation that opens a transaction and doesn't override {@link #isTransactional()} still sees an
 * exception pass through and can roll back, but the command doesn't run again. See ADR 165.
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
     * Whether a transaction is in force right now. A synchronous subscription reads this to decide what happens to the
     * handlers behind one that throws: inside a transaction dispatch stops at the first failure, outside one every
     * handler is offered the event and the failures are reported together. The application service reads it to decide
     * whether its retry strategy sees a failure after the write.
     * <p>
     * <strong>Answer for the moment of the call, not for the executor as a whole.</strong> The application service asks
     * before entering {@link #inTransaction(Supplier)}, and again inside it before the write, so an implementation whose
     * transaction depends on how it was configured or on what the caller already opened should read the live state rather
     * than return a fixed value. {@code SpringTransactionExecutor} answers from
     * {@code TransactionSynchronizationManager.isActualTransactionActive()} for exactly that reason. The service takes an
     * answer of {@code true} before entering to mean the action only joins a transaction, so the retry strategy doesn't see
     * a failure after the write. This must not throw when no transaction is open, or an execute with synchronous
     * subscriptions fails before the write.
     * <p>
     * The default is {@code false}, so an implementation that does not override it gets the isolating behaviour, which
     * cannot lose a reaction. See the 2026-08-04 amendment to ADR 57.
     *
     * @return {@code true} if a transaction is active around the caller.
     */
    default boolean isTransactional() {
        return false;
    }

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
