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
import java.util.function.Function;
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
 * A failure after the write, in the re-read of the written events or in a handler, can make the application service's
 * retry strategy run the command again only when {@link #inTransactionTellingIfOpened(Function)} says this attempt opened
 * a transaction, which the service takes to mean that transaction undoes the write. That takes a store that joins the
 * transaction and a rollback rule that covers the failure, and the service can't check either. Otherwise the strategy
 * doesn't run the command again, since that could write its events twice. A strategy made by the factory methods of
 * {@code RetryStrategy} still gives the failure to its {@code onError} listener and its {@code mapError}, and to its
 * {@code onAfterRetry} listener when the attempt was a retry. A strategy of your own that ignores the shutdown
 * predicate goes by its own, and a run after the failure returns an empty attempt, so the failure reaches the caller
 * after the strategy returns. A failure of the write itself is retried as before.
 * <p>
 * An implementation that opens a transaction and overrides neither that method nor {@link #isTransactional()} still sees
 * an exception pass through and can roll back, but the command doesn't run again. See ADR 165.
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
     * Runs {@code action} as {@link #inTransaction(Supplier)} does, and tells it whether this call opened the transaction
     * it runs in. The default infers that from {@link #isTransactional()} answering {@code false} here and {@code true}
     * inside. Override this when the implementation can tell, for example when it can suspend a transaction the caller
     * opened and open its own, which that inference reports as joining. See ADR 165.
     *
     * @param action The unit of work to run, given {@code true} if this call opened the transaction it runs in.
     * @param <T>    The result type produced by the action.
     * @return The value returned by the action.
     */
    default <T> T inTransactionTellingIfOpened(Function<Boolean, T> action) {
        boolean openBefore = isTransactional();
        return inTransaction(() -> action.apply(!openBefore && isTransactional()));
    }

    /**
     * Whether a transaction is in force right now. When it answers {@code true} for a transaction this call joined, the
     * application service asks {@link #rollsBackAJoinedTransactionOnFailure()} whether synchronous dispatch stops at the
     * first handler that throws. The default of {@link #inTransactionTellingIfOpened(Function)} reads it to tell whether
     * this call opened the transaction.
     * <p>
     * <strong>Answer for the moment of the call, not for the executor as a whole.</strong> It can be asked before
     * {@link #inTransaction(Supplier)}, by the DCB application service and by the default of
     * {@link #inTransactionTellingIfOpened(Function)}, and the application service asks again inside it before the
     * write, so an implementation whose transaction depends on how it was configured or on what the caller already
     * opened should read the live state rather than return a fixed value. {@code SpringTransactionExecutor} answers
     * from {@code TransactionSynchronizationManager.isActualTransactionActive()} for exactly that reason. This must not
     * throw when no transaction is open, or an execute with synchronous subscriptions fails before the write.
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
     * Whether a failure thrown out of this executor, while it runs in a transaction someone else opened, makes that
     * transaction roll back, so the caller can't commit the write after catching the failure. Synchronous dispatch stops at
     * the first handler that throws only when this call opened the transaction, or when it joined one and this answers
     * {@code true}.
     * Otherwise every handler gets the event and the failures are reported together, since a synchronous subscription has
     * no replay and a handler skipped while the caller commits the write would never see the event.
     * <p>
     * The default is {@code false}, so a handler failure in a joined transaction never keeps the event from the handlers
     * behind it. Answer {@code true} when a failure marks the caller's transaction rollback-only, as Spring's
     * {@code AbstractPlatformTransactionManager} does by default.
     *
     * @return {@code true} if the transaction this executor joined can't commit after a failure is thrown
     * out of this executor.
     */
    default boolean rollsBackAJoinedTransactionOnFailure() {
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
