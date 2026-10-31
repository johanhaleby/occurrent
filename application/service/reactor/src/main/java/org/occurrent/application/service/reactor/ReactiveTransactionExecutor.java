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
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * The reactive counterpart of the blocking {@code TransactionExecutor}: runs a reactive unit of work, optionally
 * inside a transaction, on behalf of the reactive application service.
 * <p>
 * Storage-neutral so the reactive application service can span the event-store write and synchronous subscription
 * handlers in one transaction without depending on Spring. A Spring implementation would be backed by a
 * {@code TransactionalOperator} / {@code ReactiveMongoTransactionManager}. The default, {@link #noTransaction()},
 * runs the work with no transaction (best-effort synchronous subscriptions).
 * <p>
 * A failure after the write, in the re-read of the written events or in a handler, reaches the application service's
 * retry strategy only when {@link #isTransactional()} emits {@code false} before this executor is entered and
 * {@code true} inside it, which the service takes to mean this attempt opened a transaction that undoes the write. That
 * takes a store that joins the transaction and a rollback rule that covers the failure, and the service can't check
 * either. Otherwise it reaches the caller, since running the command again could write its events twice. The retry
 * strategy still sees a failure of the write itself. An implementation that opens a transaction and doesn't override {@link #isTransactional()} still sees an
 * exception pass through and can roll back, but the command doesn't run again. See ADR 165.
 */
@FunctionalInterface
@NullMarked
public interface ReactiveTransactionExecutor {

    /**
     * Run {@code action} and return its result. Implementations that support transactions run it inside one,
     * committing when the returned {@link Mono} completes and rolling back if it errors. The supplier is invoked
     * per subscription, so it composes correctly with an upstream {@code retryWhen}.
     *
     * @param action Supplies the reactive unit of work.
     * @param <T>    The result type.
     * @return A {@link Mono} that runs the action, optionally transactionally.
     * @implSpec An implementation that opens a transaction is responsible for retrying a write conflict, because the
     * event store joins that transaction and an aborted transaction can only be started again by whoever began it. An
     * implementation that joins a transaction someone else opened must run the action once and let the conflict reach
     * that owner. See ADR 0074.
     */
    <T> Mono<T> inTransaction(Supplier<Mono<T>> action);

    /**
     * Whether a transaction is in force right now. A synchronous subscription reads this to decide what happens to the
     * handlers behind one that errors: inside a transaction dispatch stops at the first failure, outside one every
     * handler is offered the event and the failures are reported together. The application service reads it to decide
     * whether its retry strategy sees a failure after the write.
     * <p>
     * <strong>Answer for the moment of subscription, not for the executor as a whole.</strong> The application service
     * asks before entering {@link #inTransaction(Supplier)}, and again inside it before the write, so an implementation
     * whose transaction depends on how it was configured or on what the caller already opened should read the live state.
     * The service takes an answer of {@code true} before entering to mean the action only joins a transaction, so the
     * retry strategy doesn't see a failure after the write. An empty {@link Mono} counts as {@code false}. This must not
     * error when no transaction is open, or an execute with synchronous subscriptions fails before the write.
     * A {@link Mono} rather than a plain {@code boolean} because on this stack the transaction lives in the subscriber
     * context, which only a reactive answer can reach. {@code SpringReactiveTransactionExecutor} answers from that
     * context for exactly that reason.
     * <p>
     * The default emits {@code false}, so an implementation that does not override it gets the isolating behaviour,
     * which cannot lose a reaction. See the 2026-08-04 amendment to ADR 57.
     *
     * @return A {@link Mono} emitting {@code true} if a transaction is active around the subscriber.
     */
    default Mono<Boolean> isTransactional() {
        return Mono.just(false);
    }

    /**
     * A pass-through executor that runs the action with no transaction, deferring the supplier so it re-runs on
     * each subscription (and therefore on each retry).
     *
     * @return An executor that does not open a transaction.
     */
    static ReactiveTransactionExecutor noTransaction() {
        return new ReactiveTransactionExecutor() {
            @Override
            public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
                return Mono.defer(action);
            }

            @Override
            public String toString() {
                return "ReactiveTransactionExecutor.noTransaction()";
            }
        };
    }
}
