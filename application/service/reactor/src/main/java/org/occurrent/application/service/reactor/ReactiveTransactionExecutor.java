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
import org.occurrent.application.service.ExecutorScope;
import reactor.core.publisher.Mono;

import java.util.function.Function;
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
 * retry strategy only when {@link #inTransactionTellingIfOpened(Function)} says this attempt opened a transaction, which
 * the service takes to mean that transaction undoes the write. That takes a store that joins the transaction and a
 * rollback rule that covers the failure, and the service can't check either. Otherwise it reaches the caller, since
 * running the command again could write its events twice. The retry strategy still sees a failure of the write itself.
 * An implementation that opens a transaction and overrides neither that method nor {@link #isTransactional()} still sees
 * an exception pass through and can roll back, but the command doesn't run again. See ADR 165.
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
     * Runs {@code action} as {@link #inTransaction(Supplier)} does, and tells it whether this call opened the transaction
     * it runs in and whether it started the transaction synchronization it runs in. The default infers the transaction
     * from {@link #isTransactional()} emitting {@code false} here and {@code true} inside, and reports that it started no
     * synchronization. Override this when the implementation can tell, for example when it can suspend a transaction the
     * caller opened and open its own, which that inference reports as joining. See ADR 165.
     * <p>
     * When this tells the action it opened the transaction, a DCB application service reads as the first operation of
     * that transaction and lets the event store report marker versions for the read. So make no DCB write in the
     * transaction before you run the action. See ADR 167.
     *
     * @param action Supplies the reactive unit of work, given what this call set up for it.
     * @param <T>    The result type.
     * @return A {@link Mono} that runs the action, optionally transactionally.
     */
    default <T> Mono<T> inTransactionTellingIfOpened(Function<ExecutorScope, Mono<T>> action) {
        return isTransactional().defaultIfEmpty(false).flatMap(openBefore -> inTransaction(() ->
                isTransactional().defaultIfEmpty(false).flatMap(openInside -> action.apply(new ExecutorScope(!openBefore && openInside, false)))));
    }

    /**
     * Whether a transaction is in force right now. When it emits {@code true} for a transaction this call joined, the
     * application service asks {@link #rollsBackAJoinedTransactionOnFailure()} whether synchronous dispatch stops at the
     * first handler that errors. The default of {@link #inTransactionTellingIfOpened(Function)} reads it to tell whether
     * this call opened the transaction.
     * <p>
     * <strong>Answer for the moment of subscription, not for the executor as a whole.</strong> It can be asked before
     * {@link #inTransaction(Supplier)}, by the DCB application service and by the default of
     * {@link #inTransactionTellingIfOpened(Function)}, and the application service asks again inside it before the
     * write, so an implementation whose transaction depends on how it was configured or on what the caller already
     * opened should read the live state. An empty {@link Mono} counts as {@code false}. This must not error when no
     * transaction is open, or an execute with synchronous subscriptions fails before the write. A {@link Mono} rather
     * than a plain {@code boolean} because on this stack the transaction lives in the subscriber context, which only a
     * reactive answer can reach. {@code SpringReactiveTransactionExecutor} answers from that context for exactly that
     * reason.
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
     * Whether a failure thrown out of this executor, while it runs in a transaction someone else opened, makes that
     * transaction roll back, so the caller can't commit the write after catching the failure. Synchronous dispatch stops at
     * the first handler that errors only when this call opened the transaction, or when it joined one and this answers
     * {@code true}.
     * Otherwise every handler gets the event and the failures are reported together, since a synchronous subscription has
     * no replay and a handler skipped while the caller commits the write would never see the event.
     * <p>
     * The default is {@code false}, so a handler failure in a joined transaction never keeps the event from the handlers
     * behind it. {@code SpringReactiveTransactionExecutor} keeps the default, because a caller that catches the error
     * from a transaction it joined still commits that transaction.
     *
     * @return {@code true} if the transaction this executor joined can't commit after a failure is thrown
     * out of this executor.
     */
    default boolean rollsBackAJoinedTransactionOnFailure() {
        return false;
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
