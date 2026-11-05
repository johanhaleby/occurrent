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
     * handler is offered the event and the failures are reported together.
     * <p>
     * <strong>Answer for the moment of subscription, not for the executor as a whole.</strong> The application service
     * asks during dispatch, which runs inside {@link #inTransaction(Supplier)}, so an implementation whose transaction
     * depends on how it was configured or on what the caller already opened should read the live state.
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
     * Whether the application service may read and decide before it subscribes to {@link #inTransaction(Supplier)}, and
     * run only the append and the synchronous dispatch in it. A DCB application service with synchronous subscriptions
     * asks this before every attempt, since a store reports marker versions only for a read outside a transaction, and a
     * snapshot decider saves its snapshot only at those versions.
     * <p>
     * Emit {@code false} when a read in this subscriber context can bind a resource that {@link #inTransaction(Supplier)}
     * then fails on. Spring's reactive MongoDB template, for example, binds a session to a transaction context in the
     * subscriber context when it reads, and a {@code PROPAGATION_NEVER} executor then takes that session for an existing
     * transaction. Answer for the moment of subscription, like {@link #isTransactional()}.
     * <p>
     * The default emits {@code false}, so an implementation that doesn't override it reads inside
     * {@link #inTransaction(Supplier)}, as every executor did before 0.35.0. A snapshot decider on a service with
     * synchronous subscriptions then saves no snapshot when the executor opens a transaction, and reads its whole
     * boundary on every command.
     *
     * @return A {@link Mono} emitting {@code true} if {@link #inTransaction(Supplier)} still works after a read that ran
     * before it in the same subscriber context.
     */
    default Mono<Boolean> allowsReadBeforeTransaction() {
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

            // Opens nothing, so nothing a read binds before it can make it fail
            @Override
            public Mono<Boolean> allowsReadBeforeTransaction() {
                return Mono.just(true);
            }

            @Override
            public String toString() {
                return "ReactiveTransactionExecutor.noTransaction()";
            }
        };
    }
}
