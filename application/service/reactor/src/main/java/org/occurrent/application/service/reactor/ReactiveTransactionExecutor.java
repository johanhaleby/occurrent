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
     * that owner. See ADR 0070.
     */
    <T> Mono<T> inTransaction(Supplier<Mono<T>> action);

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
