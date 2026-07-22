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

package org.occurrent.application.service.spring.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.reactor.ReactiveTransactionExecutor;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * The reactive counterpart of {@code SpringTransactionExecutor}: a {@link ReactiveTransactionExecutor} backed by a
 * Spring {@link TransactionalOperator}. Wiring it into the reactive application service makes the event-store write
 * and any synchronous subscription handlers run inside a single reactive transaction, so they commit when the chain
 * completes and roll back if it errors.
 * <p>
 * It relies on the standard Spring transaction propagation ({@code PROPAGATION_REQUIRED}): the operator opens (or
 * joins) a transaction bound to the Reactor context, and the reactive MongoDB event store - configured with a
 * {@code ReactiveMongoTransactionManager} and {@code SessionSynchronization.ALWAYS} - participates in that same
 * transaction rather than opening its own. A handler whose {@code Mono} errors therefore rolls the write back.
 *
 * <h4>There is no free lunch</h4>
 * <p>
 * This only matters, and only pays off, while synchronous subscriptions are actually registered: a write with none
 * registered dispatches to nobody, so wrapping it in a transaction buys nothing but the (small) cost of opening one.
 * The extra per-write cost of synchronous dispatch itself (one re-read of the just-written events) is likewise paid
 * only while at least one synchronous subscription is registered.
 * </p>
 */
@NullMarked
public class SpringReactiveTransactionExecutor implements ReactiveTransactionExecutor {

    private final TransactionalOperator transactionalOperator;

    /**
     * Create an executor that runs work inside reactive transactions managed by the supplied
     * {@link ReactiveTransactionManager}, using the manager's default propagation ({@code PROPAGATION_REQUIRED}).
     *
     * @param transactionManager The reactive transaction manager to use (for MongoDB, a
     *                           {@code ReactiveMongoTransactionManager}).
     */
    public SpringReactiveTransactionExecutor(ReactiveTransactionManager transactionManager) {
        this(TransactionalOperator.create(Objects.requireNonNull(transactionManager, "transactionManager cannot be null")));
    }

    /**
     * Create an executor backed by an already-configured {@link TransactionalOperator}, for callers that need to
     * customize propagation, isolation, or timeout.
     *
     * @param transactionalOperator The transactional operator to run work through.
     */
    public SpringReactiveTransactionExecutor(TransactionalOperator transactionalOperator) {
        this.transactionalOperator = Objects.requireNonNull(transactionalOperator, "transactionalOperator cannot be null");
    }

    @Override
    public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
        Objects.requireNonNull(action, "action cannot be null");
        return Mono.defer(action).as(transactionalOperator::transactional);
    }

    @Override
    public String toString() {
        return "SpringReactiveTransactionExecutor{transactionalOperator=" + transactionalOperator + "}";
    }
}
