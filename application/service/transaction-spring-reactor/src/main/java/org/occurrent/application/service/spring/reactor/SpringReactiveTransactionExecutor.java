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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

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

    /**
     * Retries a conflict between concurrent appends, such as two contending on the same partition stream or on the
     * global position counter, but only when this executor opened the transaction. The event store joins this
     * transaction rather than opening its own, so it cannot retry a conflict itself: an aborted transaction can only
     * be started again by whoever owns it. Attempt count and backoff match the event store's own transient-conflict
     * retry, which this stands in for, and the blocking {@code SpringTransactionExecutor}, so retry behaviour is
     * identical across the two stacks (ADR 0053). See ADR 0070.
     */
    private static final Retry TRANSIENT_CONFLICT_RETRY = Retry.backoff(15, Duration.ofMillis(10))
            .maxBackoff(Duration.ofMillis(500))
            .filter(throwable -> throwable instanceof TransientDataAccessException || throwable instanceof DataIntegrityViolationException)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

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
        Mono<T> transaction = Mono.defer(action).as(transactionalOperator::transactional);
        // currentContext() signals an error when the subscriber context carries no transaction. Its exception type is
        // private to Spring, so treat any error from it as "no transaction in context" rather than naming the class.
        return TransactionContextManager.currentContext()
                .map(__ -> true)
                .onErrorReturn(false)
                .flatMap(ownedByCaller -> ownedByCaller
                        // A transaction is already open, so the operator joins it and this executor is not its owner.
                        // A conflict aborts the whole transaction, which only its owner can start again, so run the
                        // action once and let the error reach that owner. See ADR 0070.
                        ? transaction
                        : transaction.retryWhen(TRANSIENT_CONFLICT_RETRY));
    }

    @Override
    public String toString() {
        return "SpringReactiveTransactionExecutor{transactionalOperator=" + transactionalOperator + "}";
    }
}
