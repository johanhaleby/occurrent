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

package org.occurrent.application.service.spring;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A {@link TransactionExecutor} backed by a Spring {@link TransactionTemplate}. Wiring it into the application service
 * makes the event-store write and any synchronous subscription handlers run inside a single Spring-managed
 * transaction, so they commit or roll back together.
 * <p>
 * It relies on the standard Spring transaction propagation ({@code PROPAGATION_REQUIRED}): the executor opens (or
 * joins) a transaction, and the MongoDB event store - configured with a transaction manager and
 * {@code SessionSynchronization.ALWAYS} - participates in that same transaction rather than opening its own. A handler
 * that throws therefore rolls the write back.
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
public class SpringTransactionExecutor implements TransactionExecutor {

    /**
     * Retries a conflict between concurrent appends, but only when this executor opened the transaction: the store
     * joins that transaction and so cannot retry one itself, and only its owner can start a fresh one. See ADR 0070.
     * <p>
     * {@link DataIntegrityViolationException} is matched as well as {@link TransientDataAccessException} because
     * MongoDB labels a WriteConflict transient while Spring translates it to the non-transient type, so the obvious
     * predicate would miss the most common conflict there is. A genuine integrity violation is therefore retried too,
     * which is wasteful rather than wrong since it fails the same way every attempt.
     */
    private static final RetryStrategy TRANSIENT_CONFLICT_RETRY = RetryStrategy
            .exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f)
            .maxAttempts(5)
            .retryIf(throwable -> throwable instanceof TransientDataAccessException || throwable instanceof DataIntegrityViolationException);

    private final TransactionTemplate transactionTemplate;

    /**
     * Create an executor that runs work inside transactions managed by the supplied
     * {@link PlatformTransactionManager}, using the manager's default propagation ({@code PROPAGATION_REQUIRED}).
     *
     * @param transactionManager The Spring transaction manager to use (for MongoDB, a {@code MongoTransactionManager}).
     */
    public SpringTransactionExecutor(PlatformTransactionManager transactionManager) {
        this(new TransactionTemplate(Objects.requireNonNull(transactionManager, "transactionManager cannot be null")));
    }

    /**
     * Create an executor backed by an already-configured {@link TransactionTemplate}, for callers that need to
     * customize propagation, isolation, or timeout.
     *
     * @param transactionTemplate The transaction template to run work through.
     */
    public SpringTransactionExecutor(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate cannot be null");
    }

    @Override
    public <T> T inTransaction(Supplier<T> action) {
        Objects.requireNonNull(action, "action cannot be null");
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            // A transaction is already open, so the template joins it and this executor is not its owner. A conflict
            // aborts the whole transaction, which only its owner can start again, so run the action once and let the
            // error reach that owner. See ADR 0070.
            return transactionTemplate.execute(status -> action.get());
        }
        return TRANSIENT_CONFLICT_RETRY.execute(() -> transactionTemplate.execute(status -> action.get()));
    }

    @Override
    public String toString() {
        return "SpringTransactionExecutor{transactionTemplate=" + transactionTemplate + "}";
    }
}
