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
     * joins that transaction and so cannot retry one itself, and only its owner can start a fresh one. See ADR 0074.
     * <p>
     * The attempt count and backoff deliberately match the event store's own transient-conflict retry, because this
     * stands in for exactly that retry once the store has to give it up. Every append increments one global position
     * counter, so under concurrency the last writer can need to wait out all the others, and a smaller budget is not
     * enough. Five attempts, borrowed from the unrelated append-condition retry, failed a six-way contention test.
     * <p>
     * {@link DataIntegrityViolationException} is matched as well as {@link TransientDataAccessException} because
     * MongoDB labels a WriteConflict transient while Spring translates it to the non-transient type, so the obvious
     * predicate would miss the most common conflict there is. A genuine integrity violation is therefore retried too,
     * which is wasteful rather than wrong since it fails the same way every attempt.
     */
    private static final RetryStrategy DEFAULT_CONFLICT_RETRY = RetryStrategy
            .exponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(500), 2.0f)
            .maxAttempts(15)
            .retryIf(throwable -> throwable instanceof TransientDataAccessException || throwable instanceof DataIntegrityViolationException);

    private final TransactionTemplate transactionTemplate;
    private final RetryStrategy conflictRetry;

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
        this(transactionTemplate, DEFAULT_CONFLICT_RETRY);
    }

    /**
     * Create an executor with your own conflict-retry policy, for callers who need a different budget or want to
     * switch the retry off entirely with {@link RetryStrategy#none()}.
     * <p>
     * Worth knowing before you widen it: the retry re-runs the whole unit of work, which with synchronous
     * subscriptions includes the handlers, so a handler with a side effect outside the transaction can run more than
     * once for one command.
     *
     * @param transactionManager The Spring transaction manager to use (for MongoDB, a {@code MongoTransactionManager}).
     * @param conflictRetry      The retry policy to apply when this executor opens the transaction.
     */
    public SpringTransactionExecutor(PlatformTransactionManager transactionManager, RetryStrategy conflictRetry) {
        this(new TransactionTemplate(Objects.requireNonNull(transactionManager, "transactionManager cannot be null")), conflictRetry);
    }

    /**
     * Create an executor with both an already-configured {@link TransactionTemplate} and your own conflict-retry
     * policy. See {@link #SpringTransactionExecutor(PlatformTransactionManager, RetryStrategy)} for what the retry
     * covers.
     *
     * @param transactionTemplate The transaction template to run work through.
     * @param conflictRetry       The retry policy to apply when this executor opens the transaction.
     */
    public SpringTransactionExecutor(TransactionTemplate transactionTemplate, RetryStrategy conflictRetry) {
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate cannot be null");
        this.conflictRetry = Objects.requireNonNull(conflictRetry, "conflictRetry cannot be null");
    }

    /**
     * The conflict-retry policy applied when this executor opens the transaction: 15 attempts with exponential
     * backoff from 10ms to 500ms, matching the event store's own transient-conflict retry.
     *
     * @return The default retry policy.
     */
    public static RetryStrategy defaultConflictRetry() {
        return DEFAULT_CONFLICT_RETRY;
    }

    @Override
    public <T> T inTransaction(Supplier<T> action) {
        Objects.requireNonNull(action, "action cannot be null");
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            // A transaction is already open, so the template joins it and this executor is not its owner. A conflict
            // aborts the whole transaction, which only its owner can start again, so run the action once and let the
            // error reach that owner. See ADR 0074.
            return transactionTemplate.execute(status -> action.get());
        }
        return conflictRetry.execute(() -> transactionTemplate.execute(status -> action.get()));
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public String toString() {
        return "SpringTransactionExecutor{transactionTemplate=" + transactionTemplate + ", conflictRetry=" + conflictRetry + "}";
    }
}
