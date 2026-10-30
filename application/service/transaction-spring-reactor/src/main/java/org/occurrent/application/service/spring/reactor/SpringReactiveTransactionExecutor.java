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
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

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
     * identical across the two stacks (ADR 0053). That is 320 retries, 321 attempts in all, about two minutes of
     * waiting, since Reactor's jitter makes the average wait at the 500 ms cap 375 ms. An executor that uses this
     * default also stops retrying 120 seconds after the subscription started, so attempts that are slow themselves,
     * for example while MongoDB has no primary, don't stretch the budget to hours. A retry passed to a constructor is
     * used as given, this default included. See ADR 0074.
     */
    private static final RetryBackoffSpec DEFAULT_CONFLICT_RETRY = Retry.backoff(320, Duration.ofMillis(10))
            .maxBackoff(Duration.ofMillis(500))
            .filter(throwable -> throwable instanceof TransientDataAccessException || throwable instanceof DataIntegrityViolationException)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    private static final Duration DEFAULT_CONFLICT_DEADLINE = Duration.ofSeconds(120);

    private final TransactionalOperator transactionalOperator;
    private final Retry conflictRetry;
    private final boolean stopAfterDefaultDeadline;

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
        this(transactionalOperator, DEFAULT_CONFLICT_RETRY, true);
    }

    /**
     * Create an executor with your own conflict-retry policy, for callers who need a different budget or want to
     * switch the retry off entirely with {@link Retry#max(long)} of zero.
     * <p>
     * Worth knowing before you widen it: the retry re-runs the whole unit of work, which with synchronous
     * subscriptions includes the handlers, so a handler with a side effect outside the transaction can run more than
     * once for one command.
     *
     * @param transactionManager The reactive transaction manager to use.
     * @param conflictRetry      The retry spec to apply when this executor opens the transaction.
     */
    public SpringReactiveTransactionExecutor(ReactiveTransactionManager transactionManager, Retry conflictRetry) {
        this(TransactionalOperator.create(Objects.requireNonNull(transactionManager, "transactionManager cannot be null")), conflictRetry);
    }

    /**
     * Create an executor with both an already-configured {@link TransactionalOperator} and your own conflict-retry
     * policy. See {@link #SpringReactiveTransactionExecutor(ReactiveTransactionManager, Retry)} for what the retry
     * covers.
     *
     * @param transactionalOperator The transactional operator to run work through.
     * @param conflictRetry         The retry spec to apply when this executor opens the transaction.
     */
    public SpringReactiveTransactionExecutor(TransactionalOperator transactionalOperator, Retry conflictRetry) {
        this(transactionalOperator, conflictRetry, false);
    }

    private SpringReactiveTransactionExecutor(TransactionalOperator transactionalOperator, Retry conflictRetry, boolean stopAfterDefaultDeadline) {
        this.stopAfterDefaultDeadline = stopAfterDefaultDeadline;
        this.transactionalOperator = Objects.requireNonNull(transactionalOperator, "transactionalOperator cannot be null");
        this.conflictRetry = Objects.requireNonNull(conflictRetry, "conflictRetry cannot be null");
    }

    /**
     * The conflict-retry spec applied when this executor opens the transaction: 320 retries with exponential backoff
     * from 10ms to 500ms, about two minutes of waiting, matching the blocking executor and the event store's own
     * transient-conflict retry. It counts retries only. An executor built without a retry of its own adds a
     * 120 second deadline per subscription on top, which this spec does not include when you pass it to a constructor.
     *
     * @return The default retry spec.
     */
    public static Retry defaultConflictRetry() {
        return DEFAULT_CONFLICT_RETRY;
    }

    @Override
    public <T> Mono<T> inTransaction(Supplier<Mono<T>> action) {
        Objects.requireNonNull(action, "action cannot be null");
        Mono<T> transaction = Mono.defer(action).as(transactionalOperator::transactional);
        // currentContext() signals an error when the subscriber context carries no transaction. Spring keeps that
        // exception type private, so match it by simple name rather than swallowing every error: anything else means
        // we cannot tell who owns the transaction, and guessing "nobody" would restore the futile retry.
        return TransactionContextManager.currentContext()
                .map(__ -> true)
                .onErrorResume(throwable -> "NoTransactionInContextException".equals(throwable.getClass().getSimpleName())
                        ? Mono.just(false)
                        : Mono.error(throwable))
                .flatMap(ownedByCaller -> ownedByCaller
                        // A transaction is already open, so the operator joins it and this executor is not its owner.
                        // A conflict aborts the whole transaction, which only its owner can start again, so run the
                        // action once and let the error reach that owner. See ADR 0074.
                        ? transaction
                        : Mono.defer(() -> transaction.retryWhen(stopAfterDefaultDeadline ? withinDeadline() : conflictRetry)));
    }

    private static Retry withinDeadline() {
        long deadlineNanos = System.nanoTime() + DEFAULT_CONFLICT_DEADLINE.toNanos();
        return DEFAULT_CONFLICT_RETRY.modifyErrorFilter(isRetryable -> throwable -> isRetryable.test(throwable) && System.nanoTime() - deadlineNanos < 0);
    }

    /**
     * Reads the live transaction state rather than emitting {@code true} unconditionally, because a caller-supplied
     * {@link TransactionalOperator} can be configured not to open one at all, for example with
     * {@code PROPAGATION_NOT_SUPPORTED} or {@code PROPAGATION_NEVER}. Claiming a transaction that does not exist would
     * make a synchronous dispatch stop at the first handler failure with nothing rolling back, stranding the handlers
     * behind it.
     */
    @Override
    public Mono<Boolean> isTransactional() {
        return TransactionSynchronizationManager.forCurrentTransaction()
                .map(TransactionSynchronizationManager::isActualTransactionActive)
                // Absent context means no transaction. Matched by simple name for the same reason as in inTransaction:
                // Spring keeps the type private, and swallowing every error would report "no transaction" for a genuine
                // failure.
                .onErrorResume(throwable -> "NoTransactionInContextException".equals(throwable.getClass().getSimpleName())
                        ? Mono.just(false)
                        : Mono.error(throwable));
    }

    @Override
    public String toString() {
        return "SpringReactiveTransactionExecutor{transactionalOperator=" + transactionalOperator + ", conflictRetry=" + conflictRetry + "}";
    }
}
