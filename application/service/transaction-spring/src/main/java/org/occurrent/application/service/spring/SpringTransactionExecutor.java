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

import com.mongodb.MongoException;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
     * counter, and appends that share a marker document commit one at a time, so under concurrency the last writer can
     * need to wait out all the others, and a smaller budget is not enough. 240 attempts is about two minutes of
     * waiting, like the 120 seconds a native {@code withTransaction} call gives the native store. Five attempts,
     * borrowed from the unrelated append-condition retry, failed a six-way contention test. An executor that uses this
     * default also schedules no retry once 120 seconds have passed since the call started, so attempts that are slow themselves, for
     * example while MongoDB has no primary, don't stretch the budget to hours. A retry passed to a constructor is
     * used as given, this default included.
     * <p>
     * A conflict is a {@link TransientDataAccessException}, or any exception with the {@code TransientTransactionError}
     * label that MongoDB puts on an error a fresh transaction can get past, found anywhere in its cause chain. That
     * label is what the driver's own {@code withTransaction} retries on. The label is needed because Spring translates
     * the most common conflict, a WriteConflict, to {@link org.springframework.dao.DataIntegrityViolationException},
     * which is not transient. Retrying that type would retry a genuine duplicate key or validation error for two
     * minutes, running the transaction callback again each time along with any side effect it has outside the
     * transaction. Without the MongoDB driver on the classpath only the transient type applies.
     * <p>
     * Three things are not retried. A commit that fails with {@code UnknownTransactionCommitResult} may have gone
     * through, so running the callback again could apply it twice. A duplicate key or validation error is a result,
     * not a conflict. A duplicate from a write made outside any transaction has no label either, which 0.34.0 did
     * retry.
     */
    private static final RetryStrategy.Retry DEFAULT_CONFLICT_RETRY = RetryStrategy
            .exponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(500), 2.0f)
            .maxAttempts(240)
            .retryIf(SpringTransactionExecutor::isTransientConflict);

    private static final boolean MONGO_DRIVER_PRESENT = ClassUtils.isPresent("com.mongodb.MongoException", SpringTransactionExecutor.class.getClassLoader());

    private static final Duration DEFAULT_CONFLICT_DEADLINE = Duration.ofSeconds(120);

    private final TransactionTemplate transactionTemplate;
    private final RetryStrategy conflictRetry;
    private final boolean stopAfterDefaultDeadline;

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
        this(transactionTemplate, DEFAULT_CONFLICT_RETRY, true);
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
        this(transactionTemplate, conflictRetry, false);
    }

    private SpringTransactionExecutor(TransactionTemplate transactionTemplate, RetryStrategy conflictRetry, boolean stopAfterDefaultDeadline) {
        this.stopAfterDefaultDeadline = stopAfterDefaultDeadline;
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate cannot be null");
        this.conflictRetry = Objects.requireNonNull(conflictRetry, "conflictRetry cannot be null");
    }

    /**
     * The conflict-retry policy applied when this executor opens the transaction: 240 attempts in total with
     * exponential backoff from 10ms to 500ms, about two minutes of waiting, matching the event store's own
     * transient-conflict retry. It counts attempts only. An executor built without a retry of its own adds a
     * 120 second deadline per call on top, which this policy does not include when you pass it to a constructor.
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
        RetryStrategy retry = stopAfterDefaultDeadline ? DEFAULT_CONFLICT_RETRY.mapRetryPredicate(withinDeadline()) : conflictRetry;
        return retry.execute(() -> transactionTemplate.execute(status -> action.get()));
    }

    private static boolean isTransientConflict(Throwable throwable) {
        Boolean byLabel = MONGO_DRIVER_PRESENT ? MongoErrorLabels.verdict(throwable) : null;
        return byLabel != null ? byLabel : throwable instanceof TransientDataAccessException;
    }

    /**
     * Kept in a class of its own so that {@link SpringTransactionExecutor} loads when the driver is absent.
     * This class is not touched then.
     */
    private static final class MongoErrorLabels {
        /**
         * What the error labels in the cause chain say, or {@code null} when they say nothing. The unknown commit result
         * label anywhere in the chain wins over the transient label, so a commit that may have gone through is not
         * retried. The driver's {@code withTransaction} also looks at the unknown commit result label first, but it
         * retries the commit and not the callback, and the executor can only run the callback again.
         */
        static Boolean verdict(Throwable throwable) {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            boolean transientLabel = false;
            for (Throwable cause = throwable; cause != null && seen.add(cause); cause = cause.getCause()) {
                if (cause instanceof MongoException mongoException) {
                    if (mongoException.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                        return false;
                    }
                    transientLabel |= mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                }
            }
            return transientLabel ? Boolean.TRUE : null;
        }
    }

    private static Function<Predicate<Throwable>, Predicate<Throwable>> withinDeadline() {
        long deadlineNanos = System.nanoTime() + DEFAULT_CONFLICT_DEADLINE.toNanos();
        return retryable -> throwable -> retryable.test(throwable) && System.nanoTime() - deadlineNanos < 0;
    }

    /**
     * Reads the live transaction state rather than answering {@code true} unconditionally, because a caller-supplied
     * {@link TransactionTemplate} can be configured not to open one at all, for example with
     * {@code PROPAGATION_NOT_SUPPORTED} or {@code PROPAGATION_NEVER}. Claiming a transaction that does not exist would
     * make a synchronous dispatch stop at the first handler failure with nothing rolling back, stranding the handlers
     * behind it.
     */
    @Override
    public boolean isTransactional() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    @Override
    public String toString() {
        return "SpringTransactionExecutor{transactionTemplate=" + transactionTemplate + ", conflictRetry=" + conflictRetry + "}";
    }
}
