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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
        return transactionTemplate.execute(status -> action.get());
    }

    @Override
    public String toString() {
        return "SpringTransactionExecutor{transactionTemplate=" + transactionTemplate + "}";
    }
}
