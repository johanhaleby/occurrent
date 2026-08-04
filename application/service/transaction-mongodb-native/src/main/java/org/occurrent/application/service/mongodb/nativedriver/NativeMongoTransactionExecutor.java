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

package org.occurrent.application.service.mongodb.nativedriver;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.eventstore.mongodb.nativedriver.ClientSessionHolder;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A {@link TransactionExecutor} for non-Spring applications that use the native (synchronous java driver)
 * {@code MongoEventStore}. It opens a MongoDB {@link ClientSession} and transaction, binds the session to the
 * current thread via {@link ClientSessionHolder}, runs the unit of work, and commits on normal return or aborts
 * if the work throws.
 * <p>
 * While the session is bound, the native {@code MongoEventStore}'s write and DCB append join it instead of opening
 * their own session, so the event write and any synchronous subscription handlers dispatched inside the same unit
 * of work commit atomically: a handler that throws rolls the event write back. This gives a native application the
 * same "write plus synchronous handler in one transaction" guarantee a Spring-backed executor gives, without the
 * application service depending on Spring or the store changing its public write/append signatures.
 * <p>
 * The transaction is run through {@link ClientSession#withTransaction(com.mongodb.client.TransactionBody, TransactionOptions)},
 * so the driver's automatic retry of transient transaction errors and commit retries are preserved. A re-entrant
 * call (one made while a session is already bound on the thread) runs the action on the existing transaction rather
 * than opening a nested one.
 */
@NullMarked
public class NativeMongoTransactionExecutor implements TransactionExecutor {

    private final MongoClient mongoClient;
    private final TransactionOptions transactionOptions;

    /**
     * Create an executor using default {@link TransactionOptions}.
     *
     * @param mongoClient The {@link MongoClient} used to open sessions. The same client must back the
     *                    {@code MongoEventStore} whose writes should join the transaction.
     */
    public NativeMongoTransactionExecutor(MongoClient mongoClient) {
        this(mongoClient, TransactionOptions.builder().build());
    }

    /**
     * Create an executor using the supplied {@link TransactionOptions}.
     *
     * @param mongoClient        The {@link MongoClient} used to open sessions. The same client must back the
     *                           {@code MongoEventStore} whose writes should join the transaction.
     * @param transactionOptions The {@link TransactionOptions} applied to each transaction this executor opens.
     */
    public NativeMongoTransactionExecutor(MongoClient mongoClient, TransactionOptions transactionOptions) {
        this.mongoClient = Objects.requireNonNull(mongoClient, "mongoClient cannot be null");
        this.transactionOptions = Objects.requireNonNull(transactionOptions, "transactionOptions cannot be null");
    }

    @Override
    public <T> T inTransaction(Supplier<T> action) {
        Objects.requireNonNull(action, "action cannot be null");

        if (ClientSessionHolder.get() != null) {
            // Already inside a transaction on this thread (a re-entrant call): join it rather than opening a nested
            // one. The outermost call owns the session and its commit/abort.
            return action.get();
        }

        try (ClientSession session = mongoClient.startSession()) {
            ClientSessionHolder.set(session);
            try {
                return session.withTransaction(action::get, transactionOptions);
            } finally {
                ClientSessionHolder.remove();
            }
        }
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public String toString() {
        return NativeMongoTransactionExecutor.class.getSimpleName() + "{transactionOptions=" + transactionOptions + "}";
    }
}
