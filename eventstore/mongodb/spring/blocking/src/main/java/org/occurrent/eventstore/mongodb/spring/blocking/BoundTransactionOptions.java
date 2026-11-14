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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;

import static java.util.Objects.requireNonNull;

/**
 * The options of the transaction that the current thread's MongoDB session runs, as MongoDB started it. A conditional
 * append reads them to decide what its check may assume. It doesn't use what the store asked for, because a caller's
 * transaction that the append joins starts the transaction with other options. Through a mongos it also starts the
 * store's own transaction again at read concern snapshot and write concern majority (ADR 175).
 * <p>
 * This is the one place the blocking store reaches into Spring Data 5.1's transaction internals.
 */
final class BoundTransactionOptions {

    private final SessionReader reader;

    /**
     * @param databaseFactory the factory of the template the store writes with, which is the key Spring binds the
     *                        session to
     */
    BoundTransactionOptions(MongoDatabaseFactory databaseFactory) {
        this.reader = new SessionReader(requireNonNull(databaseFactory, MongoDatabaseFactory.class.getSimpleName() + " cannot be null"));
    }

    /**
     * The options, or {@code null} when there's no bound session with an active transaction, or when reading them fails
     * in any way, including a Spring Data version whose internals differ from 5.1.
     */
    @Nullable TransactionOptions current() {
        try {
            return reader.current();
        } catch (RuntimeException | LinkageError e) {
            return null;
        }
    }

    /**
     * Starts the transaction of {@code status} again at read concern snapshot and write concern majority, keeping the
     * rest of its options, when the store's own template began it on the session bound to the database factory and it
     * doesn't run at that pair already. Call it before the transaction sends anything, since the driver then aborts it
     * without a round trip. Does nothing for a transaction it didn't just begin, such as a caller's that the append
     * joined, or when it can't reach the session.
     *
     * @throws RuntimeException when the driver refuses to start the aborted transaction again, before the append wrote
     *                          anything
     */
    void restartAtOnePointInTime(TransactionStatus status) {
        final ClientSession session;
        final TransactionOptions running;
        try {
            session = reader.sessionOfNewTransaction(status);
            if (session == null) {
                return;
            }
            running = session.getTransactionOptions();
        } catch (RuntimeException | LinkageError e) {
            return;
        }
        DcbAppendConditionCheck.restartAtOnePointInTime(running).ifPresent(restarted -> {
            session.abortTransaction();
            session.startTransaction(restarted);
        });
    }

    // Never begins a transaction. It exists to reach the session Spring bound, through the protected doGetTransaction.
    private static final class SessionReader extends MongoTransactionManager {

        private SessionReader(MongoDatabaseFactory databaseFactory) {
            super(databaseFactory);
        }

        private @Nullable TransactionOptions current() {
            ClientSession session = boundSession();
            return session != null && session.hasActiveTransaction() ? session.getTransactionOptions() : null;
        }

        // The bound session, when it's the one status just began a transaction on
        private @Nullable ClientSession sessionOfNewTransaction(TransactionStatus status) {
            if (!status.isNewTransaction() || !(status instanceof DefaultTransactionStatus defaultStatus)
                    || !(defaultStatus.getTransaction() instanceof MongoTransactionObject begun)) {
                return null;
            }
            ClientSession session = boundSession();
            return session != null && session == begun.getSession() && session.hasActiveTransaction() ? session : null;
        }

        private @Nullable ClientSession boundSession() {
            return doGetTransaction() instanceof MongoTransactionObject transaction ? transaction.getSession() : null;
        }
    }
}
