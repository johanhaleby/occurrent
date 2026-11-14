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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.TransactionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The options of the transaction that the subscriber context's MongoDB session runs, as MongoDB started it. A
 * conditional append reads them to decide what its check may assume. It doesn't use what the store asked for, because a
 * caller's transaction that the append joins starts the transaction with other options. Through a mongos it also
 * starts the store's own transaction again at read concern snapshot and write concern majority (ADR 175).
 * <p>
 * This is the one place the reactor store reaches into Spring Data 5.1's transaction internals.
 */
final class BoundTransactionOptions {

    private final SessionReader reader;

    /**
     * @param databaseFactory the factory of the template the store writes with, which is the key Spring binds the
     *                        session to
     */
    BoundTransactionOptions(ReactiveMongoDatabaseFactory databaseFactory) {
        this.reader = new SessionReader(requireNonNull(databaseFactory, ReactiveMongoDatabaseFactory.class.getSimpleName() + " cannot be null"));
    }

    /**
     * The options, or empty when there's no transaction in the context, no bound session with an active transaction, or
     * when reading them fails in any way, including a Spring Data version whose internals differ from 5.1. Never
     * signals an error.
     */
    Mono<Optional<TransactionOptions>> current() {
        return TransactionSynchronizationManager.forCurrentTransaction()
                .map(synchronization -> Optional.ofNullable(read(synchronization)))
                .onErrorResume(e -> Mono.just(Optional.empty()))
                .defaultIfEmpty(Optional.empty());
    }

    /**
     * Starts the transaction of {@code status} again at read concern snapshot and write concern majority, keeping the
     * rest of its options, when the store's own operator began it on the session bound to the database factory and it
     * doesn't run at that pair already. Subscribe to it before the transaction sends anything, since the driver then
     * aborts it without a round trip. Completes without doing anything for a transaction it didn't just begin, such as
     * a caller's that the append joined, or when it can't reach the session. Signals an error when the driver refuses
     * to start the aborted transaction again, before the append wrote anything.
     */
    Mono<Void> restartAtOnePointInTime(ReactiveTransaction status) {
        return TransactionSynchronizationManager.forCurrentTransaction()
                .flatMap(synchronization -> {
                    final ClientSession session;
                    final TransactionOptions running;
                    try {
                        session = reader.sessionOfNewTransaction(synchronization, status);
                        if (session == null) {
                            return Mono.<Void>empty();
                        }
                        running = session.getTransactionOptions();
                    } catch (RuntimeException | LinkageError e) {
                        return Mono.<Void>empty();
                    }
                    return DcbAppendConditionCheck.restartAtOnePointInTime(running)
                            .map(restarted -> Mono.from(session.abortTransaction()).then(Mono.<Void>fromRunnable(() -> session.startTransaction(restarted))))
                            .orElseGet(Mono::empty);
                })
                .onErrorResume(NoTransactionException.class, e -> Mono.empty());
    }

    // Reactor rethrows a LinkageError from map instead of signalling it, so it's caught here
    private @Nullable TransactionOptions read(TransactionSynchronizationManager synchronization) {
        try {
            return reader.current(synchronization);
        } catch (RuntimeException | LinkageError e) {
            return null;
        }
    }

    // Never begins a transaction. It exists to reach the session Spring bound, through the protected doGetTransaction.
    private static final class SessionReader extends ReactiveMongoTransactionManager {

        private SessionReader(ReactiveMongoDatabaseFactory databaseFactory) {
            super(databaseFactory);
        }

        private @Nullable TransactionOptions current(TransactionSynchronizationManager synchronization) {
            ClientSession session = boundSession(synchronization);
            return session != null && session.hasActiveTransaction() ? session.getTransactionOptions() : null;
        }

        // The bound session, when it's the one status just began a transaction on
        private @Nullable ClientSession sessionOfNewTransaction(TransactionSynchronizationManager synchronization, ReactiveTransaction status) {
            if (!status.isNewTransaction() || !(status instanceof GenericReactiveTransaction genericStatus)
                    || !(genericStatus.getTransaction() instanceof ReactiveMongoTransactionObject begun)) {
                return null;
            }
            ClientSession session = boundSession(synchronization);
            return session != null && session == begun.getSession() && session.hasActiveTransaction() ? session : null;
        }

        private @Nullable ClientSession boundSession(TransactionSynchronizationManager synchronization) {
            return doGetTransaction(synchronization) instanceof ReactiveMongoTransactionObject transaction ? transaction.getSession() : null;
        }
    }
}
