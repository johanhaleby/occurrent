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
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The options of the transaction that the subscriber context's MongoDB session runs, as MongoDB started it. A
 * conditional append reads them to decide what its check may assume. It doesn't use what the store asked for, because a
 * caller's transaction that the append joins, or a resolver that ignores labels, starts the transaction with other
 * options.
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
            if (!(doGetTransaction(synchronization) instanceof ReactiveMongoTransactionObject transaction)) {
                return null;
            }
            ClientSession session = transaction.getSession();
            return session != null && session.hasActiveTransaction() ? session.getTransactionOptions() : null;
        }
    }
}
