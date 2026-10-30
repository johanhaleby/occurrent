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

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.mongodb.internal.UnknownCommitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ReactiveMongoTransactionManager} that sends a commit again when MongoDB could not say whether it succeeded.
 * <p>
 * A plain {@code ReactiveMongoTransactionManager} reports such a commit as a failure, although the transaction may have
 * been saved, and retrying the append then either writes the events twice or is refused as a duplicate. This manager
 * retries the commit while the error carries the {@code UnknownTransactionCommitResult} label, for up to two minutes
 * from the first attempt, as the driver's {@code withTransaction} does for the native event store. A
 * {@code MaxTimeMSExpired} error is not retried, which is also what the driver does. When the time is up, the last error
 * is emitted from the commit as usual.
 * <p>
 * Use it for the {@link EventStoreConfig.Builder#transactionConfig(org.springframework.transaction.ReactiveTransactionManager) event store's}
 * transaction manager. The Occurrent Spring Boot starter creates one unless the application defines its own
 * {@code ReactiveMongoTransactionManager}. An {@link EventStoreConfig} built from any other reactive transaction manager
 * logs a warning.
 */
@NullMarked
public class CommitRetryingReactiveMongoTransactionManager extends ReactiveMongoTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(CommitRetryingReactiveMongoTransactionManager.class);

    private final Duration retryLimit;

    /**
     * @see ReactiveMongoTransactionManager#ReactiveMongoTransactionManager(ReactiveMongoDatabaseFactory)
     */
    public CommitRetryingReactiveMongoTransactionManager(ReactiveMongoDatabaseFactory databaseFactory) {
        this(databaseFactory, UnknownCommitResult.RETRY_LIMIT);
    }

    // The limit is only a parameter so that a test can run out of time without waiting two minutes.
    CommitRetryingReactiveMongoTransactionManager(ReactiveMongoDatabaseFactory databaseFactory, Duration retryLimit) {
        super(databaseFactory);
        this.retryLimit = requireNonNull(retryLimit, "Retry limit cannot be null");
    }

    @Override
    protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager, ReactiveMongoTransactionObject transactionObject) {
        return Mono.defer(() -> {
            long startNanos = System.nanoTime();
            return Mono.defer(() -> super.doCommit(synchronizationManager, transactionObject))
                    .retryWhen(Retry.indefinitely()
                            .filter(failure -> System.nanoTime() - startNanos < retryLimit.toNanos() && UnknownCommitResult.isRetryable(failure))
                            .doBeforeRetry(signal -> log.debug("MongoDB could not report the result of a commit, sending it again.", signal.failure())));
        });
    }
}
