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
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.mongodb.internal.UnknownCommitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.MongoTransactionOptions;
import org.springframework.data.mongodb.MongoTransactionOptionsResolver;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * A {@link MongoTransactionManager} that sends a commit again when MongoDB could not say whether it succeeded.
 * <p>
 * A plain {@code MongoTransactionManager} reports such a commit as a failure, although the transaction may have been
 * saved, and retrying the append then either writes the events twice or is refused as a duplicate. This manager retries
 * the commit while the error carries the {@code UnknownTransactionCommitResult} label, for up to two minutes from the
 * first commit, the limit that {@code ClientSession.withTransaction} gives the native event store. The driver counts its
 * two minutes from the start of the {@code withTransaction} call instead. A top-level {@code MaxTimeMSExpired}
 * error ({@code MongoExecutionTimeoutException}) is not retried, and a write concern error with that code is, which is
 * also what the driver does. When the time is up, the last error is thrown from the commit as usual.
 * <p>
 * Use it for the {@link EventStoreConfig.Builder#transactionConfig(MongoTransactionManager) event store's}
 * transaction manager. The Occurrent Spring Boot starter creates one unless the application defines its own
 * {@code MongoTransactionManager}. A {@link SpringMongoEventStore} built on any other transaction manager logs a warning
 * when it is created.
 */
@NullMarked
public class CommitRetryingMongoTransactionManager extends MongoTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(CommitRetryingMongoTransactionManager.class);

    private final Duration retryLimit;

    /**
     * @see MongoTransactionManager#MongoTransactionManager(MongoDatabaseFactory)
     */
    public CommitRetryingMongoTransactionManager(MongoDatabaseFactory databaseFactory) {
        this(databaseFactory, null);
    }

    /**
     * @see MongoTransactionManager#MongoTransactionManager(MongoDatabaseFactory, TransactionOptions)
     */
    public CommitRetryingMongoTransactionManager(MongoDatabaseFactory databaseFactory, @Nullable TransactionOptions options) {
        this(databaseFactory, options, UnknownCommitResult.RETRY_LIMIT);
    }

    /**
     * @see MongoTransactionManager#MongoTransactionManager(MongoDatabaseFactory, MongoTransactionOptionsResolver, MongoTransactionOptions)
     */
    public CommitRetryingMongoTransactionManager(MongoDatabaseFactory databaseFactory, MongoTransactionOptionsResolver transactionOptionsResolver,
                                                 MongoTransactionOptions defaultTransactionOptions) {
        super(databaseFactory, transactionOptionsResolver, defaultTransactionOptions);
        this.retryLimit = UnknownCommitResult.RETRY_LIMIT;
    }

    // The limit is only a parameter so that a test can run out of time without waiting two minutes.
    CommitRetryingMongoTransactionManager(MongoDatabaseFactory databaseFactory, @Nullable TransactionOptions options, Duration retryLimit) {
        super(databaseFactory, options);
        this.retryLimit = requireNonNull(retryLimit, "Retry limit cannot be null");
    }

    @Override
    protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
        long startNanos = System.nanoTime();
        while (true) {
            try {
                super.doCommit(transactionObject);
                return;
            } catch (Exception e) {
                boolean withinLimit = System.nanoTime() - startNanos < retryLimit.toNanos();
                if (!withinLimit || !UnknownCommitResult.isRetryable(e)) {
                    throw e;
                }
                log.debug("MongoDB could not report the result of a commit, sending it again.", e);
            }
        }
    }
}
