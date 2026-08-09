/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoCommandException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@NullMarked
class MongoListenerLockService {
    private static final Logger log = LoggerFactory.getLogger(MongoListenerLockService.class);

    /**
     * Attempts to acquire the lock for the current subscriber ID, or refresh a lock already held by
     * the current subscriber ID (extending its lease). If the lock is acquired, a
     * {@link ListenerLock} will be returned. Otherwise, will return {@link Optional#empty()}.
     *
     * <p>Only one subscriber ID will hold a lock for a given {@code subscriptionId} at any time.
     *
     * <p>A subscriber's lease can expire while it still believes it holds the lock and is still
     * acting on events. A checkpoint written after that point can move the checkpoint backward, so
     * the new holder redelivers events already handled once. Delivery stays at least once, and the
     * redelivered events are processed again (see ADR 115).
     *
     * @param subscriptionId The subscriptionId to lock.
     * @return {@code Optional} with a {@link ListenerLock} if the lock is held by this subscriber,
     * otherwise an empty optional if the lock is held by a different subscriber.
     */
    static Optional<ListenerLock> acquireOrRefreshFor(MongoCollection<BsonDocument> collection, RetryStrategy retryStrategy, Duration leaseTime, String subscriptionId, String subscriberId) {
        return retryStrategy.execute(() -> {
            try {
                logDebug("acquireOrRefreshFor (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                // Matches on _id alone, upsert-safe, since MongoDB refuses $expr (needed to judge expiry against
                // the database's own clock) inside an upsert's filter, and matching on subscriberId as well would
                // seed that field into a freshly upserted document before the pipeline below ever saw it, hiding
                // the very "is this a fresh take" distinction that pipeline depends on. Whether the lease is
                // actually taken is instead decided entirely inside the pipeline, so it stays one atomic operation.
                final BsonDocument found = collection
                        .withWriteConcern(WriteConcern.MAJORITY)
                        .findOneAndUpdate(
                                eq("_id", subscriptionId),
                                singletonList(combine(
                                        set("subscriberId", subscriberIdIfAllowed(subscriberId)),
                                        set("version", sameIfRefreshOtherwiseIncrementIfAllowed(subscriberId)),
                                        set("expiresAt", expiresAtFromNowIfAllowed(subscriberId, leaseTime)))),
                                new FindOneAndUpdateOptions()
                                        .projection(include("version", "subscriberId"))
                                        .returnDocument(ReturnDocument.AFTER)
                                        .upsert(true));

                if (found == null) {
                    throw new IllegalStateException("No lock document upserted, but none found. This should never happen.");
                }

                if (!subscriberId.equals(found.getString("subscriberId").getValue())) {
                    // Held by someone else and not expired, so the pipeline above left it untouched.
                    return Optional.empty();
                }

                final ListenerLock lock = new ListenerLock(found.getNumber("version"));

                logDebug("Found lock: {} (subscriberId={}, subscriptionId={})", lock.version(), subscriberId, subscriptionId);

                return Optional.of(lock);
            } catch (MongoCommandException e) {
                final ErrorCategory errorCategory = ErrorCategory.fromErrorCode(e.getErrorCode());

                if (errorCategory.equals(DUPLICATE_KEY)) {
                    // Matching on _id alone, against its unique index, is exactly the shape MongoDB itself
                    // documents as immune to an upsert racing itself into a duplicate key, so this is not expected
                    // to fire. Kept as a defensive fallback rather than a signal this method relies on.
                    return Optional.empty();
                }

                logDebug("Caught {} - {} in acquireOrRefreshFor (errorCategory={}, subscriberId={}, subscriptionId={})",
                        e.getClass().getName(), e.getMessage(), errorCategory, subscriberId, subscriptionId);

                throw e;
            }
        });
    }

    /**
     * Whether {@code subscriberId} is entitled to take or keep the lease: nobody has taken it yet, it is already
     * this subscriber's, or the current holder's lease has expired.
     */
    private static Document isAllowedFor(String subscriberId) {
        return new Document("$or", asList(
                new Document("$eq", asList(new Document("$type", "$subscriberId"), "missing")),
                isCurrentHolder(subscriberId),
                lockIsExpiredExpr()));
    }

    private static Document isCurrentHolder(String subscriberId) {
        return new Document("$eq", asList("$subscriberId", subscriberId));
    }

    private static Document subscriberIdIfAllowed(String subscriberId) {
        return new Document("$cond", new Document(Map.of(
                "if", isAllowedFor(subscriberId),
                "then", subscriberId,
                "else", "$subscriberId")));
    }

    private static Document expiresAtFromNowIfAllowed(String subscriberId, Duration leaseTime) {
        return new Document("$cond", new Document(Map.of(
                "if", isAllowedFor(subscriberId),
                "then", expiresAtFromNow(leaseTime),
                "else", "$expiresAt")));
    }

    static DeleteResult remove(MongoCollection<BsonDocument> collection, RetryStrategy retryStrategy, String subscriptionId, String subscriberId) {
        return retryStrategy.execute(() -> {
            logDebug("Before removing lock (subscriptionId={})", subscriptionId);
            return collection.deleteOne(and(eq("_id", subscriptionId), eq("subscriberId", subscriberId)));
        });
    }

    static boolean commit(MongoCollection<BsonDocument> collection, RetryStrategy retryStrategy, Duration leaseTime, String subscriptionId, String subscriberId) throws LostLockException {
        return retryStrategy.execute(() -> {
            logDebug("Before commit (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            UpdateResult result = collection
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .updateOne(
                            and(
                                    eq("_id", subscriptionId),
                                    eq("subscriberId", subscriberId)),
                            singletonList(set("expiresAt", expiresAtFromNow(leaseTime))));

            boolean gotLock = result.getMatchedCount() != 0;
            logDebug("After commit gotLock={} (subscriberId={}, subscriptionId={})", gotLock, subscriberId, subscriptionId);
            return gotLock;
        });
    }

    /**
     * The database's own clock, not the calling node's, so that acquiring, refreshing and judging a lease all agree
     * with each other regardless of clock skew between nodes. {@code $$NOW} is fixed once per operation and is the
     * same value on every member of the deployment.
     */
    private static Document expiresAtFromNow(Duration leaseTime) {
        return new Document("$add", asList("$$NOW", leaseTime.toMillis()));
    }

    private static Document lockIsExpiredExpr() {
        return new Document("$or", asList(
                new Document("$eq", asList("$expiresAt", null)),
                new Document("$eq", asList(new Document("$type", "$expiresAt"), "missing")),
                new Document("$lte", asList("$expiresAt", "$$NOW"))));
    }

    /**
     * The version stays put on a refresh, increments on a genuine takeover (a fresh or expired lease), and stays
     * put again when {@code subscriberId} was not entitled to touch it at all.
     */
    private static Document sameIfRefreshOtherwiseIncrementIfAllowed(String subscriberId) {
        return new Document("$cond", new Document(Map.of(
                "if", isCurrentHolder(subscriberId),
                "then", "$version",
                "else", new Document("$cond", new Document(Map.of(
                        "if", isAllowedFor(subscriberId),
                        "then", new Document("$ifNull", asList(
                                new Document("$add", asList("$version", 1)),
                                0)),
                        "else", "$version"))))));
    }

    private static void logDebug(String message, Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}
