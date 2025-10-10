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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
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
     * <p>A subscriber lease may expire however so it is necessary to still use a kind of fencing
     * token, like an increasing version number, when taking actions which require the lock.
     *
     * @param subscriptionId The subscriptionId to lock.
     * @return {@code Optional} with a {@link ListenerLock} if the lock is held by this subscriber,
     * otherwise an empty optional if the lock is held by a different subscriber.
     */
    static Optional<ListenerLock> acquireOrRefreshFor(MongoCollection<BsonDocument> collection, Clock clock, RetryStrategy retryStrategy, Duration leaseTime, String subscriptionId, String subscriberId) {
        return retryStrategy.execute(() -> {
            try {
                logDebug("acquireOrRefreshFor (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
                final BsonDocument found = collection
                        .withWriteConcern(WriteConcern.MAJORITY)
                        .findOneAndUpdate(
                                and(
                                        eq("_id", subscriptionId),
                                        or(lockIsExpired(clock), eq("subscriberId", subscriberId))),
                                singletonList(combine(
                                        set("subscriberId", subscriberId),
                                        set("version", sameIfRefreshOtherwiseIncrement(subscriberId)),
                                        set("expiresAt", clock.instant().plus(leaseTime)))),
                                new FindOneAndUpdateOptions()
                                        .projection(include("version"))
                                        .returnDocument(ReturnDocument.AFTER)
                                        .upsert(true));

                if (found == null) {
                    throw new IllegalStateException("No lock document upserted, but none found. This should never happen.");
                }

                final ListenerLock lock = new ListenerLock(found.getNumber("version"));

                logDebug("Found lock: {} (subscriberId={}, subscriptionId={})", lock.version(), subscriberId, subscriptionId);

                return Optional.of(lock);
            } catch (MongoCommandException e) {
                final ErrorCategory errorCategory = ErrorCategory.fromErrorCode(e.getErrorCode());

                if (errorCategory.equals(DUPLICATE_KEY)) {
                    // This happens frequently, so we don't log it
                    return Optional.empty();
                }

                logDebug("Caught {} - {} in acquireOrRefreshFor (errorCategory={}, subscriberId={}, subscriptionId={})",
                        e.getClass().getName(), e.getMessage(), errorCategory, subscriberId, subscriptionId);

                throw e;
            }
        });
    }

    static DeleteResult remove(MongoCollection<BsonDocument> collection, RetryStrategy retryStrategy, String subscriptionId, String subscriberId) {
        return retryStrategy.execute(() -> {
            logDebug("Before removing lock (subscriptionId={})", subscriptionId);
            return collection.deleteOne(and(eq("_id", subscriptionId), eq("subscriberId", subscriberId)));
        });
    }

    static boolean commit(MongoCollection<BsonDocument> collection, Clock clock, RetryStrategy retryStrategy, Duration leaseTime, String subscriptionId, String subscriberId) throws LostLockException {
        return retryStrategy.execute(() -> {
            logDebug("Before commit (subscriberId={}, subscriptionId={})", subscriberId, subscriptionId);
            Instant newLeaseTime = clock.instant().plus(leaseTime);
            UpdateResult result = collection
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .updateOne(
                            and(
                                    eq("_id", subscriptionId),
                                    eq("subscriberId", subscriberId)),
                            set("expiresAt", newLeaseTime));

            boolean gotLock = result.getMatchedCount() != 0;
            logDebug("After commit gotLock={} (subscriberId={}, subscriptionId={})", gotLock, subscriberId, subscriptionId);
            return gotLock;
        });
    }

    private static Bson lockIsExpired(Clock clock) {
        return or(
                eq("expiresAt", null),
                not(exists("expiresAt")),
                lte("expiresAt", clock.instant()));
    }

    private static Document sameIfRefreshOtherwiseIncrement(String subscriberId) {
        Map<String, Object> map = new HashMap<>();
        map.put("if", new Document("$ne", asList("$subscriberId", subscriberId)));
        map.put("then", new Document("$ifNull", asList(
                new Document("$add", asList("$version", 1)),
                0)));
        map.put("else", "$version");

        return new Document("$cond", new Document(map));
    }

    private static void logDebug(String message, Object... params) {
        if (log.isDebugEnabled()) {
            log.debug(message, params);
        }
    }
}
