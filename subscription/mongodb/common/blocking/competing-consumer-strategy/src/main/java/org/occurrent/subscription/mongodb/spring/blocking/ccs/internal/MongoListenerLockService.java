/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package org.occurrent.subscription.mongodb.spring.blocking.ccs.internal;

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
import org.occurrent.retry.RetryStrategy;

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

class MongoListenerLockService {

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


                return Optional.of(lock);
            } catch (MongoCommandException e) {
                final ErrorCategory errorCategory = ErrorCategory.fromErrorCode(e.getErrorCode());

                if (errorCategory.equals(DUPLICATE_KEY)) {
                    return Optional.empty();
                }

                throw e;
            }
        });
    }

    static DeleteResult remove(MongoCollection<BsonDocument> collection, RetryStrategy retryStrategy, String subscriptionId) {
        return retryStrategy.execute(() -> collection.deleteOne(eq("_id", subscriptionId)));
    }

    static boolean commit(MongoCollection<BsonDocument> collection, Clock clock, RetryStrategy retryStrategy, Duration leaseTime, String subscriptionId, String subscriberId) throws LostLockException {
        return retryStrategy.execute(() -> {
            Instant newLeaseTime = clock.instant().plus(leaseTime);
            UpdateResult result = collection
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .updateOne(
                            and(
                                    eq("_id", subscriptionId),
                                    eq("subscriberId", subscriberId)),
                            set("expiresAt", newLeaseTime));

            return result.getMatchedCount() != 0;
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
}
