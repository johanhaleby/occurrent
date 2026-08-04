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

package org.occurrent.testing.mongodb;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Empties a MongoDB database between tests by deleting documents, leaving the collections and their indexes in place.
 * <p>
 * Compose it with {@code OccurrentSubscriptionsExtension} from {@code occurrent-testing-junit-jupiter-blocking}, which runs it
 * after stopping every subscription and before resuming any, so nothing has to pin extension order:
 * <pre>{@code
 * @RegisterExtension
 * OccurrentSubscriptionsExtension subscriptions = OccurrentSubscriptionsExtension.stoppedByDefault(subscriptionModel)
 *         .clearingStateWith(OccurrentMongoFlush.everyCollectionIn(mongoTemplate.getDb()))
 *         .clearingCheckpoints(checkpointStorage);
 * }</pre>
 * A test with no subscriptions to stop can register it on its own with {@code @RegisterExtension} instead.
 *
 * <h2>Why it deletes rather than drops</h2>
 * Two things break when a test drops a collection or a database, and only one of them is loud.
 * <p>
 * <strong>A live change stream is invalidated.</strong> Both MongoDB subscription models watch a collection, so dropping
 * it ends the stream and invalidates the resume token with it. Stopping the model first does not help, because it
 * resumes from a position that now points into a collection that no longer exists. Every subscription resumed after the
 * drop then receives nothing, and receiving nothing looks exactly like having nothing to receive.
 * <p>
 * <strong>The event store's unique indexes are destroyed, and nothing recreates them.</strong> An Occurrent MongoDB
 * event store creates them in its constructor, so they come back only when a new store is constructed. A Spring test
 * context is cached across test classes, which means it is not. Optimistic concurrency and duplicate detection then
 * have no index behind them and their assertions pass for the wrong reason, quietly, for the rest of the run. This is
 * the worse of the two failures, because the other one at least fails a test.
 *
 * <h2>What it covers</h2>
 * {@link #everyCollectionIn(MongoDatabase)} names no collections, which is the point: Occurrent creates more of them
 * than an application tends to remember. Alongside the events there is a stream position collection, a DCB checkpoint
 * collection, the subscription checkpoint collection, and a competing consumer lock collection, and a hand written list
 * silently stops covering one the day a feature is switched on. Views and {@code system.*} collections are skipped.
 */
public final class OccurrentMongoFlush implements Runnable, BeforeEachCallback {

    private static final Document EVERYTHING = new Document();
    private static final String SYSTEM_COLLECTION_PREFIX = "system.";
    private static final String COLLECTION_TYPE = "collection";

    private final MongoDatabase database;
    private final boolean dropDatabase;
    private final List<String> only;
    private final Set<String> except = new LinkedHashSet<>();

    private OccurrentMongoFlush(MongoDatabase database, boolean dropDatabase, List<String> only) {
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.dropDatabase = dropDatabase;
        this.only = only;
    }

    /**
     * Delete every document from every collection in {@code database}.
     *
     * @param database the database to empty, must not be {@code null}
     * @return a new flush
     */
    public static OccurrentMongoFlush everyCollectionIn(MongoDatabase database) {
        return new OccurrentMongoFlush(database, false, List.of());
    }

    /**
     * Delete every document from the named collections only, for a database holding something a test has to keep.
     * Remember every collection Occurrent writes to, listed above, and prefer
     * {@link #everyCollectionIn(MongoDatabase)} unless a collection genuinely has to survive. Naming a collection that
     * does not exist does nothing.
     *
     * @param database        the database holding them, must not be {@code null}
     * @param collectionNames the collections to empty, must not be {@code null}, must not contain {@code null}, and must
     *                        not be empty
     * @return a new flush
     */
    public static OccurrentMongoFlush collectionsIn(MongoDatabase database, String... collectionNames) {
        Objects.requireNonNull(collectionNames, "collectionNames must not be null");
        if (collectionNames.length == 0) {
            throw new IllegalArgumentException("collectionNames must not be empty. Use everyCollectionIn(database) to "
                    + "empty every collection.");
        }
        for (String collectionName : collectionNames) {
            Objects.requireNonNull(collectionName, "collectionNames must not contain null");
        }
        return new OccurrentMongoFlush(database, false, List.of(collectionNames));
    }

    /**
     * Drops the whole database, collections and indexes included, rather than emptying it.
     * <p>
     * This is the one thing emptying cannot express, so it exists for a test asserting that a collection or an index
     * does <em>not</em> exist. Everywhere else prefer {@link #everyCollectionIn(MongoDatabase)}, because dropping
     * invalidates a live change stream and destroys the event store's unique indexes, both described above. It is safe
     * only when no subscription is open across the flush.
     *
     * @param database the database to drop, must not be {@code null}
     * @return a new flush
     */
    public static OccurrentMongoFlush droppingTheDatabaseIn(MongoDatabase database) {
        return new OccurrentMongoFlush(database, true, List.of());
    }

    /**
     * Keep these collections' documents. Chains onto {@link #everyCollectionIn(MongoDatabase)}.
     *
     * @param collectionNames the collections to leave alone, must not be {@code null} and must not contain {@code null}
     * @return this flush, so the call can be chained
     */
    public OccurrentMongoFlush except(String... collectionNames) {
        Objects.requireNonNull(collectionNames, "collectionNames must not be null");
        for (String collectionName : collectionNames) {
            except.add(Objects.requireNonNull(collectionName, "collectionNames must not contain null"));
        }
        return this;
    }

    /**
     * Empties the database now.
     *
     * @throws IllegalStateException if MongoDB refuses, naming the database, rather than leaving the previous test's
     *                               data in place without saying so
     */
    @Override
    public void run() {
        try {
            if (dropDatabase) {
                database.drop();
            } else {
                for (String collection : collectionsToEmpty()) {
                    database.getCollection(collection).deleteMany(EVERYTHING);
                }
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("Could not empty the MongoDB database '" + database.getName()
                    + "', so this test would have started against whatever the previous one left behind.", e);
        }
    }

    /**
     * Empties the database before each test. Deliberately not an {@code AfterEachCallback}: the next test's
     * {@code beforeEach} empties it anyway, so doing it twice costs time for nothing, and what a failing test left
     * behind is what you want to look at.
     */
    @Override
    public void beforeEach(ExtensionContext context) {
        run();
    }

    // Read the names before deleting anything, rather than deleting while iterating the server's own cursor.
    private List<String> collectionsToEmpty() {
        if (!only.isEmpty()) {
            return only.stream().filter(name -> !except.contains(name)).toList();
        }
        List<String> names = new ArrayList<>();
        for (Document collection : database.listCollections()) {
            String name = collection.getString("name");
            // A view cannot be written to, so deleting from one fails, and system collections are not a test's to empty.
            if (COLLECTION_TYPE.equals(collection.getString("type"))
                    && !name.startsWith(SYSTEM_COLLECTION_PREFIX)
                    && !except.contains(name)) {
                names.add(name);
            }
        }
        return names;
    }
}
