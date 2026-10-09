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

package org.occurrent.testsupport.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Makes MongoDB drop the change stream history from a given operation time, so a test can check what a change stream
 * started there does.
 * <p>
 * A fresh replica set keeps its whole oplog, so a change stream can start at any operation time, even
 * {@code Timestamp(1, 0)}. Only once the oldest oplog entry is newer than the start does MongoDB refuse it, with
 * error 286 ({@code ChangeStreamHistoryLost}). {@link #container()} starts a replica set with the smallest oplog size
 * MongoDB accepts, one megabyte, and {@link #dropHistoryFrom} writes until MongoDB has truncated the oplog past the
 * operation time.
 */
public final class ChangeStreamHistory {

    private static final Duration TIMEOUT = Duration.ofMinutes(3);
    private static final String FILLER = "x".repeat(100_000);

    private ChangeStreamHistory() {
    }

    /**
     * A single-node replica set with a one megabyte oplog.
     */
    public static ReplicaSetReadyMongoDBContainer container() {
        ReplicaSetReadyMongoDBContainer container = ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        container.withCommand("--replSet", "docker-rs", "--oplogSize", "1");
        return container;
    }

    /**
     * Writes into a collection of its own in {@code database} until the oldest entry in the oplog is newer than
     * {@code operationTime}, then drops that collection.
     *
     * @throws AssertionError if the oplog still reaches back to {@code operationTime} after three minutes
     */
    public static void dropHistoryFrom(ReplicaSetReadyMongoDBContainer container, String database, BsonTimestamp operationTime) {
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl(database))) {
            MongoDatabase db = client.getDatabase(database);
            MongoCollection<Document> filler = db.getCollection("change-stream-history-filler");
            MongoCollection<Document> oplog = client.getDatabase("local").getCollection("oplog.rs");
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (oldestOplogEntry(oplog).compareTo(operationTime) <= 0) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("The oplog still reaches back to " + operationTime + " after " + TIMEOUT);
                }
                List<Document> batch = IntStream.range(0, 20).mapToObj(__ -> new Document("filler", FILLER)).toList();
                filler.insertMany(batch);
            }
            filler.drop();
        }
    }

    private static BsonTimestamp oldestOplogEntry(MongoCollection<Document> oplog) {
        Document oldest = oplog.find().sort(new Document("$natural", 1)).first();
        if (oldest == null) {
            throw new AssertionError("The oplog is empty");
        }
        return oldest.get("ts", BsonTimestamp.class);
    }
}
