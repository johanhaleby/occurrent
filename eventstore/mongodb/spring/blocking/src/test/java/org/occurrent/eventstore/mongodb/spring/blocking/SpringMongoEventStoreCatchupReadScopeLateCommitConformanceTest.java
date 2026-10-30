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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.internal.catchup.CatchupReadScopeLateCommitConformance;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the catch-up read scope suite against the store.
 */
@Testcontainers
class SpringMongoEventStoreCatchupReadScopeLateCommitConformanceTest extends CatchupReadScopeLateCommitConformance {

    private static final Duration CHANGE_STREAM_TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @Override
    protected EventStoreFixture createFixture() {
        return new SpringMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    }

    @Override
    protected List<String> idsInChangeStreamFrom(String liveStart, String lastEventId) {
        BsonTimestamp from = OperationTimeLiveStart.parse(liveStart);
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        List<String> ids = new ArrayList<>();
        try (MongoClient client = MongoClients.create(connectionString);
             MongoCursor<ChangeStreamDocument<Document>> changes = client.getDatabase(connectionString.getDatabase()).getCollection("events").watch().startAtOperationTime(from).iterator()) {
            long deadline = System.nanoTime() + CHANGE_STREAM_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ChangeStreamDocument<Document> change = changes.tryNext();
                Document event = change == null ? null : change.getFullDocument();
                if (event != null) {
                    ids.add(event.getString("id"));
                    if (lastEventId.equals(event.getString("id"))) {
                        return ids;
                    }
                }
            }
        }
        throw new AssertionError("The change stream from " + liveStart + " did not report " + lastEventId + " within " + CHANGE_STREAM_TIMEOUT + ", it reported " + ids);
    }
}
