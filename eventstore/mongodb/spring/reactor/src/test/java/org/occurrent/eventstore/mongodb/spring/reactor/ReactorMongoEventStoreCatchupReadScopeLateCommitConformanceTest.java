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

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
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
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runs the catch-up read scope suite against the store.
 */
@Testcontainers
class ReactorMongoEventStoreCatchupReadScopeLateCommitConformanceTest extends CatchupReadScopeLateCommitConformance {

    private static final Duration CHANGE_STREAM_TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    @Override
    protected EventStoreFixture createFixture() {
        return new ReactorMongoEventStoreConformanceFixture(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events"));
    }

    @Override
    protected List<String> idsInChangeStreamFrom(String liveStart, String lastEventId) {
        BsonTimestamp from = OperationTimeLiveStart.parse(liveStart);
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl());
        try (MongoClient client = MongoClients.create(connectionString)) {
            List<String> ids = new ArrayList<>();
            Flux.from(client.getDatabase(Objects.requireNonNull(connectionString.getDatabase())).getCollection("events").watch().startAtOperationTime(from))
                    .map(change -> change.getFullDocument())
                    .filter(Objects::nonNull)
                    .map((Document event) -> event.getString("id"))
                    .doOnNext(ids::add)
                    .takeUntil(lastEventId::equals)
                    .blockLast(CHANGE_STREAM_TIMEOUT);
            if (!ids.contains(lastEventId)) {
                throw new AssertionError("The change stream from " + liveStart + " did not report " + lastEventId + " within " + CHANGE_STREAM_TIMEOUT + ", it reported " + ids);
            }
            return ids;
        }
    }
}
