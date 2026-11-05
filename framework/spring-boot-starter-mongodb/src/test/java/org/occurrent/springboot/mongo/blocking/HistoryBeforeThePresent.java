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

package org.occurrent.springboot.mongo.blocking;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Checks that a subscription that starts at the present would open after the last write on the server, so it can't
 * be delivered the events a test wrote before it.
 * <p>
 * A blocking change-stream subscription at the present opens one increment after the operation time of a {@code ping}
 * reply ({@link MongoCommons#operationTimeAfter(Document)}). On the single-node replica set these tests use, that reply
 * already reports the last write, so the check passes on its first try. It retries for 10 seconds before it fails.
 */
final class HistoryBeforeThePresent {

    private HistoryBeforeThePresent() {
    }

    static void awaitedWith(MongoOperations mongoOperations) {
        BsonTimestamp lastWrite = lastWriteOn(mongoOperations);
        await().atMost(ofSeconds(10)).pollInterval(ofMillis(50)).untilAsserted(() -> {
            BsonTimestamp opensAt = MongoCommons.operationTimeAfter(mongoOperations.executeCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND));
            assertThat(opensAt).as("where a subscription at the present opens").isNotNull().isGreaterThan(lastWrite);
        });
    }

    // Skips the no-op entries the server writes to the oplog on its own.
    private static BsonTimestamp lastWriteOn(MongoOperations mongoOperations) {
        Document newestWrite = ((MongoTemplate) mongoOperations).getMongoDatabaseFactory().getMongoDatabase("local")
                .getCollection("oplog.rs")
                .find(new Document("op", new Document("$ne", "n")))
                .sort(new Document("$natural", -1))
                .limit(1)
                .first();
        assertThat(newestWrite).as("the newest write in the oplog").isNotNull();
        return newestWrite.get("ts", BsonTimestamp.class);
    }
}
