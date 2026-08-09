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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code remove} leaves behind. This is the property ADR 116's fence rests on: a released lock has to be
 * retaken at a higher version than it started with, never reset to 0, or a fence built on top of it would stop the
 * subscription for good the first time a lease changed hands. {@link MongoLeaseTimingTest} covers everything about
 * whether a lease is held; this is only about what releasing one does to the document underneath it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("releasing a MongoDB lock")
@Timeout(30)
class MongoListenerLockServiceTest {

    private static final String DATABASE = "mongolistenerlockservice";
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final String SUBSCRIPTION = "a-subscription";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private MongoCollection<BsonDocument> locks;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @BeforeEach
    void startWithNoLocks() {
        locks = database.getCollection("competing-consumer-locks-" + UUID.randomUUID(), BsonDocument.class);
    }

    @AfterEach
    void dropTheLocks() {
        locks.drop();
    }

    private ListenerLock acquire(String subscriberId) {
        return MongoListenerLockService.acquireOrRefreshFor(locks, RetryStrategy.none(), LEASE, SUBSCRIPTION, subscriberId)
                .orElseThrow(() -> new IllegalStateException("Expected " + subscriberId + " to acquire the lock"));
    }

    private void release(String subscriberId) {
        MongoListenerLockService.remove(locks, RetryStrategy.none(), SUBSCRIPTION, subscriberId);
    }

    private BsonDocument readLockDocument() {
        return locks.find(eq("_id", SUBSCRIPTION)).first();
    }

    @Test
    void lets_the_next_acquirer_start_from_a_higher_version_than_it_would_have_gotten_fresh() {
        ListenerLock held = acquire("the-holder");
        release("the-holder");

        ListenerLock reacquired = acquire("the-rival");

        assertThat(reacquired.version())
                .as("a version that resets to 0 on release is what lets a rejoining node overwrite a checkpoint the "
                        + "previous holder already moved past, which is the whole failure a fencing token exists to "
                        + "prevent")
                .isGreaterThan(held.version());
    }

    @Test
    void lets_the_same_subscriber_reacquire_at_a_higher_version_too() {
        ListenerLock held = acquire("the-holder");
        release("the-holder");

        ListenerLock reacquired = acquire("the-holder");

        assertThat(reacquired.version())
                .as("releasing unsets subscriberId, so the same subscriber taking the lease back looks like a fresh "
                        + "takeover rather than a refresh, and gets the same version bump a stranger would")
                .isGreaterThan(held.version());
    }

    @Test
    void leaves_the_lock_document_in_the_collection_rather_than_deleting_it() {
        acquire("the-holder");

        release("the-holder");

        assertThat(readLockDocument())
                .as("a lock collection whose documents disappear on release is one a checkpoint store could be "
                        + "dropped independently of, which is exactly the recovery hazard ADR 116 rules out")
                .isNotNull();
    }

    @Test
    void clears_who_holds_the_lock_and_when_it_expires() {
        acquire("the-holder");

        release("the-holder");

        BsonDocument document = readLockDocument();
        assertThat(document.containsKey("subscriberId"))
                .as("a missing subscriberId is what isAllowedFor already reads as a free lock")
                .isFalse();
        assertThat(document.containsKey("expiresAt"))
                .as("a missing expiresAt is what lockIsExpiredExpr already reads as expired, and clearing it also "
                        + "lets anyone reading the collection tell a held lease from one nobody holds")
                .isFalse();
    }

    @Test
    void does_nothing_when_asked_to_release_a_lock_a_different_subscriber_holds() {
        ListenerLock held = acquire("the-holder");

        release("someone-else");

        BsonDocument document = readLockDocument();
        assertThat(document.getString("subscriberId").getValue()).isEqualTo("the-holder");
        assertThat(document.getNumber("version").longValue()).isEqualTo(held.version());
    }
}
