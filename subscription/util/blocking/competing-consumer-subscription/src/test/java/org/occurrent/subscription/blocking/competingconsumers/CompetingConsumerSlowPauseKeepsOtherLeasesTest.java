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

package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Losing the lease on a subscription whose change stream is still opening pauses it, and pausing a Spring
 * subscription in that state waits for the open to finish. That wait must not keep the node from refreshing its
 * other leases. The open is slowed by a {@code failCommand} fail point on the server, which blocks the
 * {@code aggregate} opening the change stream for 15 seconds, well past the 3 second lease.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSlowPauseKeepsOtherLeasesTest {
    @Container
    private static final MongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private CompetingConsumerSubscriptionModel nodeA;
    private SpringMongoLeaseCompetingConsumerStrategy strategyB;
    private MongoClient client;

    @AfterEach
    void shutdown() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        if (nodeA != null) nodeA.shutdown();
        if (strategyB != null) strategyB.shutdown();
    }

    @Test
    void losing_a_lease_on_a_subscription_still_opening_does_not_stall_the_other_leases_on_the_node() {
        ConnectionString cs = new ConnectionString(mongo.getReplicaSetUrl() + ".events");
        client = MongoClients.create(cs);
        MongoTemplate template = new MongoTemplate(client, requireNonNull(cs.getDatabase()));
        String locks = "locks-" + UUID.randomUUID();
        SpringMongoLeaseCompetingConsumerStrategy strategyA = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(3)).collectionName(locks).build();
        strategyB = new SpringMongoLeaseCompetingConsumerStrategy.Builder(template).leaseTime(Duration.ofSeconds(3)).collectionName(locks).build();
        nodeA = new CompetingConsumerSubscriptionModel(new SpringMongoSubscriptionModel(template, "events", TimeRepresentation.RFC_3339_STRING), strategyA);

        nodeA.subscribe("a", "healthy", null, StartAt.subscriptionModelDefault(), __ -> {
        }).waitUntilStarted();
        strategyB.registerCompetingConsumer("healthy", "b");

        // The next change stream to open stalls on the server for 15 seconds
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("aggregate")).append("blockConnection", true).append("blockTimeMS", 15_000)));
        nodeA.subscribe("a", "slow", null, StartAt.subscriptionModelDefault(), __ -> {
        });
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);

        // Another node takes "slow" over, so A's next refresh finds that lease lost and pauses "slow"
        template.getCollection(locks).updateOne(eq("_id", "slow"), combine(set("subscriberId", "someone-else"), set("expiresAt", Date.from(Instant.now().plusSeconds(60)))));

        await("A keeps its healthy lease").during(8, SECONDS).atMost(9, SECONDS)
                .until(() -> !strategyB.hasLock("healthy", "b"));
    }
}
