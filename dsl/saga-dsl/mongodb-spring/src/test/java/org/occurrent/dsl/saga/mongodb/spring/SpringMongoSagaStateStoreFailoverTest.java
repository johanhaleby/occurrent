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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based, on a replica set of three members on one network, which is what a primary step-down and a loss of the
 * secondaries need. The driver connects straight to one member, because the members know each other by their network
 * names, which the host cannot resolve. A write that has not reached a majority must fail instead of returning, and an
 * acknowledged transition must read the same on the member that becomes primary.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(240)
class SpringMongoSagaStateStoreFailoverTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String COLLECTION = "saga-failover";
    private static final String QUARANTINE_ID = "q-1";
    private static final List<GenericContainer<?>> MEMBERS = new ArrayList<>();
    private static Network network;

    record Payment(long amount) {
    }

    @BeforeAll
    static void startAThreeMemberReplicaSet() {
        network = Network.newNetwork();
        // The members take the image and the environment of the shared container, since MongoDB 8.0 does not start on Linux 6.19 or newer without the GLIBC_TUNABLES it sets (SERVER-121912)
        ReplicaSetReadyMongoDBContainer template = ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        String image = template.getDockerImageName();
        for (int i = 0; i < 3; i++) {
            GenericContainer<?> member = new GenericContainer<>(image).withNetwork(network).withNetworkAliases("member" + i).withExposedPorts(27017).withEnv(template.getEnvMap())
                    .withCommand("mongod", "--replSet", "rs", "--bind_ip_all")
                    .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1));
            member.start();
            MEMBERS.add(member);
        }
        List<Document> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            members.add(new Document("_id", i).append("host", "member" + i + ":27017").append("priority", i == 0 ? 2 : 1));
        }
        try (MongoClient first = direct(0)) {
            first.getDatabase("admin").runCommand(new Document("replSetInitiate", new Document("_id", "rs").append("members", members)));
        }
        await().atMost(Duration.ofSeconds(60)).until(() -> primary() >= 0 && secondaries() == 2);
    }

    @AfterAll
    static void stop() {
        MEMBERS.forEach(GenericContainer::stop);
        network.close();
    }

    private static MongoClient direct(int member) {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri(member, "admin"))).build());
    }

    private static String uri(int member, String database) {
        GenericContainer<?> container = MEMBERS.get(member);
        return "mongodb://" + container.getHost() + ":" + container.getMappedPort(27017) + "/" + database + "?directConnection=true";
    }

    private static boolean isPrimary(int member) {
        try (MongoClient client = direct(member)) {
            return client.getDatabase("admin").runCommand(new Document("hello", 1)).getBoolean("isWritablePrimary", false);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static boolean isSecondary(int member) {
        try (MongoClient client = direct(member)) {
            return client.getDatabase("admin").runCommand(new Document("hello", 1)).getBoolean("secondary", false);
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static int primary() {
        for (int i = 0; i < 3; i++) {
            if (isPrimary(i)) {
                return i;
            }
        }
        return -1;
    }

    private static long secondaries() {
        return java.util.stream.IntStream.range(0, 3).filter(SpringMongoSagaStateStoreFailoverTest::isSecondary).count();
    }

    private static SpringMongoSagaStateStore<Payment> storeOn(int member, String database, RetryStrategy retryStrategy) {
        ConnectionString connectionString = new ConnectionString(uri(member, database));
        MongoTemplate template = new MongoTemplate(MongoClients.create(connectionString), database);
        return new SpringMongoSagaStateStore<>(template, COLLECTION, Payment.class, null, retryStrategy);
    }

    private static SagaEnvelope<Payment> releasing(String sagaId, long version) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.QUARANTINED, version, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true,
                new SagaFailure("order-1@7", 7L, NOW.minusSeconds(300), IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID), Map.of(), 1L);
    }

    private static SagaEnvelope<Payment> active(String sagaId, long version) {
        return new SagaEnvelope<>(sagaId, new Payment(12L), SagaStatus.ACTIVE, version, List.of(), Map.of(), null, NOW.minusSeconds(600), NOW, null, null, true, null, Map.of(), null);
    }

    private static void pause(List<Integer> members) {
        members.forEach(i -> MEMBERS.get(i).getDockerClient().pauseContainerCmd(MEMBERS.get(i).getContainerId()).exec());
    }

    private static void unpauseAndAwaitTheReplicaSet(List<Integer> members) {
        members.forEach(i -> MEMBERS.get(i).getDockerClient().unpauseContainerCmd(MEMBERS.get(i).getContainerId()).exec());
        await().atMost(Duration.ofSeconds(60)).until(() -> primary() >= 0 && secondaries() == 2);
    }

    // So that the primary that has lost both secondaries stays primary for as long as a test needs it to, since a primary that steps down gives a different failure
    private static void setElectionTimeout(Duration timeout) {
        try (MongoClient admin = direct(primary())) {
            Document config = admin.getDatabase("admin").runCommand(new Document("replSetGetConfig", 1)).get("config", Document.class);
            config.get("settings", Document.class).put("electionTimeoutMillis", (int) timeout.toMillis());
            config.put("version", config.getInteger("version") + 1);
            admin.getDatabase("admin").runCommand(new Document("replSetReconfig", config));
        }
    }

    private static com.mongodb.client.MongoCollection<Document> collectionOn(MongoClient client, String database) {
        return client.getDatabase(database).getCollection(COLLECTION);
    }

    @Test
    void a_delete_does_not_return_before_the_deletion_is_on_a_majority() {
        String database = "failover_" + UUID.randomUUID().toString().substring(0, 8);
        int primary = primary();
        SpringMongoSagaStateStore<Payment> store = storeOn(primary, database, RetryStrategy.fixed(1).maxAttempts(2));
        assertThat(SagaEnvelopes.of(store, null).compareAndSave("order-1", active("order-1", 1), 0)).isTrue();
        List<Integer> secondaries = java.util.stream.IntStream.range(0, 3).filter(i -> i != primary).boxed().toList();
        setElectionTimeout(Duration.ofMinutes(5));

        Throwable whileTheDeletionIsOnThePrimaryAlone;
        Throwable afterTheSecondariesAreBack;
        Document readAtMajority;
        try (MongoClient direct = direct(primary)) {
            pause(secondaries);
            try {
                // What the first attempt of a delete leaves behind when its write concern times out after the primary applied it
                long deleted = collectionOn(direct, database).withWriteConcern(WriteConcern.W1).deleteOne(Filters.eq("_id", "order-1")).getDeletedCount();
                assertThat(deleted).as("the deletion on the primary alone").isEqualTo(1);
                whileTheDeletionIsOnThePrimaryAlone = catchThrowable(() -> store.delete("order-1"));
            } finally {
                unpauseAndAwaitTheReplicaSet(secondaries);
            }
            afterTheSecondariesAreBack = catchThrowable(() -> store.delete("order-1"));
            readAtMajority = collectionOn(direct, database).withReadConcern(ReadConcern.MAJORITY).find(Filters.eq("_id", "order-1")).first();
        } finally {
            setElectionTimeout(Duration.ofSeconds(10));
        }

        assertAll(
                () -> assertThat(whileTheDeletionIsOnThePrimaryAlone).as("a delete whose deletion only the primary has must not return normally").isNotNull(),
                () -> assertThat(afterTheSecondariesAreBack).as("the delete returns once the secondaries have the deletion").isNull(),
                () -> assertThat(readAtMajority).as("the instance read at majority").isNull()
        );
    }

    @Test
    void a_transition_and_a_single_document_compare_and_set_do_not_return_while_no_secondary_has_the_write() {
        String database = "failover_" + UUID.randomUUID().toString().substring(0, 8);
        int primary = primary();
        SpringMongoSagaStateStore<Payment> store = storeOn(primary, database, RetryStrategy.none());
        SagaEnvelopes<Payment> envelopes = SagaEnvelopes.of(store, null);
        assertThat(envelopes.compareAndSave("order-1", releasing("order-1", 1), 0)).isTrue();
        List<Integer> secondaries = java.util.stream.IntStream.range(0, 3).filter(i -> i != primary).boxed().toList();
        secondaries.forEach(i -> MEMBERS.get(i).getDockerClient().pauseContainerCmd(MEMBERS.get(i).getContainerId()).exec());

        Throwable transition;
        Throwable compareAndSet;
        try {
            transition = catchThrowable(() -> envelopes.compareAndSaveSkipped(store, releasing("order-1", 2), 1, QUARANTINE_ID, new SkippedInput(1, "a@1")));
            compareAndSet = catchThrowable(() -> envelopes.compareAndSave("order-2", releasing("order-2", 1), 0));
        } finally {
            secondaries.forEach(i -> MEMBERS.get(i).getDockerClient().unpauseContainerCmd(MEMBERS.get(i).getContainerId()).exec());
            await().atMost(Duration.ofSeconds(60)).until(() -> primary() >= 0 && secondaries() == 2);
        }

        assertAll(
                () -> assertThat(transition).as("a transition no majority has must not return normally").isNotNull(),
                () -> assertThat(compareAndSet).as("a compare-and-set no majority has must not return normally").isNotNull()
        );
    }

    @Test
    void an_acknowledged_transition_reads_the_same_on_the_member_that_becomes_primary() {
        String database = "failover_" + UUID.randomUUID().toString().substring(0, 8);
        int oldPrimary = primary();
        SpringMongoSagaStateStore<Payment> store = storeOn(oldPrimary, database, RetryStrategy.none());
        SagaEnvelopes<Payment> envelopes = SagaEnvelopes.of(store, null);
        assertThat(envelopes.compareAndSave("order-1", releasing("order-1", 1), 0)).isTrue();
        assertThat(envelopes.compareAndSaveSkipped(store, releasing("order-1", 2), 1, QUARANTINE_ID, new SkippedInput(1, "a@1"))).isTrue();
        assertThat(envelopes.compareAndSaveSkipped(store, releasing("order-1", 3), 2, QUARANTINE_ID, new SkippedInput(2, "b@2"))).isTrue();

        try (MongoClient admin = direct(oldPrimary)) {
            admin.getDatabase("admin").runCommand(new Document("replSetStepDown", 30).append("secondaryCatchUpPeriodSecs", 10).append("force", true));
        } catch (RuntimeException closedByTheStepDown) {
            // the step-down closes the connection it was sent on
        }
        await().atMost(Duration.ofSeconds(60)).until(() -> primary() >= 0 && primary() != oldPrimary);
        SpringMongoSagaStateStore<Payment> onTheNewPrimary = storeOn(primary(), database, RetryStrategy.none());

        assertAll(
                () -> assertThat(SagaEnvelopes.of(onTheNewPrimary, null).find("order-1").orElseThrow().version()).as("version on the new primary").isEqualTo(3),
                () -> assertThat(onTheNewPrimary.findSkippedInputs("order-1", QUARANTINE_ID, 10)).as("records on the new primary").containsExactly(new SkippedInput(1, "a@1"), new SkippedInput(2, "b@2"))
        );
    }
}
