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

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A three-node replica set whose last member can be told to stop applying the primary's writes, so a test can read from
 * a secondary that is known to be behind. A delayed member can't do this, since MongoDB doesn't advertise one to a
 * client.
 * <p>
 * The members reach each other by network alias and the host reaches them on the same port each listens on, so the
 * client settings from {@link #clientSettings()} resolve the aliases to the loopback address.
 * <p>
 * A set from {@link #startForFailover()} can also be failed over on demand. Replication to both secondaries stops, the
 * initial primary steps down, and the first secondary is elected.
 */
public final class LaggingSecondaryReplicaSet implements AutoCloseable {

    private static final String REPLICA_SET = "lagging-rs";

    /**
     * Selects the member that {@link #stopApplyingWrites()} holds back.
     */
    public static final TagSet LAGGING_MEMBER = new TagSet(new Tag("role", "lagging"));

    /**
     * The member that is primary when the set has started, and the one {@link #stepDownInitialPrimary()} steps down.
     */
    public static final int INITIAL_PRIMARY = 0;

    /**
     * The member that is elected when {@link #stepDownInitialPrimary()} runs.
     */
    public static final int ELECTED_AFTER_STEP_DOWN = 1;

    private static final int SECOND_SECONDARY = 2;

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private final List<Integer> ports = new ArrayList<>();

    private LaggingSecondaryReplicaSet() {
    }

    public static LaggingSecondaryReplicaSet start() {
        return start(new Document());
    }

    /**
     * Starts a set that elects a new primary two seconds after the old one steps down, without waiting for the
     * secondaries to catch up first.
     */
    public static LaggingSecondaryReplicaSet startForFailover() {
        return start(new Document("electionTimeoutMillis", 2000).append("catchUpTimeoutMillis", 0));
    }

    private static LaggingSecondaryReplicaSet start(Document settings) {
        LaggingSecondaryReplicaSet replicaSet = new LaggingSecondaryReplicaSet();
        try {
            replicaSet.startMembers(settings);
        } catch (RuntimeException | Error e) {
            replicaSet.close();
            throw e;
        }
        return replicaSet;
    }

    public MongoClientSettings.Builder clientSettings() {
        return MongoClientSettings.builder()
                .applyToClusterSettings(cluster -> cluster
                        .hosts(List.of(new ServerAddress(alias(0), ports.get(0)), new ServerAddress(alias(1), ports.get(1)), new ServerAddress(alias(2), ports.get(2))))
                        .requiredReplicaSetName(REPLICA_SET))
                .inetAddressResolver(host -> List.of(InetAddress.getByAddress(host, new byte[]{127, 0, 0, 1})))
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary());
    }

    public MongoClientSettings.Builder laggingReaderSettings() {
        return clientSettings().readPreference(ReadPreference.secondary(LAGGING_MEMBER));
    }

    private void startMembers(Document settings) {
        DockerImageName image = DockerImageName.parse(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName());
        for (int member = 0; member < 3; member++) {
            startMember(member, image);
        }
        initiate(settings);
        awaitMembers();
    }

    // Docker hands out ephemeral host ports to other containers too, so a port that was free a moment ago can be taken
    private void startMember(int member, DockerImageName image) {
        for (int attempt = 1; ; attempt++) {
            int port = freePort();
            GenericContainer<?> container = new GenericContainer<>(image)
                    .withNetwork(network)
                    .withNetworkAliases(alias(member))
                    .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
                    .withCommand("--replSet", REPLICA_SET, "--bind_ip_all", "--port", String.valueOf(port), "--setParameter", "enableTestCommands=1")
                    // Docker 28 drops a port binding unless the port is also exposed
                    .withCreateContainerCmdModifier(command -> command
                            .withExposedPorts(new ExposedPort(port))
                            .getHostConfig()
                            .withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
                    .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1));
            try {
                container.start();
            } catch (ContainerLaunchException e) {
                container.stop();
                if (attempt == 5 || !portWasTaken(e)) {
                    throw e;
                }
                continue;
            }
            containers.add(container);
            ports.add(port);
            return;
        }
    }

    private static boolean portWasTaken(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("address already in use")) {
                return true;
            }
        }
        return false;
    }

    private void initiate(Document settings) {
        Document config = new Document("_id", REPLICA_SET).append("members", List.of(
                new Document("_id", 0).append("host", alias(0) + ":" + ports.get(0)).append("priority", 2),
                new Document("_id", 1).append("host", alias(1) + ":" + ports.get(1)).append("priority", 1),
                new Document("_id", 2).append("host", alias(2) + ":" + ports.get(2)).append("priority", 0)
                        .append("tags", new Document("role", "lagging"))));
        if (!settings.isEmpty()) {
            config.append("settings", settings);
        }
        try (MongoClient direct = directClient()) {
            direct.getDatabase("admin").runCommand(new Document("replSetInitiate", config));
        }
    }

    private void awaitMembers() {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        try (MongoClient direct = directClient()) {
            while (System.nanoTime() < deadline) {
                try {
                    Document status = direct.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
                    List<Document> members = status.getList("members", Document.class);
                    long primaries = members.stream().filter(member -> "PRIMARY".equals(member.getString("stateStr"))).count();
                    long secondaries = members.stream().filter(member -> "SECONDARY".equals(member.getString("stateStr"))).count();
                    if (members.size() == 3 && primaries == 1 && secondaries == 2) {
                        return;
                    }
                } catch (RuntimeException ignored) {
                    // the set is still electing
                }
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the replica set", e);
        }
        throw new IllegalStateException("The three-node replica set had no primary and two secondaries after 90 seconds");
    }

    public void stopApplyingWrites() {
        setApplyingWrites(false);
    }

    public void resumeApplyingWrites() {
        setApplyingWrites(true);
    }

    private void setApplyingWrites(boolean applying) {
        try (MongoClient direct = directClient(2)) {
            direct.getDatabase("admin").runCommand(new Document("configureFailPoint", "rsSyncApplyStop").append("mode", applying ? "off" : "alwaysOn"));
        }
    }

    /**
     * Makes the secondaries stop fetching the primary's writes, after every member holds all that has been written so
     * far. From here a write that waits for majority can't be acknowledged until {@link #resumeReplicatingToSecondaries()}.
     * Returns when both secondaries have stopped.
     */
    public void stopReplicatingToSecondaries() {
        try (MongoClient primary = directClient(INITIAL_PRIMARY)) {
            primary.getDatabase("sync").getCollection("members").withWriteConcern(new WriteConcern(3).withWTimeout(60, TimeUnit.SECONDS))
                    .insertOne(new Document("everyMemberHasThis", true));
            // The member to be elected stops last, since the writes that wake the others up reach it and make it at
            // least as up to date as the other secondary, which votes only for a candidate that is
            for (int member : List.of(SECOND_SECONDARY, ELECTED_AFTER_STEP_DOWN)) {
                setReplicationProducer(member, false);
                awaitProducerStopped(primary, member);
            }
        }
    }

    public void resumeReplicatingToSecondaries() {
        setReplicationProducer(ELECTED_AFTER_STEP_DOWN, true);
        setReplicationProducer(SECOND_SECONDARY, true);
    }

    private void setReplicationProducer(int member, boolean fetching) {
        try (MongoClient direct = directClient(member)) {
            direct.getDatabase("admin").runCommand(new Document("configureFailPoint", "stopReplProducer").append("mode", fetching ? "off" : "alwaysOn"));
        }
    }

    // The fail point only takes effect when the member fetches the next batch, so writes on the primary wake it up
    private void awaitProducerStopped(MongoClient primary, int member) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try {
            for (int wake = 0; System.nanoTime() < deadline; wake++) {
                if (containers.get(member).getLogs().contains("stopReplProducer fail point enabled")) {
                    return;
                }
                primary.getDatabase("sync").getCollection("wake").insertOne(new Document("wake", wake));
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for replication to stop", e);
        }
        throw new IllegalStateException("Member " + member + " was still fetching writes 60 seconds after it was told to stop");
    }

    /**
     * Makes the next {@code find} that a client with the given application name sends to the initial primary wait for
     * the given time before it runs.
     */
    public void holdNextFindOnInitialPrimary(String applicationName, Duration hold) {
        try (MongoClient direct = directClient(INITIAL_PRIMARY)) {
            direct.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("find")).append("appName", applicationName)
                            .append("blockConnection", true).append("blockTimeMS", (int) hold.toMillis())));
        }
    }

    /**
     * Steps the initial primary down, keeps it from being elected again, and returns when the member that
     * {@link #ELECTED_AFTER_STEP_DOWN follows it} is primary.
     */
    public void stepDownInitialPrimary() {
        try (MongoClient direct = directClient(INITIAL_PRIMARY)) {
            direct.getDatabase("admin").runCommand(new Document("replSetStepDown", 300).append("secondaryCatchUpPeriodSecs", 0).append("force", true));
        } catch (MongoException stepDownClosesTheConnection) {
            // the primary may drop the connection the command arrived on
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try (MongoClient elected = directClient(ELECTED_AFTER_STEP_DOWN)) {
            while (System.nanoTime() < deadline) {
                try {
                    if (elected.getDatabase("admin").runCommand(new Document("hello", 1)).getBoolean("isWritablePrimary", false)) {
                        return;
                    }
                } catch (RuntimeException ignored) {
                    // the set is still electing
                }
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the election", e);
        }
        throw new IllegalStateException("Member " + ELECTED_AFTER_STEP_DOWN + " was not primary 60 seconds after the primary stepped down");
    }

    /**
     * A client connected to one member only, whatever its role, with write concern 1 so it never waits on the others.
     */
    public MongoClient directClient(int member) {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:" + ports.get(member) + "/?directConnection=true"))
                .writeConcern(WriteConcern.W1)
                .build());
    }

    private MongoClient directClient() {
        return directClient(INITIAL_PRIMARY);
    }

    private static String alias(int member) {
        return "lagging-mongo-" + member;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        containers.forEach(GenericContainer::stop);
        network.close();
    }
}
