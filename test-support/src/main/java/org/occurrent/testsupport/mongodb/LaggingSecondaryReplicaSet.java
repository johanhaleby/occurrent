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
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
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

/**
 * A three-node replica set whose last member can be told to stop applying the primary's writes, so a test can read from
 * a secondary that is known to be behind. A delayed member can't do this, since MongoDB doesn't advertise one to a
 * client.
 * <p>
 * The members reach each other by network alias and the host reaches them on the same port each listens on, so the
 * client settings from {@link #clientSettings()} resolve the aliases to the loopback address.
 */
public final class LaggingSecondaryReplicaSet implements AutoCloseable {

    private static final String REPLICA_SET = "lagging-rs";

    /**
     * Selects the member that {@link #stopApplyingWrites()} holds back.
     */
    public static final TagSet LAGGING_MEMBER = new TagSet(new Tag("role", "lagging"));

    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private final List<Integer> ports = new ArrayList<>();

    private LaggingSecondaryReplicaSet() {
    }

    public static LaggingSecondaryReplicaSet start() {
        LaggingSecondaryReplicaSet replicaSet = new LaggingSecondaryReplicaSet();
        try {
            replicaSet.startMembers();
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

    private void startMembers() {
        DockerImageName image = DockerImageName.parse(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName());
        for (int i = 0; i < 3; i++) {
            int port = freePort();
            ports.add(port);
            GenericContainer<?> container = new GenericContainer<>(image)
                    .withNetwork(network)
                    .withNetworkAliases(alias(i))
                    .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
                    .withCommand("--replSet", REPLICA_SET, "--bind_ip_all", "--port", String.valueOf(port), "--setParameter", "enableTestCommands=1")
                    .withCreateContainerCmdModifier(command -> command.getHostConfig()
                            .withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
                    .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1));
            containers.add(container);
            container.start();
        }
        initiate();
        awaitMembers();
    }

    private void initiate() {
        Document config = new Document("_id", REPLICA_SET).append("members", List.of(
                new Document("_id", 0).append("host", alias(0) + ":" + ports.get(0)).append("priority", 2),
                new Document("_id", 1).append("host", alias(1) + ":" + ports.get(1)).append("priority", 1),
                new Document("_id", 2).append("host", alias(2) + ":" + ports.get(2)).append("priority", 0)
                        .append("tags", new Document("role", "lagging"))));
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

    private MongoClient directClient() {
        return directClient(0);
    }

    private MongoClient directClient(int member) {
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost:" + ports.get(member) + "/?directConnection=true"))
                .build());
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
