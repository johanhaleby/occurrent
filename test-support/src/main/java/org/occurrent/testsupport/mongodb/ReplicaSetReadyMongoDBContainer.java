/*
 * Copyright 2020 Johan Haleby
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

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A {@link MongoDBContainer} that does not report itself started until a writable primary is reachable
 * through the mapped host port. {@link MongoDBContainer#withReplicaSet()} waits for a primary inside the
 * container, but on a loaded CI runner the replica set can still be mid-election when the driver first
 * connects from the host, which surfaces as a "no server matches primary" or "connection refused" timeout
 * in a test's first operations. This container performs a real write over {@link #getReplicaSetUrl()} before
 * returning from start, so a test, and the Spring context wired through {@code @ServiceConnection}, only ever
 * sees a ready primary. The check runs once per container start and costs a few milliseconds when healthy.
 * <p>
 * The image defaults to the {@code test.mongo.version} system property (the repository-wide test version),
 * falling back to {@code 8.0}.
 */
public final class ReplicaSetReadyMongoDBContainer extends MongoDBContainer {

    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

    /**
     * Create a container for the given {@code mongo:...} image, running as a single-node replica set.
     *
     * @param dockerImageName the Mongo image, for example {@code mongo:8.0}
     */
    public ReplicaSetReadyMongoDBContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
        withReplicaSet();
    }

    /**
     * Create a container for the {@code test.mongo.version} system property, defaulting to {@code 8.0}.
     */
    public static ReplicaSetReadyMongoDBContainer withDefaultVersion() {
        return new ReplicaSetReadyMongoDBContainer("mongo:" + System.getProperty("test.mongo.version", "8.0"));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarted(containerInfo, reused);
        awaitWritablePrimary();
    }

    // A write from the host is the honest readiness signal. It proves a primary has been elected and is
    // reachable through the mapped port, which is exactly what the app does next.
    private void awaitWritablePrimary() {
        long deadline = System.nanoTime() + READINESS_TIMEOUT.toNanos();
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (MongoClient client = MongoClients.create(getReplicaSetUrl())) {
                MongoDatabase probe = client.getDatabase("occurrent-readiness-probe");
                probe.getCollection("ping").insertOne(new Document("ok", 1));
                probe.drop();
                return;
            } catch (RuntimeException e) {
                lastFailure = e;
                sleep();
            }
        }
        throw new IllegalStateException("MongoDB replica set did not accept a write within " + READINESS_TIMEOUT
                + " of container start", lastFailure);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the MongoDB replica set to become writable", e);
        }
    }
}
