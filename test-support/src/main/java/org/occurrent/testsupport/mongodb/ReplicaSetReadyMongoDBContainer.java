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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
 * falling back to the version the build filtered into {@code occurrent-test-support.properties}.
 */
public final class ReplicaSetReadyMongoDBContainer extends MongoDBContainer {

    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);
    private static final String VERSION_RESOURCE = "occurrent-test-support.properties";
    private static final String MONGO_VERSION_KEY = "mongo.version";

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
     * Create a container for the {@code test.mongo.version} system property, falling back to the version the
     * build was compiled against when that property is absent, which is what an IDE run gets.
     */
    public static ReplicaSetReadyMongoDBContainer withDefaultVersion() {
        return new ReplicaSetReadyMongoDBContainer("mongo:" + defaultVersion());
    }

    private static String defaultVersion() {
        String fromSystemProperty = System.getProperty("test.mongo.version");
        if (fromSystemProperty != null && !fromSystemProperty.isBlank()) {
            return fromSystemProperty;
        }
        // Surefire passes test.mongo.version for a Maven run. An IDE run gets nothing, so read the version the
        // build filtered into this resource rather than repeating it here, where it could drift from the pom.
        try (InputStream stream = ReplicaSetReadyMongoDBContainer.class.getResourceAsStream("/" + VERSION_RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException(VERSION_RESOURCE + " is missing from the classpath, so the Mongo version is unknown. Build test-support, or pass -Dtest.mongo.version.");
            }
            Properties properties = new Properties();
            properties.load(stream);
            String version = properties.getProperty(MONGO_VERSION_KEY);
            if (version == null || version.isBlank()) {
                throw new IllegalStateException(MONGO_VERSION_KEY + " is missing from " + VERSION_RESOURCE + ", so the Mongo version is unknown.");
            }
            if (version.startsWith("${")) {
                throw new IllegalStateException(VERSION_RESOURCE + " was copied without Maven resource filtering, so it still holds " + version + ". Build test-support through Maven, or pass -Dtest.mongo.version.");
            }
            return version;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + VERSION_RESOURCE + " to determine the Mongo version", e);
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarted(containerInfo, reused);
        awaitWritablePrimary();
    }

    // A write from the host is the honest readiness signal. It proves a primary has been elected and is
    // reachable through the mapped port, which is exactly what the app does next. The probe client uses short
    // server-selection and connect timeouts so a not-yet-ready attempt fails within PROBE_TIMEOUT rather than
    // the driver's 30 second default, which keeps the loop honest about READINESS_TIMEOUT.
    private void awaitWritablePrimary() {
        long deadline = System.nanoTime() + READINESS_TIMEOUT.toNanos();
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (MongoClient client = MongoClients.create(probeSettings())) {
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

    private MongoClientSettings probeSettings() {
        long probeMillis = PROBE_TIMEOUT.toMillis();
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(getReplicaSetUrl()))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(probeMillis, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(b -> b.connectTimeout((int) probeMillis, TimeUnit.MILLISECONDS)
                        .readTimeout((int) probeMillis, TimeUnit.MILLISECONDS))
                .build();
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
