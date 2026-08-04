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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * A {@link MongoDBContainer} that does not report itself started until a writable primary is reachable
 * through the mapped host port, and that hands out a database name no other test can touch.
 * <p>
 * <h2>Readiness</h2>
 * {@link MongoDBContainer#withReplicaSet()} waits for a primary inside the container, but on a loaded CI
 * runner the replica set can still be mid-election when the driver first connects from the host, which
 * surfaces as a "no server matches primary" or "connection refused" timeout in a test's first operations.
 * This container performs a real write over the mapped port before returning from start, so a test, and the
 * Spring context wired through {@code @ServiceConnection}, only ever sees a ready primary. The check runs
 * once per container start and costs a few milliseconds when healthy.
 * <p>
 * <h2>Isolation</h2>
 * {@link MongoDBContainer#getReplicaSetUrl()} names the database {@code test}, so every test that used it
 * shared one database. Since {@link FlushMongoDBExtension} drops the whole database before each test, two
 * overlapping Maven runs on one machine deleted each other's data, and the symptom read exactly like the
 * event store losing a committed write. This container prefixes every database name with a scope that is
 * unique to one container object in one JVM, so a flush can only ever reach data this container owns:
 * <ul>
 *     <li>the OS process id separates two concurrent runs, since they are distinct live processes, and</li>
 *     <li>a counter separates container objects inside one JVM, which in practice means one database per
 *     test class, because the dominant shape is a single {@code @Container static} field per class.</li>
 * </ul>
 * Both {@code getReplicaSetUrl()} overloads are covered, because the no-argument one delegates to the other.
 * Appending a collection to the returned url still works, so {@code getReplicaSetUrl() + ".events"} names
 * the {@code events} collection inside this container's own database.
 * <p>
 * <h2>Version</h2>
 * The image defaults to the {@code test.mongo.version} system property (the repository-wide test version),
 * falling back to the version the build filtered into {@code occurrent-test-support.properties}. Constructing
 * the image name by hand from that system property yields {@code mongo:null} in an IDE, where Surefire is not
 * there to supply it, which is why {@link #withDefaultVersion()} exists.
 */
public final class ReplicaSetReadyMongoDBContainer extends MongoDBContainer {

    private static final Duration READINESS_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);
    private static final String VERSION_RESOURCE = "occurrent-test-support.properties";
    private static final String MONGO_VERSION_KEY = "mongo.version";
    private static final String READINESS_PROBE_SUFFIX = "readiness_probe";

    /**
     * MongoDB rejects a database name of 64 bytes or more. The driver does not check the length, so without
     * this the limit would surface as an opaque server error on the first operation instead of at the call
     * site that chose the name.
     */
    private static final int MAX_DATABASE_NAME_LENGTH = 63;

    /**
     * Shared by every container in this JVM, so a stale database left by a run that once held this process id
     * can be recognised and dropped.
     */
    private static final String JVM_SCOPE = "oc" + ProcessHandle.current().pid();

    private static final AtomicInteger CONTAINER_COUNT = new AtomicInteger();
    private static final AtomicBoolean SWEPT_THIS_JVM = new AtomicBoolean();

    private final String databaseScope = JVM_SCOPE + "_" + CONTAINER_COUNT.incrementAndGet();

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

    /**
     * {@inheritDoc}
     * <p>
     * The name is prefixed with this container's scope, so no other test class and no concurrent Maven run
     * can reach the returned database. The no-argument overload delegates here, so it is scoped as well.
     */
    @Override
    public String getReplicaSetUrl(String databaseName) {
        return super.getReplicaSetUrl(scopedDatabaseName(databaseScope, databaseName));
    }

    // Package private so the naming rules can be tested without a Docker daemon.
    static String scopedDatabaseName(String scope, String databaseName) {
        String scopedName = scope + "_" + databaseName;
        if (scopedName.indexOf('.') >= 0) {
            // A connection string splits the path on its first dot into database and collection, so a dot here
            // would silently move part of the name into the collection instead of failing.
            throw new IllegalArgumentException("A MongoDB database name cannot contain a dot, but was " + databaseName
                    + ". Pass the database name only, and append '.' plus the collection to the url instead.");
        }
        if (scopedName.length() > MAX_DATABASE_NAME_LENGTH) {
            throw new IllegalArgumentException("Scoping " + databaseName + " for this test run produces " + scopedName
                    + ", which is " + scopedName.length() + " characters and so exceeds MongoDB's limit of "
                    + MAX_DATABASE_NAME_LENGTH + ". Shorten the database name.");
        }
        return scopedName;
    }

    private static String defaultVersion() {
        String fromSystemProperty = System.getProperty("test.mongo.version");
        if (fromSystemProperty != null && !fromSystemProperty.isBlank()) {
            // Trimmed because an image name cannot contain spaces, and a padded -D value would otherwise only
            // show up as a container creation failure.
            return fromSystemProperty.trim();
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
            // Properties.load keeps trailing whitespace, and trimming before the check below means a padded
            // placeholder is still recognised as unfiltered.
            version = version.trim();
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
        // Reuse keeps a container alive between runs, so it can be holding databases from runs that are long
        // gone. The first container in this JVM sweeps every database this process id owns, which is safe
        // precisely because it runs before any of our own databases holds data. Later containers only drop
        // their own, since a sibling container's database may be in use by a Spring context that is still
        // cached.
        Predicate<String> stale = SWEPT_THIS_JVM.compareAndSet(false, true)
                ? ownedBy(JVM_SCOPE)
                : ownedBy(databaseScope);
        if (!dropDatabases(getConnectionString(), stale)) {
            // Cleanup is convenience, not correctness: every test that cares flushes its own database, and a
            // leftover is a few hundred kilobytes. Failing the run over it would be worse than leaking it.
            logger().warn("Failed to drop stale test databases, continuing anyway");
        }
        logger().info("MongoDB container {} owns the databases named {}_*", getContainerId(), databaseScope);
    }

    // A write from the host is the honest readiness signal. It proves a primary has been elected and is
    // reachable through the mapped port, which is exactly what the app does next. The probe client uses short
    // server-selection and connect timeouts so a not-yet-ready attempt fails within PROBE_TIMEOUT rather than
    // the driver's 30 second default, which keeps the loop honest about READINESS_TIMEOUT.
    private void awaitWritablePrimary() {
        long deadline = System.nanoTime() + READINESS_TIMEOUT.toNanos();
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (MongoClient client = MongoClients.create(shortTimeoutSettings(getConnectionString()))) {
                // Scoped to the JVM rather than to this container, on purpose in both directions. Not a shared
                // literal, so two concurrent runs cannot drop each other's probe mid-write. And not this
                // container's own database, because containerIsStarted runs on a reuse hit too, so a probe that
                // dropped the scoped database would wipe the run's data from the second container onwards.
                MongoDatabase probe = client.getDatabase(JVM_SCOPE + "_" + READINESS_PROBE_SUFFIX);
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

    /**
     * Databases belonging to the given scope. The trailing separator is what makes this safe to drop: without it
     * scope {@code oc4349} also matches a database pid 43490 is using right now, and scope {@code oc4349_1} also
     * matches this JVM's eighteenth container. Dropping either would recreate the very bug this scoping removes.
     */
    // Package private so the rule can be tested without a Docker daemon.
    static Predicate<String> ownedBy(String scope) {
        return name -> name.startsWith(scope + "_");
    }

    /**
     * @return whether the drop ran; false when the server could not be reached, which the caller decides how
     * loudly to report.
     */
    private static boolean dropDatabases(String server, Predicate<String> matches) {
        try (MongoClient client = MongoClients.create(shortTimeoutSettings(server))) {
            List<String> toDrop = new ArrayList<>();
            client.listDatabaseNames().forEach(name -> {
                if (matches.test(name)) {
                    toDrop.add(name);
                }
            });
            toDrop.forEach(name -> client.getDatabase(name).drop());
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static MongoClientSettings shortTimeoutSettings(String connectionString) {
        long probeMillis = PROBE_TIMEOUT.toMillis();
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
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
