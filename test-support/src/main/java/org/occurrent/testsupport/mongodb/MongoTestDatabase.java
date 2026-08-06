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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * The database a container owns, as a {@link MongoDatabase}, for the test-side APIs that ask for one rather than for a
 * connection string. {@code OccurrentMongoFlush} is the reason this exists. It takes a database on purpose, because a
 * connection string is what let a test name a collection where it meant to name a database.
 * <p>
 * One {@link MongoClient} is kept per server for the lifetime of the JVM, and every test class asking for the same
 * server shares it. That is deliberate rather than incidental. A client per call would mean one per test method across
 * some ninety test classes, and the extension this replaces opened and closed one on every single {@code beforeEach}.
 * A shared client is safe here because the databases are not shared, since
 * {@link ReplicaSetReadyMongoDBContainer} gives each container object its own.
 */
public final class MongoTestDatabase {

    private static final Map<String, MongoClient> CLIENTS_BY_SERVER = new ConcurrentHashMap<>();

    private MongoTestDatabase() {
    }

    /**
     * The database this container owns.
     *
     * @param container a started container, must not be {@code null}
     * @return its database
     */
    public static MongoDatabase of(MongoDBContainer container) {
        return databaseAt(requireNonNull(container, "container must not be null").getReplicaSetUrl());
    }

    /**
     * The named database this container owns, for a test that asked its container for one by name.
     *
     * @param container    a started container, must not be {@code null}
     * @param databaseName the name the test passed to {@code getReplicaSetUrl}, must not be {@code null}
     * @return that database
     */
    public static MongoDatabase of(MongoDBContainer container, String databaseName) {
        requireNonNull(container, "container must not be null");
        requireNonNull(databaseName, "databaseName must not be null");
        return databaseAt(container.getReplicaSetUrl(databaseName));
    }

    /**
     * The database named in a connection string, for a test that already built one.
     *
     * @param connectionString a connection string carrying a database, must not be {@code null}
     * @return that database
     */
    public static MongoDatabase at(ConnectionString connectionString) {
        return databaseAt(requireNonNull(connectionString, "connectionString must not be null").getConnectionString());
    }

    private static MongoDatabase databaseAt(String url) {
        ConnectionString connectionString = new ConnectionString(url);
        String databaseName = requireNonNull(connectionString.getDatabase(),
                "The connection string names no database, so there is nothing to hand out: " + url);
        // Keyed on the server rather than on the whole url, so two test classes on one container share a client even
        // when they name different databases or collections.
        String server = String.join(",", connectionString.getHosts());
        MongoClient client = CLIENTS_BY_SERVER.computeIfAbsent(server, ignored -> MongoClients.create(url));
        return client.getDatabase(databaseName);
    }
}
