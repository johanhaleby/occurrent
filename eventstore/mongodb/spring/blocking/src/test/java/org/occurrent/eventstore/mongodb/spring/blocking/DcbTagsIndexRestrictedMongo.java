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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.List;

/**
 * A standalone mongod with authentication, and a user whose role can read and index one collection but cannot run
 * {@code listIndexes} on it. Used to show what a store does when the check cannot list the indexes.
 */
final class DcbTagsIndexRestrictedMongo {

    static final String DATABASE = "dcbtagsindexrestricted";

    private static final int MONGOD_PORT = 27017;
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASSWORD = "root-test-password";
    private static final String RESTRICTED_USER = "restricted";
    private static final String RESTRICTED_PASSWORD = "restricted-test-password";
    private static final String ROLE = "readAndIndexOnly";

    private DcbTagsIndexRestrictedMongo() {
    }

    static GenericContainer<?> container() {
        return new GenericContainer<>(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                .withEnv("MONGO_INITDB_ROOT_USERNAME", ROOT_USER)
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", ROOT_PASSWORD)
                // Same workaround as ReplicaSetReadyMongoDBContainer, so mongod starts on any kernel (SERVER-121912).
                .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
                .withExposedPorts(MONGOD_PORT)
                // The image starts a temporary mongod to create the root user, and then the real one, and both log this.
                .waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 2));
    }

    static MongoClient rootClient(GenericContainer<?> container) {
        return MongoClients.create(new ConnectionString("mongodb://" + ROOT_USER + ":" + ROOT_PASSWORD + "@" + hostAndPort(container) + "/?authSource=admin"));
    }

    /**
     * Creates the role and the user, replacing any left by an earlier test, and returns a client authenticated as that user.
     */
    static MongoClient restrictedClient(GenericContainer<?> container, MongoClient rootClient, String collection) {
        MongoDatabase database = rootClient.getDatabase(DATABASE);
        database.runCommand(new Document("dropAllUsersFromDatabase", 1));
        database.runCommand(new Document("dropAllRolesFromDatabase", 1));
        database.runCommand(new Document("createRole", ROLE)
                .append("privileges", List.of(
                        new Document("resource", new Document("db", DATABASE).append("collection", collection))
                                .append("actions", List.of("find", "createIndex")),
                        // The store asks whether its collections exist before it creates indexes.
                        new Document("resource", new Document("db", DATABASE).append("collection", ""))
                                .append("actions", List.of("listCollections"))))
                .append("roles", List.of()));
        database.runCommand(new Document("createUser", RESTRICTED_USER)
                .append("pwd", RESTRICTED_PASSWORD)
                .append("roles", List.of(ROLE)));
        return MongoClients.create(new ConnectionString("mongodb://" + RESTRICTED_USER + ":" + RESTRICTED_PASSWORD + "@" + hostAndPort(container) + "/" + DATABASE + "?authSource=" + DATABASE));
    }

    private static String hostAndPort(GenericContainer<?> container) {
        return container.getHost() + ":" + container.getMappedPort(MONGOD_PORT);
    }
}
