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
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Docker-based. A store the server tells it is a mongos, answering {@code hello} with {@code msg: isdbgrid} while it
 * runs on a replica set, is not a store on a standalone server. It writes atomically and its read of an absent instance
 * is read again at read concern linearizable. Only a deployment that is neither a replica set nor a mongos refuses
 * transitions and skips the second read.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreMongosDeploymentTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-mongos";

    private final List<String> readConcernsOfTheReadsOfTheInstances = new CopyOnWriteArrayList<>();

    // Answers hello as a mongos does and passes every other command on to the replica set
    private static final class MongosAnsweringTemplate extends MongoTemplate {
        MongosAnsweringTemplate(com.mongodb.client.MongoClient client, String database) {
            super(client, database);
        }

        @Override
        public Document executeCommand(Document command) {
            if (command.containsKey("hello")) {
                return new Document("msg", "isdbgrid").append("ok", 1.0);
            }
            return super.executeCommand(command);
        }
    }

    private SpringMongoSagaStateStore<String> storeThatSeesAMongos() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-mongos-" + UUID.randomUUID()));
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                BsonDocument command = event.getCommand();
                if ("find".equals(event.getCommandName()) && COLLECTION.equals(command.getString("find").getValue())) {
                    readConcernsOfTheReadsOfTheInstances.add(command.getDocument("readConcern").getString("level").getValue());
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(listener).build();
        return new SpringMongoSagaStateStore<>(new MongosAnsweringTemplate(MongoClients.create(settings), requireNonNull(connectionString.getDatabase())), COLLECTION, String.class);
    }

    @Test
    void a_store_on_a_mongos_does_not_refuse_atomic_transitions() {
        SpringMongoSagaStateStore<String> store = storeThatSeesAMongos();

        assertThatCode(store::requireAtomicTransitions).doesNotThrowAnyException();
    }

    @Test
    void a_store_on_a_mongos_reads_an_absent_instance_again_at_read_concern_linearizable() {
        SpringMongoSagaStateStore<String> store = storeThatSeesAMongos();

        assertThat(SagaEnvelopes.of(store, null).find("missing")).as("the instance is absent").isEmpty();

        assertThat(readConcernsOfTheReadsOfTheInstances).as("the read concern of each read, in order").containsExactly("majority", "linearizable");
    }
}
