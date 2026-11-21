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

package org.occurrent.mongodb.deployment.internal;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MongoDeployment")
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoDeploymentTest {

    @Test
    void detects_REPLICA_SET_for_an_answer_with_a_replica_set_name() {
        assertThat(MongoDeployment.detect(answering(new Document("setName", "rs0")))).isEqualTo(MongoDeployment.REPLICA_SET);
    }

    @Test
    void detects_MONGOS_for_an_answer_from_a_mongos() {
        assertThat(MongoDeployment.detect(answering(new Document("msg", "isdbgrid")))).isEqualTo(MongoDeployment.MONGOS);
    }

    @Test
    void detects_MONGOS_for_an_answer_with_a_replica_set_name_that_comes_from_a_mongos() {
        assertThat(MongoDeployment.detect(answering(new Document("setName", "rs0").append("msg", "isdbgrid")))).isEqualTo(MongoDeployment.MONGOS);
    }

    @Test
    void detects_OTHER_for_an_answer_without_a_replica_set_name() {
        assertThat(MongoDeployment.detect(answering(new Document()))).as("a standalone server has no setName").isEqualTo(MongoDeployment.OTHER);
    }

    @Test
    void detects_OTHER_for_an_answer_with_other_fields_and_neither_msg_isdbgrid_nor_a_setName() {
        assertThat(MongoDeployment.detect(answering(new Document("isWritablePrimary", true)))).isEqualTo(MongoDeployment.OTHER);
    }

    @Test
    void asks_hello_and_nothing_else_when_the_server_knows_it() {
        List<Document> sent = new ArrayList<>();

        MongoDeployment.detect(command -> {
            sent.add(command);
            return new Document("setName", "rs0");
        });

        assertThat(sent).containsExactly(new Document("hello", 1));
    }

    @Test
    void falls_back_to_isMaster_when_hello_fails_with_command_not_found_and_uses_its_answer() {
        List<Document> sent = new ArrayList<>();
        AtomicReference<MongoDeployment> deployment = new AtomicReference<>();

        assertThatCode(() -> deployment.set(MongoDeployment.detect(oldServerAnswering(sent, new Document("setName", "rs0")))))
                .as("MongoDB 4.2.0 to 4.2.9 answer hello with code 59, and creating the store must not fail on that")
                .doesNotThrowAnyException();

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(deployment).as("the isMaster answer has a setName").hasValue(MongoDeployment.REPLICA_SET);
    }

    @Test
    void falls_back_to_isMaster_when_hello_fails_with_command_not_found_and_answers_MONGOS_for_a_mongos() {
        List<Document> sent = new ArrayList<>();
        AtomicReference<MongoDeployment> deployment = new AtomicReference<>();

        assertThatCode(() -> deployment.set(MongoDeployment.detect(oldServerAnswering(sent, new Document("msg", "isdbgrid")))))
                .doesNotThrowAnyException();

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(deployment).as("the isMaster answer is from a mongos").hasValue(MongoDeployment.MONGOS);
    }

    @Test
    void falls_back_to_isMaster_when_command_not_found_is_the_cause_of_the_exception_hello_throws() {
        List<Document> sent = new ArrayList<>();
        AtomicReference<MongoDeployment> deployment = new AtomicReference<>();
        Function<Document, Document> server = command -> {
            sent.add(command);
            if (command.containsKey("hello")) {
                throw new IllegalStateException("The Spring template wraps the driver's exception", new RuntimeException("Another layer", commandFailed(59)));
            }
            return new Document("setName", "rs0");
        };

        assertThatCode(() -> deployment.set(MongoDeployment.detect(server)))
                .as("a template wraps the driver's exception, so the code is looked for in the causes and not only in the exception thrown")
                .doesNotThrowAnyException();

        assertThat(sent).containsExactly(new Document("hello", 1), new Document("isMaster", 1));
        assertThat(deployment).hasValue(MongoDeployment.REPLICA_SET);
    }

    @Test
    void propagates_a_failure_of_hello_that_is_not_command_not_found() {
        MongoCommandException unauthorized = commandFailed(13);
        List<Document> sent = new ArrayList<>();

        assertThatThrownBy(() -> MongoDeployment.detect(command -> {
            sent.add(command);
            throw unauthorized;
        })).as("only a server that doesn't know hello gets the fallback, any other failure must stop the store from being created")
                .isSameAs(unauthorized);

        assertThat(sent).containsExactly(new Document("hello", 1));
    }

    @Test
    void propagates_a_failure_of_hello_that_is_not_a_command_exception() {
        IllegalStateException failure = new IllegalStateException("The server is unreachable");

        assertThatThrownBy(() -> MongoDeployment.detect(command -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void propagates_command_not_found_from_isMaster() {
        MongoCommandException notFoundAgain = commandFailed(59);

        assertThatThrownBy(() -> MongoDeployment.detect(command -> {
            throw command.containsKey("hello") ? commandFailed(59) : notFoundAgain;
        })).as("a server that knows neither command has nothing left to ask")
                .isSameAs(notFoundAgain);
    }

    private static Function<Document, Document> answering(Document answer) {
        return command -> answer;
    }

    // A server that fails hello with code 59, as MongoDB 4.2.0 to 4.2.9 do, and answers isMaster
    private static Function<Document, Document> oldServerAnswering(List<Document> sent, Document isMasterAnswer) {
        return command -> {
            sent.add(command);
            if (command.containsKey("hello")) {
                throw commandFailed(59);
            }
            return isMasterAnswer;
        };
    }

    private static MongoCommandException commandFailed(int code) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(0))
                .append("errmsg", new BsonString("command failed"))
                .append("code", new BsonInt32(code));
        return new MongoCommandException(response, new ServerAddress());
    }
}
