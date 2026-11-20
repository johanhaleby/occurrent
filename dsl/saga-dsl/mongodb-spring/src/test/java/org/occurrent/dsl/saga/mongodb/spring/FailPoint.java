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

import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;

/**
 * MongoDB's {@code failCommand} fail point, which makes the server fail a named command on purpose. It needs a server
 * started with {@code enableTestCommands}, which {@link #container()} gives. Closing it switches it off.
 */
final class FailPoint implements AutoCloseable {
    private final MongoClient client;

    private FailPoint(MongoClient client) {
        this.client = client;
    }

    /** A single-node replica set that accepts {@code configureFailPoint}. Replaces the command {@code withReplicaSet()} set, which is the same one plus the parameter */
    static MongoDBContainer container() {
        return ReplicaSetReadyMongoDBContainer.withDefaultVersion().withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    }

    /** Fails {@code times} calls of {@code command} with {@code errorCode}, and labels the failure with {@code labels} */
    static FailPoint failing(MongoClient client, String command, int times, int errorCode, String... labels) {
        return configure(client, new Document("times", times), new Document("failCommands", List.of(command)).append("errorCode", errorCode).append("errorLabels", List.of(labels)));
    }

    /** Lets the first {@code skip} calls of {@code command} through, then fails every call with {@code errorCode} */
    static FailPoint failingAfter(MongoClient client, String command, int skip, int errorCode) {
        return configure(client, new Document("skip", skip), new Document("failCommands", List.of(command)).append("errorCode", errorCode));
    }

    /** Runs the first {@code times} calls of {@code command} to completion and answers each with a write concern error, so the effect is there and the caller is told it may not be */
    static FailPoint answeringWithAWriteConcernError(MongoClient client, String command, int times, int code) {
        return configure(client, new Document("times", times), new Document("failCommands", List.of(command))
                .append("writeConcernError", new Document("code", code).append("errmsg", "failpoint")));
    }

    private static FailPoint configure(MongoClient client, Object mode, Document data) {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", mode).append("data", data));
        return new FailPoint(client);
    }

    @Override
    public void close() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
    }
}
