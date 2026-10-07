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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.Document;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * The server's {@code failCommand} fail point, which needs a MongoDB started with {@code enableTestCommands=1}. It is
 * scoped to the commands of one application name, so a test breaks the client of the subscription model and leaves the
 * client that writes the events alone.
 */
final class FailPoint {

    private FailPoint() {
    }

    /**
     * Makes the next {@code commandName} that a client with {@code applicationName} sends fail as described by {@code failure}.
     *
     * @param admin           A client to configure the fail point with.
     * @param applicationName The application name of the client that is to fail.
     * @param commandName     The command to fail.
     * @param failure         What the command does instead, for example {@code new Document("errorCode", 286)}.
     */
    static void failNext(MongoClient admin, String applicationName, String commandName, Document failure) {
        Document data = new Document("failCommands", List.of(commandName)).append("appName", applicationName);
        data.putAll(failure);
        configure(admin, new Document("mode", new Document("times", 1)).append("data", data));
    }

    static void off(MongoClient admin) {
        configure(admin, new Document("mode", "off"));
    }

    private static void configure(MongoClient admin, Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        Mono.from(admin.getDatabase("admin").runCommand(command)).block();
    }
}
