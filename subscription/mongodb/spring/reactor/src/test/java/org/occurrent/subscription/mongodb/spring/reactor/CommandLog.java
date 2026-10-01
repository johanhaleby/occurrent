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

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records the commands a MongoDB client sends, so a test can see how a subscription model talks to the server. Register
 * it on the client the subscription model uses.
 */
final class CommandLog implements CommandListener {

    /**
     * A command the client sent, with the reply once there is one.
     */
    static final class Sent {
        private final String name;
        private final BsonDocument command;
        private volatile BsonDocument reply;

        private Sent(String name, BsonDocument command) {
            this.name = name;
            this.command = command;
        }

        String name() {
            return name;
        }

        BsonDocument command() {
            return command;
        }

        BsonDocument lsid() {
            return command.getDocument("lsid");
        }

        // Null until the server has answered, and for a command that failed
        BsonDocument reply() {
            return reply;
        }

        long cursorId() {
            return reply.getDocument("cursor").getNumber("id").longValue();
        }

        boolean isChangeStream() {
            return name.equals("aggregate") && command.get("pipeline") != null && command.getArray("pipeline").getFirst().asDocument().containsKey("$changeStream");
        }

        @Override
        public String toString() {
            return name + " " + command.toJson();
        }

        // The $changeStream stage of an aggregate that opens a change stream
        BsonDocument changeStreamStage() {
            return command.getArray("pipeline").getFirst().asDocument().getDocument("$changeStream");
        }
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<Integer, Sent> byRequestId = new ConcurrentHashMap<>();

    @Override
    public void commandStarted(CommandStartedEvent event) {
        Sent command = new Sent(event.getCommandName(), event.getCommand().clone());
        byRequestId.put(event.getRequestId(), command);
        sent.add(command);
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        Sent command = byRequestId.remove(event.getRequestId());
        if (command != null) {
            command.reply = event.getResponse().clone();
        }
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        byRequestId.remove(event.getRequestId());
    }

    /**
     * @return Every command sent, in the order they were sent.
     */
    List<Sent> all() {
        return List.copyOf(sent);
    }

    List<Sent> named(String commandName) {
        return sent.stream().filter(command -> command.name().equals(commandName)).toList();
    }

    /**
     * @return The aggregates that opened a change stream, in the order they were sent. One that the server refused is among them.
     */
    List<Sent> changeStreamsOpened() {
        return sent.stream().filter(Sent::isChangeStream).toList();
    }

    /**
     * @return The value of {@code field} in the {@code $changeStream} stage of the aggregate, or null if it has none.
     */
    static BsonValue changeStreamField(Sent aggregate, String field) {
        return aggregate.changeStreamStage().get(field);
    }
}
