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

package org.occurrent.testsupport.mongodb;

import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records the commands a MongoDB client sends and what the server replied, so a test can check how a read was made:
 * which session, which read concern, whether it joined a transaction. Register it on the client settings with
 * {@code addCommandListener}.
 */
public final class RecordingCommandListener implements CommandListener {

    private final List<Command> commands = new CopyOnWriteArrayList<>();
    private final Map<Integer, Command> byRequestId = new ConcurrentHashMap<>();

    public static final class Command {
        private final String name;
        private final String database;
        private final BsonDocument document;
        private volatile @Nullable BsonDocument reply;

        private Command(String name, String database, BsonDocument document) {
            this.name = name;
            this.database = database;
            this.document = document;
        }

        public String name() {
            return name;
        }

        public String database() {
            return database;
        }

        public @Nullable String collection() {
            return document.containsKey(name) && document.get(name) instanceof BsonString collection ? collection.getValue() : null;
        }

        public BsonDocument document() {
            return document;
        }

        public @Nullable BsonDocument reply() {
            return reply;
        }

        public @Nullable BsonDocument sessionId() {
            return document.containsKey("lsid") ? document.getDocument("lsid") : null;
        }

        public @Nullable String readConcernLevel() {
            if (!document.containsKey("readConcern") || !document.getDocument("readConcern").containsKey("level")) {
                return null;
            }
            return document.getDocument("readConcern").getString("level").getValue();
        }

        /**
         * Whether the command asks the server to wait for a cluster time, which is how a causally consistent session
         * reads.
         */
        public boolean waitsForClusterTime() {
            return document.containsKey("readConcern") && document.getDocument("readConcern").containsKey("afterClusterTime");
        }

        public boolean inTransaction() {
            return document.containsKey("txnNumber") || document.containsKey("autocommit");
        }

        @Override
        public String toString() {
            return name + " on " + database + ": " + document.toJson();
        }
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        Command command = new Command(event.getCommandName(), event.getDatabaseName(), event.getCommand().clone());
        byRequestId.put(event.getRequestId(), command);
        commands.add(command);
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        Command command = byRequestId.remove(event.getRequestId());
        if (command != null) {
            command.reply = event.getResponse().clone();
        }
    }

    public List<Command> commands() {
        return List.copyOf(commands);
    }

    public List<Command> commands(String name, String collection) {
        return commands.stream().filter(command -> command.name.equals(name) && collection.equals(command.collection())).toList();
    }

    public void clear() {
        commands.clear();
    }
}
