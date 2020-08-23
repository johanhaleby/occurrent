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
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static java.util.Objects.requireNonNull;

public class FlushMongoDBExtension implements BeforeEachCallback {

    private final ConnectionString connectionString;

    public FlushMongoDBExtension(ConnectionString connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        String databaseName = requireNonNull(connectionString.getDatabase(), "Database cannot be null in MongoDB connection string");
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            mongoClient.getDatabase(databaseName).drop();
        }
    }
}