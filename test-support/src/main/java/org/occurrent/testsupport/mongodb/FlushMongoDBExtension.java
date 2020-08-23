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