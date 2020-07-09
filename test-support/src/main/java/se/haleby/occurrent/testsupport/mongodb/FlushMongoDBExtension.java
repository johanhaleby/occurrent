package se.haleby.occurrent.testsupport.mongodb;

import com.mongodb.ConnectionString;
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
        MongoClients.create(connectionString)
                .getDatabase(requireNonNull(connectionString.getDatabase(), "Database cannot be null in MongoDB connection string"))
                .drop();
    }
}