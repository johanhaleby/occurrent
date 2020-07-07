package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class FlushEventsInMongoDBExtension implements BeforeEachCallback {

    private final ConnectionString connectionString;

    public FlushEventsInMongoDBExtension(ConnectionString connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        String database = connectionString.getDatabase();
        String collection = connectionString.getCollection();
        MongoClients.create(connectionString).getDatabase(database).getCollection(collection).drop();
    }
}
