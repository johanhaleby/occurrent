package se.haleby.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import se.haleby.occurrent.changestreamer.mongodb.spring.reactor.SpringReactiveChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.spring.reactor.SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

@SpringBootApplication
public class ChangeStreamerFromMongoDBToSpringEventApplication {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Bean(destroyMethod = "close")
    public MongoClient mongoClient() {
        return MongoClients.create(connectionString());
    }

    @Bean
    public EventStore eventStore(MongoClient mongoClient) {
        ConnectionString connectionString = connectionString();
        String database = connectionString.getDatabase();
        String collection = connectionString.getCollection();

        return new MongoEventStore(mongoClient, database, collection, new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
    }

    @Bean
    public SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB changeStreamerForMongoDB(ReactiveMongoOperations mongoOperations) {
        SpringReactiveChangeStreamerForMongoDB streamer = new SpringReactiveChangeStreamerForMongoDB(mongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
        return new SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB(streamer, mongoOperations, "resumeTokens");
    }

    @Bean
    public ConnectionString connectionString() {
        return new ConnectionString(mongoUri + ".events");
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure jackson to add type information to each serialized object
        // Allows deserializing interfaces such as DomainEvent
        objectMapper.activateDefaultTyping(new LaissezFaireSubTypeValidator(), EVERYTHING);
        return objectMapper;
    }
}