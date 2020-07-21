package se.haleby.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.mongodb.ConnectionString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import se.haleby.occurrent.changestreamer.mongodb.spring.reactive.SpringReactiveChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.spring.reactive.SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

@SpringBootApplication
public class ChangeStreamerFromMongoDBToSpringEventApplication {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Bean
    public EventStore eventStore() {
        return new MongoEventStore(connectionString(), StreamConsistencyGuarantee.transactional("event-consistency"));
    }

    @Bean
    public SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB changeStreamerForMongoDB(ReactiveMongoOperations mongoOperations) {
        SpringReactiveChangeStreamerForMongoDB streamer = new SpringReactiveChangeStreamerForMongoDB(mongoOperations, "events");
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