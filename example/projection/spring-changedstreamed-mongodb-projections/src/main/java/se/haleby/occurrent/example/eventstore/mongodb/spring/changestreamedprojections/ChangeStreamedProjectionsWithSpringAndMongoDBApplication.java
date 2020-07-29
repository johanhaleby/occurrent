package se.haleby.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee;

@SpringBootApplication
@EnableMongoRepositories
public class ChangeStreamedProjectionsWithSpringAndMongoDBApplication {

    private static final String EVENTS_COLLECTION = "events";

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate) {
        return new SpringBlockingMongoEventStore(mongoTemplate, new EventStoreConfig(EVENTS_COLLECTION, StreamConsistencyGuarantee.none(), TimeRepresentation.RFC_3339_STRING));
    }

    @Bean
    public SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB springBlockingChangeStreamerForMongoDB(MongoTemplate mongoTemplate) {
        SpringBlockingChangeStreamerForMongoDB springBlockingChangeStreamerForMongoDB = new SpringBlockingChangeStreamerForMongoDB(EVENTS_COLLECTION, new DefaultMessageListenerContainer(mongoTemplate));
        return new SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB(springBlockingChangeStreamerForMongoDB, mongoTemplate, "event-subscribers");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}