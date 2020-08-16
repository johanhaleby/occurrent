package se.haleby.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;

@SpringBootApplication
@EnableMongoRepositories
public class ChangeStreamedProjectionsWithSpringAndMongoDBApplication {

    private static final String EVENTS_COLLECTION = "events";

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate, MongoTransactionManager transactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        return new SpringBlockingMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Bean
    public SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB springBlockingChangeStreamerForMongoDB(MongoTemplate mongoTemplate) {
        SpringBlockingChangeStreamerForMongoDB springBlockingChangeStreamerForMongoDB = new SpringBlockingChangeStreamerForMongoDB(mongoTemplate, EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING);
        return new SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB(springBlockingChangeStreamerForMongoDB, mongoTemplate, "event-subscribers");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}