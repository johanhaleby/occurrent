package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;

@SpringBootApplication
@EnableMongoRepositories
public class SubscriptionProjectionsWithSpringAndMongoDBApplication {

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
    public SpringBlockingSubscriptionWithPositionPersistenceForMongoDB springBlockingSubscriptionForMongoDB(MongoTemplate mongoTemplate) {
        SpringBlockingSubscriptionForMongoDB springBlockingSubscriptionForMongoDB = new SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING);
        return new SpringBlockingSubscriptionWithPositionPersistenceForMongoDB(springBlockingSubscriptionForMongoDB, mongoTemplate, "event-subscribers");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}