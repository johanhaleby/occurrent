package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import org.occurrent.subscription.api.blocking.BlockingSubscription;
import org.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import org.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionPositionStorageForMongoDB;
import org.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionWithPositionPersistenceForMongoDB;

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
    public PositionAwareBlockingSubscription positionAwareBlockingSubscription(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION, TimeRepresentation.RFC_3339_STRING);
    }

    @Bean
    public BlockingSubscriptionPositionStorage storage(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionPositionStorageForMongoDB(mongoTemplate, "subscriptions");
    }

    @Bean
    public BlockingSubscription<CloudEvent> springBlockingSubscriptionForMongoDB(PositionAwareBlockingSubscription subscription, BlockingSubscriptionPositionStorage storage) {
        return new SpringBlockingSubscriptionWithPositionPersistenceForMongoDB(subscription, storage);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}