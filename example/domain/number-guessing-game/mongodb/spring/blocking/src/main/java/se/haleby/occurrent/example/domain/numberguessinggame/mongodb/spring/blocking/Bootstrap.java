package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.retry.annotation.EnableRetry;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.BlockingSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionForMongoDB;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionPositionStorageForMongoDB;
import se.haleby.occurrent.subscription.mongodb.spring.blocking.SpringBlockingSubscriptionWithPositionPersistenceForMongoDB;

import java.net.URI;

@SpringBootApplication
@EnableRetry
@EnableMongoRepositories
@EnableRabbit
public class Bootstrap {
    private static final String NUMBER_GUESSING_GAME_TOPIC = "number-guessing-game";
    private static final String EVENTS_COLLECTION_NAME = "events";

    public static void main(String[] args) {
        SpringApplication.run(Bootstrap.class, args);
    }

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public SpringBlockingMongoEventStore eventStore(MongoTemplate template, MongoTransactionManager transactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(EVENTS_COLLECTION_NAME).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.DATE).build();
        return new SpringBlockingMongoEventStore(template, eventStoreConfig);
    }

    @Bean
    public SpringBlockingSubscriptionWithPositionPersistenceForMongoDB springBlockingSubscriptionWithPositionPersistenceForMongoDB(PositionAwareBlockingSubscription subscription, BlockingSubscriptionPositionStorage storage) {
        return new SpringBlockingSubscriptionWithPositionPersistenceForMongoDB(subscription, storage);
    }

    @Bean
    public PositionAwareBlockingSubscription subscription(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionForMongoDB(mongoTemplate, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE);
    }

    @Bean
    public BlockingSubscriptionPositionStorage storage(MongoTemplate mongoTemplate) {
        return new SpringBlockingSubscriptionPositionStorageForMongoDB(mongoTemplate, "subscriptions");
    }

    @Bean
    public BlockingSubscription<CloudEvent> subscriptionWithAutomaticPersistence(PositionAwareBlockingSubscription subscription, BlockingSubscriptionPositionStorage storage) {
        return new SpringBlockingSubscriptionWithPositionPersistenceForMongoDB(subscription, storage);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Serialization serialization(ObjectMapper objectMapper) {
        return new Serialization(objectMapper, URI.create("urn:occurrent:domain:numberguessinggame"));
    }

    @Bean
    public MessageConverter amqpMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    TopicExchange numberGuessingGameTopic() {
        return new TopicExchange(NUMBER_GUESSING_GAME_TOPIC);
    }
}