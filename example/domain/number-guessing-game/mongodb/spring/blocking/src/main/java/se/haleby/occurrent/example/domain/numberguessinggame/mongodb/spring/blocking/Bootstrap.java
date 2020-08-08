package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.retry.annotation.EnableRetry;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;

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
        return new SpringBlockingMongoEventStore(template, new EventStoreConfig(EVENTS_COLLECTION_NAME, StreamConsistencyGuarantee.transactional("streamVersion", transactionManager), TimeRepresentation.DATE));
    }

    @Bean
    public SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB changeStreamer(MongoTemplate mongoTemplate) {
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer(mongoTemplate);
        SpringBlockingChangeStreamerForMongoDB streamer = new SpringBlockingChangeStreamerForMongoDB(EVENTS_COLLECTION_NAME, container, TimeRepresentation.DATE);
        return new SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB(streamer, mongoTemplate, "changeStreamPosition");
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