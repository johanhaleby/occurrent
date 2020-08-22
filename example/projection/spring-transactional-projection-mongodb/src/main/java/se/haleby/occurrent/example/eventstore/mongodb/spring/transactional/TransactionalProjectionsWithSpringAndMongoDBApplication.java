package se.haleby.occurrent.example.eventstore.mongodb.spring.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;

import javax.annotation.PostConstruct;

import static com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.EVERYTHING;

@SpringBootApplication
@EnableMongoRepositories
public class TransactionalProjectionsWithSpringAndMongoDBApplication {

    @Autowired
    private MongoOperations mongoOperations;

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate, MongoTransactionManager mongoTransactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(mongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        return new SpringBlockingMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // Configure jackson to add type information to each serialized object
        // Allows deserializing interfaces such as DomainEvent
        objectMapper.activateDefaultTyping(new LaissezFaireSubTypeValidator(), EVERYTHING);
        return objectMapper;
    }

    @PostConstruct
    void createCollectionForCurrentNameProjection() {
        // Cannot be done in a multi-document transaction
        mongoOperations.createCollection("current-name-projection");
    }
}