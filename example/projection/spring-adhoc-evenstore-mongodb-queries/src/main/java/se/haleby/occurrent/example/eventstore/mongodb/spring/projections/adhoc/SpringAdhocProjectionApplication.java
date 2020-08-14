package se.haleby.occurrent.example.eventstore.mongodb.spring.projections.adhoc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.transaction.support.TransactionTemplate;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;


@SpringBootApplication
@EnableMongoRepositories
public class SpringAdhocProjectionApplication {

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate, TransactionTemplate transactionTemplate) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(transactionTemplate).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        return new SpringBlockingMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}