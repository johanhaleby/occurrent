package org.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.SpringReactorMongoEventStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import javax.annotation.PostConstruct;


@SpringBootApplication
@EnableReactiveMongoRepositories
public class TransactionalProjectionsWithSpringAndMongoDBApplication {

    @Autowired
    private MongoOperations mongoOperations;

    @Bean
    public ReactiveMongoTransactionManager transactionManager(ReactiveMongoDatabaseFactory dbFactory) {
        return new ReactiveMongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(ReactiveMongoTemplate mongoTemplate, ReactiveMongoTransactionManager reactiveMongoTransactionManager) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        return new SpringReactorMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @PostConstruct
    void createCollectionForCurrentNameProjection() {
        // Cannot be done in a multi-document transaction
        mongoOperations.createCollection("current-name-projection");
    }
}