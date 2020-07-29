package se.haleby.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import se.haleby.occurrent.eventstore.api.reactor.EventStore;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.SpringReactorMongoEventStore;

import javax.annotation.PostConstruct;

import static se.haleby.occurrent.eventstore.mongodb.spring.reactor.StreamConsistencyGuarantee.transactional;


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
        return new SpringReactorMongoEventStore(mongoTemplate, new EventStoreConfig("events", transactional("stream-consistency", reactiveMongoTransactionManager), TimeRepresentation.RFC_3339_STRING));
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