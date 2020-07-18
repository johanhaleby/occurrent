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
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;

import static se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.transactional;

@SpringBootApplication
@EnableMongoRepositories
public class SpringAdhocProjectionApplication {

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public EventStore eventStore(MongoTemplate mongoTemplate, TransactionTemplate transactionTemplate) {
        return new SpringBlockingMongoEventStore(mongoTemplate, "events", transactional("stream-consistency", transactionTemplate));
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}