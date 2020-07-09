package se.haleby.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;

@SpringBootApplication
@EnableMongoRepositories
public class ChangeStreamedProjectionsWithSpringAndMongoDBApplication {

    @Bean
    public EventStore eventStore(MongoOperations mongoOperations) {
        return new SpringBlockingMongoEventStore(mongoOperations, "events");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}