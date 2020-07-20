package se.haleby.occurrent.example.eventstore.mongodb.spring.reactor.transactional;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface CurrentNameProjection extends ReactiveCrudRepository<CurrentName, String> {
}
