package se.haleby.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import org.springframework.data.repository.CrudRepository;

public interface CurrentNameProjection extends CrudRepository<CurrentName, String> {
}
