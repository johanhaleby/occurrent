package se.haleby.occurrent.example.eventstore.mongodb.spring.changestreamedprojections;

import org.springframework.data.repository.CrudRepository;

public interface CurrentNameProjection extends CrudRepository<CurrentName, String> {
}
