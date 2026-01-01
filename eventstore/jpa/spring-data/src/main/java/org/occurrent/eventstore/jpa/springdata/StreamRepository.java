package org.occurrent.eventstore.jpa.springdata;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
interface StreamRepository extends JpaRepository<StreamEntity, UUID> {

    Optional<StreamEntity> getByName(String name);

    boolean existsByName(String streamId);

    void deleteByName(String streamId);
}
