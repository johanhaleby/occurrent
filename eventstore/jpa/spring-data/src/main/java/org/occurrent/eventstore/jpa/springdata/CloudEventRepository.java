package org.occurrent.eventstore.jpa.springdata;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CloudEventRepository extends JpaRepository<CloudEventEntity, UUID>,
        JpaSpecificationExecutor<CloudEventEntity> {
    List<CloudEventEntity> findByStream(StreamEntity stream, Pageable aPageable);

    void deleteByStream_Name(String streamId);

    Optional<CloudEventEntity> findByEventIdAndSource(String cloudEventId, URI cloudEventSource);

    void deleteByEventIdAndSource(String cloudEventId, URI cloudEventSource);

    long countByStream_Name(String aStreamId);
}
