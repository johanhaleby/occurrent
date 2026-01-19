package org.occurrent.eventstore.jpa.springdata;

import io.cloudevents.CloudEvent;
import jakarta.transaction.Transactional;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A JPA-based implementation of the {@link EventStore} interface.
 */
@Component
public class SpringDataJpaEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

    private final CloudEventRepository itsCloudEventRepository;
    private final StreamRepository itsStreamRepository;
    private final CloudEventMapper itsCloudEventMapper;
    private final QueryMapper itsQueryMapper;

    public SpringDataJpaEventStore(
            CloudEventRepository itsCloudEventRepository,
            StreamRepository itsStreamRepository,
            CloudEventMapper itsCloudEventMapper,
            QueryMapper itsQueryMapper) {
        this.itsCloudEventRepository = itsCloudEventRepository;
        this.itsStreamRepository = itsStreamRepository;
        this.itsCloudEventMapper = itsCloudEventMapper;
        this.itsQueryMapper = itsQueryMapper;
    }

    @Override
    @Transactional
    public WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        // Fetch the stream Version.
        StreamEntity stream = itsStreamRepository.getByName(streamId)
                .orElseGet(() -> {
                    StreamEntity newStream = new StreamEntity();
                    newStream.setName(streamId);
                    newStream.setVersion(0L);
                    return itsStreamRepository.save(newStream);
                });
        long oldVersion = stream.getVersion();
        // Evaluate the condition
        if (!evaluateCondition(writeCondition, oldVersion)) {
            throw new WriteConditionNotFulfilledException(
                    streamId,
                    stream.getVersion(),
                    writeCondition,
                    String.format("%s was not fulfilled. Expected version %s but was %s.",
                            WriteCondition.class.getSimpleName(),
                            writeCondition,
                            stream.getVersion()));
        }

        // enrich the events with stream information
        AtomicLong counter = new AtomicLong(stream.getVersion());
        List<CloudEventEntity> newEvents = events
                .map(e -> {
                    var res = itsCloudEventMapper.toEntity(e);
                    res.setStream(stream);
                    res.setStreamPosition(counter.incrementAndGet());
                    return res;
                })
                .toList();
        // Update the stream version
        stream.setVersion(counter.get());
        itsStreamRepository.save(stream);
        // Persist the events
        try {
            itsCloudEventRepository.saveAllAndFlush(newEvents);
        } catch (DataIntegrityViolationException dive){
            throw new DuplicateCloudEventException(streamId, null, null, dive);
        }
        return new WriteResult(streamId, oldVersion, stream.getVersion());
    }

    private boolean evaluateCondition(WriteCondition aCondition, long aVersion) {
        if (aCondition.isAnyStreamVersion()) {
            return true;
        }

        if (aCondition instanceof WriteCondition.StreamVersionWriteCondition condition) {
            return LongConditionEvaluator.evaluate(condition.condition(), aVersion);
        }
        return false;

    }

    @Override
    public boolean exists(String streamId) {
        // Better check if events are there ?
        return itsStreamRepository.existsByName(streamId);
    }

    @Override
    @Transactional
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        Optional<StreamEntity> stream = itsStreamRepository.getByName(streamId);
        if (stream.isEmpty()) {
            return new EventStreamImpl(streamId, 0L, List.of());
        }
        List<? extends CloudEvent> events = itsCloudEventRepository.findByStream(
                stream.orElse(null),
                new OffsetBasedPageRequest(
                        skip,
                        limit,
                        Sort.by(
                                Sort.Direction.ASC,
                                "streamPosition")));
        return new EventStreamImpl(
                stream.get().getName(),
                stream.get().getVersion(),
                (List<CloudEvent>) events);
    }

    @Override
    public WriteResult write(String streamId, Stream<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return itsCloudEventRepository.findAll(
                        itsQueryMapper.mapFilter(filter),
                        new OffsetBasedPageRequest(
                                skip,
                                limit,
                                itsQueryMapper.mapSortBy(sortBy)))
                .stream()
                .map(e -> (CloudEvent) e);
    }

    @Override
    public long count(Filter aFilter) {
        Specification<CloudEventEntity> spec = itsQueryMapper.mapFilter(aFilter);
        return itsCloudEventRepository.count(spec);
    }

    @Override
    public boolean exists(Filter filter) {
        Specification<CloudEventEntity> spec = itsQueryMapper.mapFilter(filter);
        return itsCloudEventRepository.exists(spec);
    }

    @Override
    @Transactional
    public void deleteEventStream(String streamId) {
        itsCloudEventRepository.deleteByStream_Name(streamId);
        itsStreamRepository.deleteByName(streamId);
    }

    @Override
    @Transactional
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        itsCloudEventRepository.deleteByEventIdAndSource(cloudEventId, cloudEventSource);

    }

    @Override
    @Transactional
    public void delete(Filter filter) {
        Specification<CloudEventEntity> spec = itsQueryMapper.mapFilter(filter);
        itsCloudEventRepository.delete(spec);
    }

    @Override
    @Transactional
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        Optional<CloudEventEntity> eventOpt = itsCloudEventRepository.findByEventIdAndSource(cloudEventId, cloudEventSource);
        if (eventOpt.isEmpty()) {
            return Optional.empty();
        }
        var event = eventOpt.get();
        UUID oldId = event.getPK();
        var newEvent = itsCloudEventMapper.toEntity(
                updateFunction.apply(event));
        if (newEvent == null) {
            throw new IllegalArgumentException("Cloud event update function is not allowed to return null");
        }
        newEvent.setPK(oldId);
        itsCloudEventRepository.save(newEvent);
        return Optional.of(event);
    }
}
