package org.occurrent.eventstore.jpa;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.WriteCondition.anyStreamVersion;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.zip;

import io.cloudevents.CloudEvent;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.val;
import org.hibernate.exception.ConstraintViolationException;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.functionalsupport.internal.FunctionalSupport;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

@Builder
public class JPAEventStore<TKey, T extends CloudEventDaoTraits<TKey>>
    implements EventStore, EventStoreOperations, EventStoreQueries {
  private final EventLogOperations<T> eventLogOperations;
  private final JPAEventLog<T, ?> eventLog;
  private final CloudEventConverter<T> converter;

  @Override
  public WriteResult write(String streamId, Stream<CloudEvent> events) {
    return write(streamId, anyStreamVersion(), events);
  }

  @Override
  @Transactional
  @jakarta.transaction.Transactional
  public WriteResult write(
      String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {

    if (writeCondition == null) {
      throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
    }

    val currentStreamVersion = eventLog.count(eventLogOperations.byStreamId(streamId));

    if (!isFulfilled(currentStreamVersion, writeCondition)) {
      throw new WriteConditionNotFulfilledException(
          streamId,
          currentStreamVersion,
          writeCondition,
          String.format(
              "%s was not fulfilled. Expected version %s but was %s.",
              WriteCondition.class.getSimpleName(), writeCondition, currentStreamVersion));
    }

    val eventsAndRevisions =
        zip(
                LongStream.iterate(currentStreamVersion + 1, i -> i + 1).boxed(),
                events,
                FunctionalSupport.Pair::new)
            .toList();

    List<T> daos =
        eventsAndRevisions.stream()
            .map(pair -> converter.toDao(pair.t1, streamId, pair.t2))
            .collect(Collectors.toList());

    if (daos.isEmpty()) {
      return new WriteResult(streamId, currentStreamVersion, currentStreamVersion);
    }

    try {
      eventLog.saveAll(daos);
    } catch (DataIntegrityViolationException e) {
      // TODO: gross. Move this to a mixin so it can be injected
      if (e.getCause() != null && e.getCause() instanceof ConstraintViolationException cve) {
        val regex = ".*Detail: Key\\s+\\(.*\\)=\\((.*)\\)\\s+already exists.*";
        val pattern = Pattern.compile(regex);
        //  Detail: Key (event_id)=(20d3bbf2-aa99-4fa0-b45a-98c48561dd47) already exists.] [insert
        // into cloud_events
        // (data,data_content_type,data_schema,event_id,source,spec_version,stream_id,stream_revision,subject,timestamp,type) values (?,?,?,?,?,?,?,?,?,?,?)]
        val match = pattern.matcher(e.getMessage());
        String problemUUID = null;
        while (match.find()) {
          problemUUID = match.group(1);
        }
        if (problemUUID != null) {
          final String finalProblemUUID = problemUUID;
          val eventOption =
              eventsAndRevisions.stream()
                  .map(x -> x.t2)
                  .filter(x -> x.getId().equals(finalProblemUUID))
                  .findAny();
          if (eventOption.isPresent()) {
            val event = eventOption.get();
            throw new DuplicateCloudEventException(
                event.getId(), event.getSource(), e.getMessage().trim(), e);
          }
        }
        throw e;
      }
    }
    val newVersion = eventsAndRevisions.stream().map(x -> x.t1).max(Long::compareTo).get();
    return new WriteResult(streamId, currentStreamVersion, newVersion);
  }

  private static boolean isFulfilled(long currentStreamVersion, WriteCondition writeCondition) {
    if (writeCondition.isAnyStreamVersion()) {
      return true;
    }

    if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
      throw new IllegalArgumentException(
          "Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
    }

    Condition<Long> condition =
        ((WriteCondition.StreamVersionWriteCondition) writeCondition).condition();
    return LongConditionEvaluator.evaluate(condition, currentStreamVersion);
  }

  @Override
  public boolean exists(String streamId) {
    return eventLog.count(eventLogOperations.byStreamId(streamId)) > 0;
  }

  @Override
  public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
    val events =
        eventLog
            .findAll(eventLogOperations.byStreamId(streamId), PageRequest.of(skip, limit))
            .map(converter::toCloudEvent)
            .stream();
    val currentRevision = this.count();
    return new EventStreamImpl<>(streamId, currentRevision, events);
  }

  @Override
  @Transactional
  @jakarta.transaction.Transactional
  public void deleteEventStream(String streamId) {
    eventLog.delete(eventLogOperations.byStreamId(streamId));
  }

  @Override
  @Transactional
  @jakarta.transaction.Transactional
  public void deleteEvent(String cloudEventId, URI cloudEventSource) {
    eventLog.delete(eventLogOperations.byCloudEventIdAndSource(cloudEventId, cloudEventSource));
  }

  @Override
  @Transactional
  @jakarta.transaction.Transactional
  public void delete(Filter filter) {
    eventLog.delete(eventLogOperations.byFilter(filter));
  }

  @Override
  public Optional<CloudEvent> updateEvent(
      String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
    val events =
        eventLog.findAll(
            eventLogOperations.byCloudEventIdAndSource(cloudEventId, cloudEventSource));
    if (events.isEmpty()) {
      return Optional.empty();
    }
    if (events.size() > 1) {
      // TODO: more specific exception?
      throw new RuntimeException("Found more than 1 event.");
    }
    val dao = events.get(0);
    val updated = updateFunction.apply(converter.toCloudEvent(dao));
    val updatedDao = converter.toDao(dao.streamRevision(), dao.streamId(), updated);
    updatedDao.setKey(dao.key());
    eventLog.save(updatedDao);
    return Optional.of(updated);
  }

  @Override
  public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
    return eventLog
        .findAll(
            eventLogOperations.sorted(eventLogOperations.byFilter(filter), sortBy),
            PageRequest.of(skip, limit))
        .stream()
        .map(converter::toCloudEvent);
  }

  @Override
  public long count(Filter filter) {
    requireNonNull(filter, "Filter cannot be null");
    if (filter instanceof Filter.All) {
      return eventLog.count();
    }
    return eventLog.count(eventLogOperations.byFilter(filter));
  }

  @Override
  public boolean exists(Filter filter) {
    requireNonNull(filter, "Filter cannot be null");
    return count(filter) > 0;
  }

  private record EventStreamImpl<T>(String id, long version, Stream<T> events)
      implements EventStream<T> {}
}
