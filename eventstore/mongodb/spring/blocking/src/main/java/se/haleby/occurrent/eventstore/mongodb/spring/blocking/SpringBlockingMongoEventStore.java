package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.StreamUtils;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.Transactional;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.mongodb.converter.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToDocument;
import static se.haleby.occurrent.eventstore.mongodb.spring.common.internal.ConditionToCriteriaConverter.convertConditionToCriteria;

public class SpringBlockingMongoEventStore implements EventStore, EventStoreOperations {

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final MongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final StreamConsistencyGuarantee streamConsistencyGuarantee;
    private final EventFormat cloudEventSerializer;

    public SpringBlockingMongoEventStore(MongoTemplate mongoTemplate, String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee) {
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        initializeEventStore(eventStoreCollectionName, streamConsistencyGuarantee, mongoTemplate);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        final EventStream<Document> eventStream;
        if (streamConsistencyGuarantee instanceof None) {
            Stream<Document> stream = readCloudEvents(streamId, skip, limit);
            eventStream = new EventStreamImpl<>(streamId, 0, stream);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            eventStream = transactional.transactionTemplate.execute(transactionStatus -> readEventStream(streamId, skip, limit, transactional.streamVersionCollectionName));
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return requireNonNull(eventStream).map(document -> convertToCloudEvent(cloudEventSerializer, document));
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        } else if (streamConsistencyGuarantee instanceof None && !writeCondition.isAnyStreamVersion()) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " other than 'any' when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        List<Document> serializedEvents = events
                .map(cloudEvent -> convertToDocument(cloudEventSerializer, streamId, cloudEvent))
                .collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            insertAll(serializedEvents);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            String streamVersionCollectionName = transactional.streamVersionCollectionName;
            transactional.transactionTemplate.executeWithoutResult(transactionStatus -> conditionallyWriteEvents(streamId, streamVersionCollectionName, writeCondition, serializedEvents));
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, StreamVersionWriteCondition.any(), events);
    }

    @Override
    public boolean exists(String streamId) {
        return mongoTemplate.exists(query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");

        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            transactional.transactionTemplate.executeWithoutResult(__ -> {
                mongoTemplate.remove(query(where(ID).is(streamId)), transactional.streamVersionCollectionName);
                deleteAllEventsInEventStream(streamId);
            });
        } else if (streamConsistencyGuarantee instanceof None) {
            deleteAllEventsInEventStream(streamId);
        }
    }

    @Override
    public void deleteAllEventsInEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");
        Runnable deleteEvents = () -> mongoTemplate.remove(query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);

        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            transactional.transactionTemplate.executeWithoutResult(__ -> deleteEvents.run());
        } else if (streamConsistencyGuarantee instanceof None) {
            deleteEvents.run();
        }
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        mongoTemplate.remove(query(where("id").is(cloudEventId).and("source").is(cloudEventSource)), eventStoreCollectionName);
    }

    @SuppressWarnings("unused")
    private static class EventStreamImpl<T> implements EventStream<T> {
        private String _id;
        private long version;
        private Stream<T> events;

        @SuppressWarnings("unused")
        EventStreamImpl() {
        }

        EventStreamImpl(String _id, long version, Stream<T> events) {
            this._id = _id;
            this.version = version;
            this.events = events;
        }

        @Override
        public String id() {
            return _id;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Stream<T> events() {
            return events;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        public void setEvents(Stream<T> events) {
            this.events = events;
        }
    }

    // Write
    private void conditionallyWriteEvents(String streamId, String streamVersionCollectionName, WriteCondition writeCondition, List<Document> serializedEvents) {
        increaseStreamVersion(streamId, writeCondition, streamVersionCollectionName);
        insertAll(serializedEvents);
    }

    private void insertAll(List<Document> documents) {
        try {
            mongoTemplate.getCollection(eventStoreCollectionName).insertMany(documents);
        } catch (MongoBulkWriteException e) {
            throw translateToDuplicateCloudEventException(e);
        }
    }

    private void increaseStreamVersion(String streamId, WriteCondition writeCondition, String streamVersionCollectionName) {
        UpdateResult updateResult = mongoTemplate.updateFirst(query(generateUpdateCondition(streamId, writeCondition)),
                new Update().inc(VERSION, 1L), streamVersionCollectionName);

        if (updateResult.getMatchedCount() == 0) {
            Document document = mongoTemplate.findOne(query(where(ID).is(streamId)), Document.class, streamVersionCollectionName);
            if (document == null) {
                Map<String, Object> data = new HashMap<String, Object>() {{
                    put(ID, streamId);
                    put(VERSION, 1L);
                }};
                mongoTemplate.insert(new Document(data), streamVersionCollectionName);
            } else {
                long eventStreamVersion = document.getLong(VERSION);
                throw new WriteConditionNotFulfilledException(streamId, eventStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), eventStreamVersion));
            }
        }
    }

    private static Criteria generateUpdateCondition(String streamId, WriteCondition writeCondition) {
        Criteria streamEq = where(ID).is(streamId);
        if (writeCondition.isAnyStreamVersion()) {
            return streamEq;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        StreamVersionWriteCondition c = (StreamVersionWriteCondition) writeCondition;
        return streamEq.andOperator(convertConditionToCriteria(VERSION, c.condition));
    }

    // Read
    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, String streamVersionCollectionName) {
        @SuppressWarnings("unchecked")
        EventStreamImpl<Document> es = mongoTemplate.findOne(query(where(ID).is(streamId)), EventStreamImpl.class, streamVersionCollectionName);
        if (es == null) {
            return new EventStreamImpl<>(streamId, 0, Stream.empty());
        }

        Stream<Document> stream = readCloudEvents(streamId, skip, limit);
        es.setEvents(stream);
        return es;
    }

    private Stream<Document> readCloudEvents(String streamId, int skip, int limit) {
        Query query = query(where(STREAM_ID).is(streamId));
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            query.skip(skip).limit(limit);
        }
        return StreamUtils.createStreamFromIterator(mongoTemplate.stream(query, Document.class, eventStoreCollectionName));
    }

    // Initialization
    private static void initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(eventStoreCollectionName)) {
            mongoTemplate.createCollection(eventStoreCollectionName);
        }
        mongoTemplate.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        mongoTemplate.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        if (streamConsistencyGuarantee instanceof Transactional) {
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            createStreamVersionCollectionAndIndex(streamVersionCollectionName, mongoTemplate);
        }

        if (!(streamConsistencyGuarantee instanceof None)) {
            // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
            // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
            mongoTemplate.setSessionSynchronization(ALWAYS);
        }
    }

    private static void createStreamVersionCollectionAndIndex(String streamVersionCollectionName, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(streamVersionCollectionName)) {
            mongoTemplate.createCollection(streamVersionCollectionName);
        }
        mongoTemplate.getCollection(streamVersionCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
    }
}