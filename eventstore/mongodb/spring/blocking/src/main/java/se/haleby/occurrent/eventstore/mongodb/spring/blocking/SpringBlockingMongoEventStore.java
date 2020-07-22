package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteError;
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
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.api.common.DuplicateCloudEventException;
import se.haleby.occurrent.eventstore.api.common.WriteCondition;
import se.haleby.occurrent.eventstore.api.common.WriteCondition.StreamVersionWriteCondition;
import se.haleby.occurrent.eventstore.api.common.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.Transactional;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.TransactionalAnnotation;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToDocuments;
import static se.haleby.occurrent.eventstore.mongodb.spring.common.internal.ConditionToCriteriaConverter.convertConditionToCriteria;

public class SpringBlockingMongoEventStore implements EventStore {

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
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            eventStream = readEventStream(streamId, skip, limit, ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return convertToCloudEvent(cloudEventSerializer, eventStream);
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }
        writeInternal(streamId, writeCondition, events);
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        writeInternal(streamId, null, events);
    }

    private void writeInternal(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (streamConsistencyGuarantee instanceof None && writeCondition != null) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        List<Document> serializedEvents = convertToDocuments(cloudEventSerializer, streamId, events).collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            insertAll(serializedEvents);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            String streamVersionCollectionName = transactional.streamVersionCollectionName;
            transactional.transactionTemplate.executeWithoutResult(transactionStatus -> conditionallyWriteEvents(streamId, streamVersionCollectionName, writeCondition, serializedEvents));
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            String streamVersionCollectionName = ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName;
            conditionallyWriteEvents(streamId, streamVersionCollectionName, writeCondition, serializedEvents);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    @Override
    public boolean exists(String streamId) {
        return mongoTemplate.exists(query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);
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

    @SuppressWarnings("UnnecessaryLocalVariable")
    private void insertAll(List<Document> documents) {
        try {
            mongoTemplate.getCollection(eventStoreCollectionName).insertMany(documents);
        } catch (MongoBulkWriteException e) {
            DuplicateCloudEventException duplicateCloudEventException = e.getWriteErrors().stream()
                    .filter(error -> error.getCode() == 11000)
                    .map(WriteError::getMessage)
                    .filter(errorMessage -> errorMessage.contains("{ id: \"") && errorMessage.contains(", source: \""))
                    .map(errorMessage -> {
                        int idKeyStartIndex = errorMessage.indexOf("{ id: \"");
                        int idValueStartIndex = idKeyStartIndex + "{ id: \"".length();
                        int idValueEndIndex = errorMessage.indexOf("\"", idValueStartIndex);
                        String id = errorMessage.substring(idValueStartIndex, idValueEndIndex);

                        int sourceKeyStartIndex = errorMessage.indexOf(", source: \"", idValueEndIndex);
                        int sourceValueStartIndex = sourceKeyStartIndex + ", source: \"".length();
                        int sourceValueEndIndex = errorMessage.indexOf("\" }", sourceValueStartIndex);
                        String source = errorMessage.substring(sourceValueStartIndex, sourceValueEndIndex);

                        return new DuplicateCloudEventException(id, URI.create(source), e);
                    })
                    .findFirst()
                    .orElse(new DuplicateCloudEventException(null, null, e));

            throw duplicateCloudEventException;
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
        if (writeCondition == null) {
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
            // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
            // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
            mongoTemplate.setSessionSynchronization(ALWAYS);
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            createStreamVersionCollectionAndIndex(streamVersionCollectionName, mongoTemplate);
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            String streamVersionCollectionName = ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName;
            createStreamVersionCollectionAndIndex(streamVersionCollectionName, mongoTemplate);
        }
    }

    private static void createStreamVersionCollectionAndIndex(String streamVersionCollectionName, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(streamVersionCollectionName)) {
            mongoTemplate.createCollection(streamVersionCollectionName);
        }
        mongoTemplate.getCollection(streamVersionCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
    }
}