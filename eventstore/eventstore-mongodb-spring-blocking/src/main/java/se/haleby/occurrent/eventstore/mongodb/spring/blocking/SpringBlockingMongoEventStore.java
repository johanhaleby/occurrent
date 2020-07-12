package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.StreamUtils;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.Transactional;
import se.haleby.occurrent.eventstore.mongodb.spring.blocking.StreamConsistencyGuarantee.TransactionalAnnotation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

public class SpringBlockingMongoEventStore implements EventStore {

    private static final String STREAM_ID = "streamId";
    private static final String CLOUD_EVENT = "cloudEvent";
    private static final String ID = "_id";

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
        return requireNonNull(eventStream).map(Document::toJson).map(eventJsonString -> eventJsonString.getBytes(UTF_8)).map(cloudEventSerializer::deserialize);
    }

    @Override
    public void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        List<Document> serializedEvents = events.map(cloudEventSerializer::serialize)
                .map(bytes -> new String(bytes, UTF_8))
                .map(Document::parse)
                .map(cloudEvent -> {
                    Map<String, Object> data = new HashMap<>();
                    data.put(STREAM_ID, streamId);
                    data.put(CLOUD_EVENT, cloudEvent);
                    return new Document(data);
                })
                .collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            insertAll(serializedEvents);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            String streamVersionCollectionName = transactional.streamVersionCollectionName;
            transactional.transactionTemplate.executeWithoutResult(transactionStatus -> conditionallyWriteEvents(streamId, streamVersionCollectionName, expectedStreamVersion, serializedEvents));
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            String streamVersionCollectionName = ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName;
            conditionallyWriteEvents(streamId, streamVersionCollectionName, expectedStreamVersion, serializedEvents);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    @Override
    public boolean exists(String streamId) {
        return mongoTemplate.exists(query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);
    }

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
    private void conditionallyWriteEvents(String streamId, String streamVersionCollectionName, long expectedStreamVersion, List<Document> serializedEvents) {
        increaseStreamVersion(streamId, expectedStreamVersion, streamVersionCollectionName);
        insertAll(serializedEvents);
    }

    // TODO If inserts fail due to duplicate cloud event, remove this event from the documents list
    //  (and all events before this document since they have been successfully inserted) and retry!
    private void insertAll(List<Document> documents) {
        mongoTemplate.getCollection(eventStoreCollectionName).insertMany(documents);
    }

    private void increaseStreamVersion(String streamId, long expectedStreamVersion, String streamVersionCollectionName) {
        mongoTemplate.upsert(query(where(ID).is(streamId).and("version").is(expectedStreamVersion)),
                update("version", expectedStreamVersion + 1), streamVersionCollectionName);
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
        return StreamUtils.createStreamFromIterator(mongoTemplate.stream(query, Document.class, eventStoreCollectionName))
                .map(document -> document.get(CLOUD_EVENT, Document.class));
    }

    // Initialization
    private static void initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(eventStoreCollectionName)) {
            mongoTemplate.createCollection(eventStoreCollectionName);
        }
        mongoTemplate.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        mongoTemplate.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(CLOUD_EVENT + ".id"), Indexes.ascending(CLOUD_EVENT + ".source")), new IndexOptions().unique(true));
        if (streamConsistencyGuarantee instanceof Transactional) {
            // Need to be always in order for TransactionTemplate to work with mongo template!
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
        mongoTemplate.getCollection(streamVersionCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending("version")), new IndexOptions().unique(true));
    }
}