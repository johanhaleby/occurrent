package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.eventstore.api.Filter;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreQueries;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.Transactional;

import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.api.Filter.TIME;
import static se.haleby.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import static se.haleby.occurrent.eventstore.api.WriteCondition.anyStreamVersion;
import static se.haleby.occurrent.eventstore.mongodb.internal.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToDocument;
import static se.haleby.occurrent.eventstore.mongodb.nativedriver.internal.ConditionConverter.convertConditionToBsonCriteria;
import static se.haleby.occurrent.eventstore.mongodb.nativedriver.internal.FilterToBsonFilterConverter.convertFilterToBsonFilter;

public class MongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final MongoCollection<Document> eventCollection;
    private final EventFormat cloudEventSerializer;
    private final StreamConsistencyGuarantee streamConsistencyGuarantee;
    private final MongoClient mongoClient;
    private final MongoDatabase mongoDatabase;
    private final TimeRepresentation timeRepresentation;

    public MongoEventStore(ConnectionString connectionString, EventStoreConfig config) {
        requireNonNull(connectionString, "Connection string cannot be null");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        log.info("Connecting to MongoDB using connection string: {}", connectionString);
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoDatabase = mongoClient.getDatabase(databaseName);
        String eventCollectionName = requireNonNull(connectionString.getCollection());
        eventCollection = mongoDatabase.getCollection(eventCollectionName);
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.streamConsistencyGuarantee = config.streamConsistencyGuarantee;
        this.timeRepresentation = config.timeRepresentation;
        initializeEventStore(eventCollectionName, streamConsistencyGuarantee, mongoDatabase);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        final EventStream<Document> eventStream;
        if (streamConsistencyGuarantee instanceof None) {
            Stream<Document> stream = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC, null);
            eventStream = new EventStreamImpl<>(streamId, 0, stream);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            eventStream = readEventStream(streamId, skip, limit, transactional);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return eventStream.map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, Transactional transactional) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                Document document = mongoDatabase
                        .getCollection(transactional.streamVersionCollectionName)
                        .find(clientSession, eq(ID, streamId), Document.class)
                        .first();

                if (document == null) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC, clientSession);
                return new EventStreamImpl<>(streamId, document.getLong(VERSION), stream);
            }, transactional.transactionOptions);
        }
    }

    private Stream<Document> readCloudEvents(Bson query, int skip, int limit, SortBy sortBy, ClientSession clientSession) {
        final FindIterable<Document> documentsWithoutSkipAndLimit;
        if (clientSession == null) {
            documentsWithoutSkipAndLimit = eventCollection.find(query);
        } else {
            documentsWithoutSkipAndLimit = eventCollection.find(clientSession, query);
        }

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }

        switch (sortBy) {
            case TIME_ASC:
                documentsWithoutSkipAndLimit.sort(ascending(TIME));
                break;
            case TIME_DESC:
                documentsWithoutSkipAndLimit.sort(descending(TIME));
                break;
            case NATURAL_ASC:
                break;
            case NATURAL_DESC:
                documentsWithoutSkipAndLimit.sort(descending(ID));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + sortBy);
        }

        return StreamSupport.stream(documentsWithSkipAndLimit.spliterator(), false);
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, anyStreamVersion(), events);
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        } else if (streamConsistencyGuarantee instanceof None && !writeCondition.isAnyStreamVersion()) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " other than 'any' when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        List<Document> cloudEventDocuments = events
                .map(cloudEvent -> convertToDocument(cloudEventSerializer, timeRepresentation, streamId, cloudEvent))
                .collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            try {
                eventCollection.insertMany(cloudEventDocuments);
            } catch (MongoBulkWriteException e) {
                throw translateToDuplicateCloudEventException(e);
            }
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            consistentlyWrite(streamId, writeCondition, cloudEventDocuments);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    private void consistentlyWrite(String streamId, WriteCondition writeCondition, List<Document> serializedEvents) {
        Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
        TransactionOptions transactionOptions = transactional.transactionOptions;
        String streamVersionCollectionName = transactional.streamVersionCollectionName;
        try (ClientSession clientSession = mongoClient.startSession()) {
            clientSession.withTransaction(() -> {
                MongoCollection<Document> streamVersionCollection = mongoDatabase.getCollection(streamVersionCollectionName);

                UpdateResult updateResult = streamVersionCollection
                        .updateOne(clientSession, generateCriteriaFromCondition(streamId, writeCondition),
                                inc(VERSION, 1L));

                if (updateResult.getMatchedCount() == 0) {
                    Document document = streamVersionCollection.find(clientSession, eq(ID, streamId)).first();
                    if (document == null) {
                        Map<String, Object> data = new HashMap<String, Object>() {{
                            put(ID, streamId);
                            put(VERSION, 1L);
                        }};
                        streamVersionCollection.insertOne(clientSession, new Document(data));
                    } else {
                        long eventStreamVersion = document.getLong(VERSION);
                        throw new WriteConditionNotFulfilledException(streamId, eventStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), eventStreamVersion));
                    }
                }

                try {
                    eventCollection.insertMany(clientSession, serializedEvents);
                } catch (MongoBulkWriteException e) {
                    throw translateToDuplicateCloudEventException(e);
                }
                return "";
            }, transactionOptions);
        }
    }

    private static Bson generateCriteriaFromCondition(String streamId, WriteCondition writeCondition) {
        Bson streamEq = eq(ID, streamId);
        if (writeCondition.isAnyStreamVersion()) {
            return streamEq;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        StreamVersionWriteCondition c = (StreamVersionWriteCondition) writeCondition;
        return and(streamEq, convertConditionToBsonCriteria(VERSION, c.condition));
    }


    @Override
    public boolean exists(String streamId) {
        if (streamConsistencyGuarantee instanceof Transactional) {
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            return mongoDatabase.getCollection(streamVersionCollectionName).countDocuments(eq(ID, streamId)) > 0;
        } else {
            return eventCollection.countDocuments(eq(STREAM_ID, streamId)) > 0;
        }
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");
        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            TransactionOptions transactionOptions = transactional.transactionOptions;
            MongoCollection<Document> streamVersionCollection = mongoDatabase.getCollection(transactional.streamVersionCollectionName);
            try (ClientSession clientSession = mongoClient.startSession()) {
                clientSession.withTransaction(() -> {
                    streamVersionCollection.deleteMany(clientSession, eq(ID, streamId));
                    eventCollection.deleteMany(clientSession, eq(STREAM_ID, streamId));
                    return "";
                }, transactionOptions);
            }
        } else if (streamConsistencyGuarantee instanceof None) {
            eventCollection.deleteMany(eq(STREAM_ID, streamId));
        }
    }

    @Override
    public void deleteAllEventsInEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");
        if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            TransactionOptions transactionOptions = transactional.transactionOptions;
            try (ClientSession clientSession = mongoClient.startSession()) {
                clientSession.withTransaction(() -> {
                    eventCollection.deleteMany(clientSession, eq(STREAM_ID, streamId));
                    return "";
                }, transactionOptions);
            }
        } else if (streamConsistencyGuarantee instanceof None) {
            eventCollection.deleteMany(eq(STREAM_ID, streamId));
        }
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        eventCollection.deleteOne(uniqueCloudEvent(cloudEventId, cloudEventSource));
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireNonNull(updateFunction, "Update function cannot be null");

        Bson cloudEvent = uniqueCloudEvent(cloudEventId, cloudEventSource);
        final Optional<CloudEvent> result;
        if (streamConsistencyGuarantee instanceof None) {
            result = updateCloudEvent(updateFunction, () -> eventCollection.find(cloudEvent), updatedDocument -> eventCollection.replaceOne(cloudEvent, updatedDocument));
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            TransactionOptions transactionOptions = ((Transactional) streamConsistencyGuarantee).transactionOptions;
            try (ClientSession clientSession = mongoClient.startSession()) {
                result = clientSession.withTransaction(
                        () -> updateCloudEvent(updateFunction, () -> eventCollection.find(clientSession, cloudEvent), updatedDocument -> eventCollection.replaceOne(clientSession, cloudEvent, updatedDocument)),
                        transactionOptions);
            }
        } else {
            throw new IllegalStateException("Stream consistency guarantee " + streamConsistencyGuarantee.getClass().getName() + " is invalid");
        }
        return result;
    }

    private Optional<CloudEvent> updateCloudEvent(Function<CloudEvent, CloudEvent> fn, Supplier<FindIterable<Document>> cloudEventFinder, Function<Document, UpdateResult> cloudEventUpdater) {
        Document document = cloudEventFinder.get().first();
        if (document == null) {
            return Optional.empty();
        } else {
            CloudEvent currentCloudEvent = convertToCloudEvent(cloudEventSerializer, timeRepresentation, document);
            CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
            if (updatedCloudEvent == null) {
                throw new IllegalArgumentException("Cloud event update function is not allowed to return null");
            } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                String streamId = (String) currentCloudEvent.getExtension(STREAM_ID);
                Document updatedDocument = convertToDocument(cloudEventSerializer, timeRepresentation, streamId, updatedCloudEvent);
                updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                cloudEventUpdater.apply(updatedDocument);
            }
            return Optional.of(updatedCloudEvent);
        }
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        requireNonNull(filter, "Filter cannot be null");
        final Bson query = convertFilterToBsonFilter(timeRepresentation, filter);
        return readCloudEvents(query, skip, limit, sortBy, null)
                .map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    private static class EventStreamImpl<T> implements EventStream<T> {
        private final String id;
        private final long version;
        private final Stream<T> events;

        EventStreamImpl(String id, long version, Stream<T> events) {
            this.id = id;
            this.version = version;
            this.events = events;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Stream<T> events() {
            return events;
        }

    }

    private static void initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, MongoDatabase
            mongoDatabase) {
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        if (streamConsistencyGuarantee instanceof Transactional) {
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;
            createStreamVersionCollectionAndIndex(streamVersionCollectionName, mongoDatabase);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean collectionExists(MongoDatabase mongoDatabase, String collectionName) {
        for (String listCollectionName : mongoDatabase.listCollectionNames()) {
            if (listCollectionName.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private static void createStreamVersionCollectionAndIndex(String streamVersionCollectionName, MongoDatabase mongoDatabase) {
        if (!collectionExists(mongoDatabase, streamVersionCollectionName)) {
            mongoDatabase.createCollection(streamVersionCollectionName);
        }
        mongoDatabase.getCollection(streamVersionCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
    }

    private static Bson streamIdEqualTo(String streamId) {
        return eq(STREAM_ID, streamId);
    }

    private static Bson uniqueCloudEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");
        return and(eq("id", cloudEventId), eq("source", cloudEventSource.toString()));
    }
}