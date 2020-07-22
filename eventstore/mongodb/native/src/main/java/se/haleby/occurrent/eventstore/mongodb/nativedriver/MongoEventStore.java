package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
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
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition.MultiOperation;
import se.haleby.occurrent.eventstore.api.WriteCondition.Condition.Operation;
import se.haleby.occurrent.eventstore.api.WriteCondition.MultiOperationName;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.converter.OccurrentCloudEventMongoDBDocumentMapper.convertToDocuments;

public class MongoEventStore implements EventStore {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final MongoCollection<Document> eventCollection;
    private final EventFormat cloudEventSerializer;
    private final StreamConsistencyGuarantee streamConsistencyGuarantee;
    private final MongoClient mongoClient;
    private final String databaseName;

    public MongoEventStore(ConnectionString connectionString, StreamConsistencyGuarantee streamConsistencyGuarantee) {
        log.info("Connecting to MongoDB using connection string: {}", connectionString);
        mongoClient = MongoClients.create(connectionString);
        databaseName = Objects.requireNonNull(connectionString.getDatabase());
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        String eventCollectionName = Objects.requireNonNull(connectionString.getCollection());
        eventCollection = mongoDatabase.getCollection(eventCollectionName);
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        initializeEventStore(eventCollectionName, streamConsistencyGuarantee, mongoDatabase);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        final EventStream<Document> eventStream;
        if (streamConsistencyGuarantee instanceof None) {
            Stream<Document> stream = readCloudEvents(streamId, skip, limit, null);
            eventStream = new EventStreamImpl<>(streamId, -1, stream);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            eventStream = readEventStream(streamId, skip, limit, transactional);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return convertToCloudEvent(cloudEventSerializer, eventStream);
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, Transactional transactional) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                Document document = mongoClient
                        .getDatabase(databaseName)
                        .getCollection(transactional.streamVersionCollectionName)
                        .find(clientSession, eq(ID, streamId), Document.class)
                        .first();

                if (document == null) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamId, skip, limit, clientSession);
                return new EventStreamImpl<>(streamId, document.getLong(VERSION), stream);
            }, transactional.transactionOptions);
        }
    }

    private Stream<Document> readCloudEvents(String streamId, int skip, int limit, ClientSession clientSession) {
        final Bson filter = eq(OccurrentCloudEventExtension.STREAM_ID, streamId);
        final FindIterable<Document> documentsWithoutSkipAndLimit;
        if (clientSession == null) {
            documentsWithoutSkipAndLimit = eventCollection.find(filter);
        } else {
            documentsWithoutSkipAndLimit = eventCollection.find(clientSession, filter);
        }

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }
        return StreamSupport.stream(documentsWithSkipAndLimit.spliterator(), false);
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        writeInternal(streamId, null, events);
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }
        writeInternal(streamId, writeCondition, events);
    }

    private void writeInternal(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (streamConsistencyGuarantee instanceof None && writeCondition != null) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        List<Document> cloudEventDocuments = convertToDocuments(cloudEventSerializer, streamId, events).collect(Collectors.toList());

        if (streamConsistencyGuarantee instanceof None) {
            eventCollection.insertMany(cloudEventDocuments);
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
                MongoCollection<Document> streamVersionCollection = mongoClient
                        .getDatabase(databaseName)
                        .getCollection(streamVersionCollectionName);

                UpdateResult updateResult = streamVersionCollection
                        .updateOne(clientSession, generateUpdateCondition(streamId, writeCondition),
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


                mongoClient
                        .getDatabase(databaseName)
                        .getCollection(eventCollection.getNamespace().getCollectionName())
                        .insertMany(clientSession, serializedEvents);
                return "";
            }, transactionOptions);
        }
    }

    private static Bson generateUpdateCondition(String streamId, WriteCondition writeCondition) {
        Bson streamEq = eq(ID, streamId);
        if (writeCondition == null) {
            return streamEq;
        }

        if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        WriteCondition.StreamVersionWriteCondition c = (WriteCondition.StreamVersionWriteCondition) writeCondition;
        return and(streamEq, generateUpdateCondition(c.condition));
    }


    private static Bson generateUpdateCondition(Condition<Long> condition) {
        if (condition instanceof MultiOperation) {
            MultiOperation<Long> operation = (MultiOperation<Long>) condition;
            MultiOperationName operationName = operation.operationName;
            List<Condition<Long>> operations = operation.operations;
            Bson[] filters = operations.stream().map(MongoEventStore::generateUpdateCondition).toArray(Bson[]::new);
            switch (operationName) {
                case AND:
                    return Filters.and(filters);
                case OR:
                    return Filters.or(filters);
                case NOT:
                    return Filters.not(filters[0]);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else if (condition instanceof Operation) {
            Operation<Long> operation = (Operation<Long>) condition;
            long expectedVersion = operation.operand;
            WriteCondition.OperationName operationName = operation.operationName;
            switch (operationName) {
                case EQ:
                    return eq(VERSION, expectedVersion);
                case LT:
                    return lt(VERSION, expectedVersion);
                case GT:
                    return gt(VERSION, expectedVersion);
                case LTE:
                    return lte(VERSION, expectedVersion);
                case GTE:
                    return gte(VERSION, expectedVersion);
                case NE:
                    return ne(VERSION, expectedVersion);
                default:
                    throw new IllegalStateException("Unexpected value: " + operationName);
            }
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
        }
    }

    @Override
    public boolean exists(String streamId) {
        return eventCollection.countDocuments(eq(OccurrentCloudEventExtension.STREAM_ID, streamId)) > 0;
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

    private static void initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, MongoDatabase mongoDatabase) {
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID));
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
}