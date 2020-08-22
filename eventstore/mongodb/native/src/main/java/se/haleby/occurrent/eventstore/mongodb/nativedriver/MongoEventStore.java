package se.haleby.occurrent.eventstore.mongodb.nativedriver;

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
import se.haleby.occurrent.condition.Condition;
import se.haleby.occurrent.eventstore.api.LongConditionEvaluator;
import se.haleby.occurrent.eventstore.api.WriteCondition;
import se.haleby.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreOperations;
import se.haleby.occurrent.eventstore.api.blocking.EventStoreQueries;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.api.internal.functional.FunctionalSupport.Pair;
import se.haleby.occurrent.filter.Filter;
import se.haleby.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static se.haleby.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import static se.haleby.occurrent.eventstore.api.WriteCondition.anyStreamVersion;
import static se.haleby.occurrent.eventstore.api.internal.functional.FunctionalSupport.zip;
import static se.haleby.occurrent.eventstore.mongodb.internal.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static se.haleby.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToDocument;
import static se.haleby.occurrent.filter.Filter.TIME;
import static se.haleby.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter.convertFilterToBsonFilter;

public class MongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries {
    private static final String ID = "_id";

    private final MongoCollection<Document> eventCollection;
    private final EventFormat cloudEventSerializer;
    private final MongoClient mongoClient;
    private final TimeRepresentation timeRepresentation;
    private final TransactionOptions transactionOptions;

    public MongoEventStore(MongoClient mongoClient, String databaseName, String eventCollectionName, EventStoreConfig config) {
        this(requireNonNull(mongoClient, "Mongo client cannot be null"),
                requireNonNull(mongoClient.getDatabase(databaseName), "Database must be defined"),
                mongoClient.getDatabase(databaseName).getCollection(eventCollectionName), config);
    }

    public MongoEventStore(MongoClient mongoClient, MongoDatabase database, MongoCollection<Document> eventCollection, EventStoreConfig config) {
        requireNonNull(mongoClient, "Mongo client cannot be null");
        requireNonNull(database, "Database must be defined");
        requireNonNull(eventCollection, "Event collection must be defined");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.mongoClient = mongoClient;
        this.eventCollection = eventCollection;
        transactionOptions = config.transactionOptions;
        this.timeRepresentation = config.timeRepresentation;
        initializeEventStore(eventCollection, database);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        EventStream<Document> eventStream = readEventStream(streamId, skip, limit, transactionOptions);
        return eventStream.map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, TransactionOptions transactionOptions) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                long currentStreamVersion = currentStreamVersion(streamId);
                if (currentStreamVersion == 0) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC, clientSession);
                return new EventStreamImpl<>(streamId, currentStreamVersion, stream);
            }, transactionOptions);
        }
    }

    private long currentStreamVersion(String streamId) {
        Document documentWithLatestStreamVersion = eventCollection.find(streamIdEqualTo(streamId)).sort(descending(STREAM_VERSION)).limit(1).projection(include(STREAM_VERSION)).first();
        final long currentStreamVersion;
        if (documentWithLatestStreamVersion == null) {
            currentStreamVersion = 0;
        } else {
            currentStreamVersion = documentWithLatestStreamVersion.getLong(STREAM_VERSION);
        }
        return currentStreamVersion;
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
        }

        try (ClientSession clientSession = mongoClient.startSession()) {
            clientSession.withTransaction(() -> {
                long currentStreamVersion = currentStreamVersion(streamId);

                if (!isFulfilled(currentStreamVersion, writeCondition)) {
                    throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion));
                }

                List<Document> cloudEventDocuments = zip(LongStream.iterate(currentStreamVersion + 1, i -> i + 1).boxed(), events, Pair::new)
                        .map(pair -> convertToDocument(cloudEventSerializer, timeRepresentation, streamId, pair.t1, pair.t2))
                        .collect(Collectors.toList());

                try {
                    eventCollection.insertMany(clientSession, cloudEventDocuments);
                } catch (MongoBulkWriteException e) {
                    throw translateToDuplicateCloudEventException(e);
                }
                return "";
            }, transactionOptions);
        }
    }

    private static boolean isFulfilled(long currentStreamVersion, WriteCondition writeCondition) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        Condition<Long> condition = ((StreamVersionWriteCondition) writeCondition).condition;
        return LongConditionEvaluator.evaluate(condition, currentStreamVersion);
    }

    @Override
    public boolean exists(String streamId) {
        return eventCollection.countDocuments(eq(STREAM_ID, streamId)) > 0;
    }

    @Override
    public void deleteEventStream(String streamId) {
        eventCollection.deleteMany(eq(STREAM_ID, streamId));
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
        try (ClientSession clientSession = mongoClient.startSession()) {
            result = clientSession.withTransaction(
                    () -> updateCloudEvent(updateFunction, () -> eventCollection.find(clientSession, cloudEvent), updatedDocument -> eventCollection.replaceOne(clientSession, cloudEvent, updatedDocument)),
                    transactionOptions);
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
                long streamVersion = (long) currentCloudEvent.getExtension(STREAM_VERSION);
                Document updatedDocument = convertToDocument(cloudEventSerializer, timeRepresentation, streamId, streamVersion, updatedCloudEvent);
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

    private static void initializeEventStore(MongoCollection<Document> eventStoreCollection, MongoDatabase mongoDatabase) {
        String eventStoreCollectionName = eventStoreCollection.getNamespace().getCollectionName();
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        // Create a streamId index
        eventStoreCollection.createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        // Create a streamId + streamVersion index
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.descending(STREAM_VERSION)), new IndexOptions().unique(true));
    }

    private static boolean collectionExists(MongoDatabase mongoDatabase, String collectionName) {
        for (String listCollectionName : mongoDatabase.listCollectionNames()) {
            if (listCollectionName.equals(collectionName)) {
                return true;
            }
        }
        return false;
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