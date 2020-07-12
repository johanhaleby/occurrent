package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class MongoEventStore implements EventStore {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private static final String STREAM_ID = "streamId";
    private static final String CLOUD_EVENT = "cloudEvent";
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
            Stream<Document> stream = readCloudEvents(streamId, skip, limit);
            eventStream = new EventStreamImpl<>(streamId, 0, stream);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            eventStream = readEventStream(streamId, skip, limit, transactional);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return requireNonNull(eventStream).map(Document::toJson).map(eventJsonString -> eventJsonString.getBytes(UTF_8)).map(cloudEventSerializer::deserialize);
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, Transactional transactional) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                Document document = mongoClient
                        .getDatabase(databaseName)
                        .getCollection(transactional.streamVersionCollectionName)
                        .find(eq(ID, streamId), Document.class)
                        .first();

                if (document == null) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamId, skip, limit);
                return new EventStreamImpl<>(ID, document.getLong(VERSION), stream);
            }, transactional.transactionOptions);
        }
    }


    private Stream<Document> readCloudEvents(String streamId, int skip, int limit) {
        FindIterable<Document> documents = eventCollection.find(eq(STREAM_ID, streamId));
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documents = documents.skip(skip).limit(limit);
        }
        MongoIterable<Document> iterable = documents.map(document -> document.get(CLOUD_EVENT, Document.class));
        return StreamSupport.stream(iterable.spliterator(), false);
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
            eventCollection.insertMany(serializedEvents);
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            consistentlyWrite(streamId, expectedStreamVersion, serializedEvents);

        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
    }

    private void consistentlyWrite(String streamId, long expectedStreamVersion, List<Document> serializedEvents) {
        Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
        TransactionOptions transactionOptions = transactional.transactionOptions;
        String streamVersionCollectionName = transactional.streamVersionCollectionName;
        try (ClientSession clientSession = mongoClient.startSession()) {
            clientSession.withTransaction(() -> {
                mongoClient
                        .getDatabase(databaseName)
                        .getCollection(streamVersionCollectionName)
                        .updateOne(and(eq(ID, streamId), eq(VERSION, expectedStreamVersion)),
                                set("version", expectedStreamVersion + 1),
                                new UpdateOptions().upsert(true));

                mongoClient
                        .getDatabase(databaseName)
                        .getCollection(eventCollection.getNamespace().getCollectionName())
                        .insertMany(serializedEvents);
                return expectedStreamVersion + 1;
            }, transactionOptions);
        }
    }

    @Override
    public boolean exists(String streamId) {
        return eventCollection.countDocuments(eq("_id", streamId)) > 0;
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
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        mongoDatabase.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(CLOUD_EVENT + ".id"), Indexes.ascending(CLOUD_EVENT + ".source")), new IndexOptions().unique(true));
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