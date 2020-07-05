package se.haleby.occurrent;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class MongoEventStore implements EventStore {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private final MongoCollection<Document> eventCollection;

    public MongoEventStore(ConnectionString connectionString) {
        log.info("Connecting to MongoDB using connection string: {}", connectionString);
        MongoClient mongoClient = MongoClients.create(connectionString);
        eventCollection = mongoClient.getDatabase(Objects.requireNonNull(connectionString.getDatabase()))
                .getCollection(Objects.requireNonNull(connectionString.getCollection()));
        eventCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("_id"), Indexes.ascending("version")));
    }

    @Override
    public <T> EventStream<T> read(String streamId, Class<T> t) {
        Document document = eventCollection.find(eq("_id", streamId)).first();
        if (document == null) {
            return null;
        }
        long version = document.getLong("version");
        List<String> serializedCloudEvents = document.getList("events", String.class, Collections.emptyList());
        List<CloudEventImpl<T>> events = serializedCloudEvents.stream()
                .map(serializedCloudEvent -> Json.<CloudEventImpl<T>>decodeValue(serializedCloudEvent, CloudEventImpl.class, t))
                .collect(Collectors.toList());
        return new EventStreamImpl<>(streamId, version, events);
    }

    @Override
    public <T> void write(String streamId, long expectedStreamVersion, Stream<CloudEventImpl<T>> events) {
        List<String> serializedEvents = events.map(Json::encode).collect(Collectors.toList());
        // Note that upsert will fail if version is different since _id is unique in MongoDB
        eventCollection.updateOne(and(eq("_id", streamId), eq("version", expectedStreamVersion)),
                combine(pushEach("events", serializedEvents), set("version", expectedStreamVersion + 1)),
                new UpdateOptions().upsert(true));
    }

    private static class EventStreamImpl<T> implements EventStream<T> {
        private final String id;
        private final long version;
        private final List<CloudEventImpl<T>> events;

        EventStreamImpl(String id, long version, List<CloudEventImpl<T>> events) {
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
        public Stream<CloudEventImpl<T>> events() {
            return events.stream();
        }
    }
}