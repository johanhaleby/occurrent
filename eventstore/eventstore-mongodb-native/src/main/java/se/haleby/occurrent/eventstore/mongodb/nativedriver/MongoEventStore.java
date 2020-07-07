package se.haleby.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.haleby.occurrent.EventStore;
import se.haleby.occurrent.EventStream;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.slice;
import static com.mongodb.client.model.Updates.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MongoEventStore implements EventStore {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);

    private final MongoCollection<Document> eventCollection;
    private final EventFormat cloudEventSerializer;

    public MongoEventStore(ConnectionString connectionString) {
        log.info("Connecting to MongoDB using connection string: {}", connectionString);
        MongoClient mongoClient = MongoClients.create(connectionString);
        eventCollection = mongoClient.getDatabase(Objects.requireNonNull(connectionString.getDatabase()))
                .getCollection(Objects.requireNonNull(connectionString.getCollection()));
        eventCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("_id"), Indexes.ascending("version")));
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        Document document = eventCollection.find(eq("_id", streamId)).projection(slice("events", skip, limit)).first();
        if (document == null) {
            return null;
        }
        long version = document.getLong("version");
        List<String> serializedCloudEvents = document.getList("events", String.class, Collections.emptyList());
        List<CloudEvent> events = serializedCloudEvents.stream()
                .map(cloudEventAsString -> cloudEventAsString.getBytes(UTF_8))
                .map(cloudEventSerializer::deserialize)
                .collect(Collectors.toList());
        return new EventStreamImpl(streamId, version, events);
    }

    @Override
    public void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        List<String> serializedEvents = events.map(cloudEventSerializer::serialize).map(bytes -> new String(bytes, UTF_8)).collect(Collectors.toList());

        // Note that upsert will fail if version is different since _id is unique in MongoDB
        eventCollection.updateOne(and(eq("_id", streamId), eq("version", expectedStreamVersion)),
                combine(pushEach("events", serializedEvents), set("version", expectedStreamVersion + 1)),
                new UpdateOptions().upsert(true));
    }

    private static class EventStreamImpl implements EventStream<CloudEvent> {
        private final String id;
        private final long version;
        private final List<CloudEvent> events;

        EventStreamImpl(String id, long version, List<CloudEvent> events) {
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
        public Stream<CloudEvent> events() {
            return events.stream();
        }
    }
}