package se.haleby.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;

import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

public class SpringBlockingMongoEventStore implements EventStore {

    private final MongoOperations mongoOperations;
    private final String eventStoreCollectionName;
    private final EventFormat cloudEventSerializer;

    public SpringBlockingMongoEventStore(MongoOperations mongoOperations, String eventStoreCollectionName) {
        this.mongoOperations = mongoOperations;
        this.eventStoreCollectionName = eventStoreCollectionName;
        mongoOperations.getCollection(eventStoreCollectionName).createIndex(Indexes.compoundIndex(Indexes.ascending("_id"), Indexes.ascending("version")));
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        Aggregation aggregation = newAggregation(
                match(where("_id").is(streamId)),
                project("_id", "version").and("events").slice(limit, skip)
        );

        return mongoOperations.aggregate(aggregation, eventStoreCollectionName, EventStreamImpl.class).getMappedResults().stream()
                .findFirst()
                .map(document -> document.map(eventJsonString -> eventJsonString.getBytes(UTF_8)).map(cloudEventSerializer::deserialize))
                .orElse(new EmptyEventStreamImpl(streamId));
    }

    @Override
    public void write(String streamId, long expectedStreamVersion, Stream<CloudEvent> events) {
        String[] serializedEvents = events.map(cloudEventSerializer::serialize).map(bytes -> new String(bytes, UTF_8)).toArray(String[]::new);
        //noinspection ConfusingArgumentToVarargsMethod
        mongoOperations.upsert(query(where("_id").is(streamId).and("version").is(expectedStreamVersion)),
                update("version", expectedStreamVersion + 1).push("events").each(serializedEvents),
                eventStoreCollectionName);
    }

    private static class EmptyEventStreamImpl implements EventStream<CloudEvent> {
        private final String id;

        public EmptyEventStreamImpl(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public long version() {
            return 0;
        }

        @Override
        public Stream<CloudEvent> events() {
            return Stream.empty();
        }
    }

    private static class EventStreamImpl implements EventStream<String> {
        private String _id;
        private long version;
        private List<String> events;

        @SuppressWarnings("unused")
        EventStreamImpl() {
        }

        EventStreamImpl(String _id, long version, List<String> events) {
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
        public Stream<String> events() {
            return events.stream();
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        public void setEvents(List<String> events) {
            this.events = events;
        }
    }
}