package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBStreamPosition;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.util.function.Function;

import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;

/**
 * This is a change streamer that uses project reactor and Spring to listen to changes from an event store.
 * This ChangeStreamer doesn't maintain the stream position, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc.
 */
public class SpringReactiveChangeStreamerForMongoDB {

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final TimeRepresentation timeRepresentation;
    private final EventFormat cloudEventSerializer;

    /**
     * Create a blocking change streamer using Spring
     *
     * @param mongo              The {@link ReactiveMongoOperations} instance to use when reading events from the event store
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringReactiveChangeStreamerForMongoDB(ReactiveMongoOperations mongo, String eventCollection, TimeRepresentation timeRepresentation) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    /**
     * Stream events from the event store as they arrive.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    public Flux<CloudEventWithStreamPosition<MongoDBStreamPosition>> stream() {
        return stream(ChangeStreamWithFilterAndProjection::listen);
    }

    /**
     * Stream events from the event store as they arrive and provide a function which allows to configure the
     * {@link CloudEventWithStreamPosition} that is used. Use this method if want to start streaming from a specific
     * position.
     *
     * @return A {@link Flux} with cloud events which also includes the {@link se.haleby.occurrent.changestreamer.StreamPosition} that can be used to resume the stream from the current position.
     */
    public Flux<CloudEventWithStreamPosition<MongoDBStreamPosition>> stream(Function<ChangeStreamWithFilterAndProjection<Document>, Flux<ChangeStreamEvent<Document>>> fn) {
        ChangeStreamWithFilterAndProjection<Document> changeStream = mongo.changeStream(Document.class).watchCollection(eventCollection);
        return fn.apply(changeStream)
                .flatMap(changeEvent ->
                        deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw(), timeRepresentation)
                                .map(cloudEvent -> new CloudEventWithStreamPosition<>(cloudEvent, new MongoDBStreamPosition(changeEvent.getBsonTimestamp(), changeEvent.getResumeToken())))
                                .map(Mono::just)
                                .orElse(Mono.empty())
                );
    }
}