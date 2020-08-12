package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.TerminatingChangeStream;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.api.reactor.ReactorChangeStreamer;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.applyStartPosition;

/**
 * This is a change streamer that uses project reactor and Spring to listen to changes from an event store.
 * This ChangeStreamer doesn't maintain the stream position, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc.
 */
public class SpringReactiveChangeStreamerForMongoDB implements ReactorChangeStreamer {

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

    @Override
    public Flux<CloudEventWithStreamPosition> stream(ChangeStreamFilter filter, StartAt startAt) {
        ChangeStreamWithFilterAndProjection<Document> changeStream = mongo.changeStream(Document.class).watchCollection(eventCollection);
        TerminatingChangeStream<Document> changeStreamAtStreamPosition = applyStartPosition(changeStream, cast(ChangeStreamWithFilterAndProjection::startAfter), cast(ChangeStreamWithFilterAndProjection::resumeAt), startAt);
        return changeStreamAtStreamPosition.listen()
                .flatMap(changeEvent ->
                        deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw(), timeRepresentation)
                                .map(cloudEvent -> new CloudEventWithStreamPosition(cloudEvent, new MongoDBResumeTokenBasedStreamPosition(requireNonNull(changeEvent.getResumeToken()).asDocument())))
                                .map(Mono::just)
                                .orElse(Mono.empty())
                );
    }

    private static <T1, Type, T2> BiFunction<T1, Type, T1> cast(BiFunction<T1, Type, T2> fn) {
        //noinspection unchecked
        return fn.andThen(t2 -> (T1) t2);
    }
}