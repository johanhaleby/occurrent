package se.haleby.occurrent.changestreamer.mongodb.spring.reactor;

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.CloudEventWithChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.api.reactor.PositionAwareReactorChangeStreamer;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.applyStartPosition;
import static se.haleby.occurrent.changestreamer.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder.applyFilter;

/**
 * This is a change streamer that uses project reactor and Spring to listen to changes from an event store.
 * This ChangeStreamer doesn't maintain the stream position, you need to store it yourself
 * (or use another pre-existing component in conjunction with this one) in order to continue the stream from where
 * it's left off on application restart/crash etc.
 */
public class SpringReactorChangeStreamerForMongoDB implements PositionAwareReactorChangeStreamer {

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
    public SpringReactorChangeStreamerForMongoDB(ReactiveMongoOperations mongo, String eventCollection, TimeRepresentation timeRepresentation) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.timeRepresentation = timeRepresentation;
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    @Override
    public Flux<CloudEventWithChangeStreamPosition> stream(ChangeStreamFilter filter, StartAt startAt) {
        // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
        ChangeStreamOptionsBuilder builder = applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, startAt);
        final ChangeStreamOptions changeStreamOptions = applyFilter(filter, builder);
        Flux<ChangeStreamEvent<Document>> changeStream = mongo.changeStream(eventCollection, changeStreamOptions, Document.class);
        return changeStream
                .flatMap(changeEvent ->
                        deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw(), timeRepresentation)
                                .map(cloudEvent -> new CloudEventWithChangeStreamPosition(cloudEvent, new MongoDBResumeTokenBasedChangeStreamPosition(requireNonNull(changeEvent.getResumeToken()).asDocument())))
                                .map(Mono::just)
                                .orElse(Mono.empty()));
    }

    @Override
    public Mono<ChangeStreamPosition> globalChangeStreamPosition() {
        return mongo.executeCommand(new Document("hostInfo", 1))
                .map(MongoDBCommons::getServerOperationTime)
                .map(MongoDBOperationTimeBasedChangeStreamPosition::new);
    }


}