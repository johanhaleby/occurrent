package se.haleby.occurrent.changestreamer.mongodb.spring.reactive;

import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamEvent;
import org.springframework.data.mongodb.core.ReactiveChangeStreamOperation.ChangeStreamWithFilterAndProjection;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;

import java.util.function.Function;

import static se.haleby.occurrent.changestreamer.mongodb.common.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;

public class SpringReactiveChangeStreamerForMongoDB {

    private final ReactiveMongoOperations mongo;
    private final String eventCollection;
    private final EventFormat cloudEventSerializer;

    public SpringReactiveChangeStreamerForMongoDB(ReactiveMongoOperations mongo, String eventCollection) {
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    public Flux<CloudEventWithStreamPosition<BsonValue>> stream() {
        return stream(ChangeStreamWithFilterAndProjection::listen);
    }

    public Flux<CloudEventWithStreamPosition<BsonValue>> stream(Function<ChangeStreamWithFilterAndProjection<Document>, Flux<ChangeStreamEvent<Document>>> fn) {
        ChangeStreamWithFilterAndProjection<Document> changeStream = mongo.changeStream(Document.class).watchCollection(eventCollection);
        return fn.apply(changeStream)
                .flatMap(changeEvent ->
                        deserializeToCloudEvent(cloudEventSerializer, changeEvent.getRaw())
                                .map(cloudEvent -> new CloudEventWithStreamPosition<>(cloudEvent, changeEvent.getResumeToken()))
                                .map(Mono::just)
                                .orElse(Mono.empty())
                );
    }
}