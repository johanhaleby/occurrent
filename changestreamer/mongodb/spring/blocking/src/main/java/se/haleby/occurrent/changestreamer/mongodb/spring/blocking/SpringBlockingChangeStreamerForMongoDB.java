package se.haleby.occurrent.changestreamer.mongodb.spring.blocking;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import se.haleby.occurrent.changestreamer.ChangeStreamFilter;
import se.haleby.occurrent.changestreamer.ChangeStreamPosition;
import se.haleby.occurrent.changestreamer.CloudEventWithChangeStreamPosition;
import se.haleby.occurrent.changestreamer.StartAt;
import se.haleby.occurrent.changestreamer.api.blocking.PositionAwareBlockingChangeStreamer;
import se.haleby.occurrent.changestreamer.api.blocking.Subscription;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBOperationTimeBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedChangeStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.DocumentAdapter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.applyStartPosition;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCommons.getServerOperationTime;

/**
 * This is a change streamer that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This ChangeStreamer doesn't maintain the stream position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this change streamer doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringBlockingChangeStreamerForMongoDB implements PositionAwareBlockingChangeStreamer {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, org.springframework.data.mongodb.core.messaging.Subscription> subscriptions;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;

    /**
     * Create a blocking change streamer using Spring
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringBlockingChangeStreamerForMongoDB(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation) {
        requireNonNull(mongoTemplate, MongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");

        this.mongoOperations = mongoTemplate;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.subscriptions = new ConcurrentHashMap<>();
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.messageListenerContainer = new DefaultMessageListenerContainer(mongoTemplate);
        this.messageListenerContainer.start();
    }

    @Override
    public Subscription stream(String subscriptionId, ChangeStreamFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEventWithChangeStreamPosition> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAtSupplier, "StartAt cannot be null");

        // TODO We should change builder::resumeAt to builder::startAtOperationTime once Spring adds support for it (see https://jira.spring.io/browse/DATAMONGO-2607)
        ChangeStreamOptionsBuilder builder = applyStartPosition(ChangeStreamOptions.builder(), ChangeStreamOptionsBuilder::startAfter, ChangeStreamOptionsBuilder::resumeAt, startAtSupplier.get());
        final ChangeStreamOptions changeStreamOptions = applyFilter(filter, builder);

        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            deserializeToCloudEvent(requireNonNull(cloudEventSerializer), raw, timeRepresentation)
                    .map(cloudEvent -> new CloudEventWithChangeStreamPosition(cloudEvent, new MongoDBResumeTokenBasedChangeStreamPosition(resumeToken)))
                    .ifPresent(action);
        };

        ChangeStreamRequestOptions options = new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = messageListenerContainer.register(new ChangeStreamRequest<>(listener, options), Document.class);
        subscriptions.put(subscriptionId, subscription);
        return new MongoDBSpringSubscription(subscriptionId, subscription);
    }

    private static ChangeStreamOptions applyFilter(ChangeStreamFilter filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
        final ChangeStreamOptions changeStreamOptions;
        if (filter == null) {
            changeStreamOptions = changeStreamOptionsBuilder.build();
        } else if (filter instanceof JsonMongoDBFilterSpecification) {
            changeStreamOptions = changeStreamOptionsBuilder.filter(Document.parse(((JsonMongoDBFilterSpecification) filter).getJson())).build();
        } else if (filter instanceof BsonMongoDBFilterSpecification) {
            Bson[] aggregationStages = ((BsonMongoDBFilterSpecification) filter).getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            Document[] documents = Stream.of(aggregationStages).map(aggregationStage -> {
                final Document result;
                if (aggregationStage instanceof Document) {
                    result = (Document) aggregationStage;
                } else if (aggregationStage instanceof BsonDocument) {
                    result = documentAdapter.fromBson((BsonDocument) aggregationStage);
                } else {
                    BsonDocument bsonDocument = aggregationStage.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry());
                    result = documentAdapter.fromBson(bsonDocument);
                }
                return result;
            }).toArray(Document[]::new);

            changeStreamOptions = changeStreamOptionsBuilder.filter(documents).build();
        } else {
            throw new IllegalArgumentException("Unrecognized " + ChangeStreamFilter.class.getSimpleName() + " for MongoDB change streamer");
        }
        return changeStreamOptions;
    }

    public void cancelSubscription(String subscriptionId) {
        org.springframework.data.mongodb.core.messaging.Subscription subscription = subscriptions.remove(subscriptionId);
        if (subscription != null) {
            messageListenerContainer.remove(subscription);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        subscriptions.clear();
        messageListenerContainer.stop();
    }

    @Override
    public ChangeStreamPosition globalChangeStreamPosition() {
        BsonTimestamp currentOperationTime = getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)));
        return new MongoDBOperationTimeBasedChangeStreamPosition(currentOperationTime);
    }
}