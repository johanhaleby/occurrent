package se.haleby.occurrent.changestreamer.mongodb.spring.blocking;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Subscription;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBResumeTokenBasedStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.internal.DocumentAdapter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;

/**
 * This is a change streamer that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This ChangeStreamer doesn't maintain the stream position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this change streamer doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringBlockingChangeStreamerForMongoDB {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, Subscription> subscriptions;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;

    /**
     * Create a blocking change streamer using Spring
     *
     * @param eventCollection          The collection that contains the events
     * @param messageListenerContainer The message listener container that'll be used when subscribing to events from the event store. Typically you would instantiate it using <code>new DefaultMessageListenerContainer(mongoTemplate)</code>.
     * @param timeRepresentation       How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringBlockingChangeStreamerForMongoDB(String eventCollection, MessageListenerContainer messageListenerContainer, TimeRepresentation timeRepresentation) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(messageListenerContainer, "messageListenerContainer cannot be null");
        requireNonNull(timeRepresentation, TimeRepresentation.class.getSimpleName() + " cannot be null");

        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.subscriptions = new ConcurrentHashMap<>();
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.messageListenerContainer = messageListenerContainer;
        this.messageListenerContainer.start();
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition> action) {
        return stream(subscriptionId, action, null);
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId The id of the subscription, must be unique!
     * @param action         This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @param filter         The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition> action, MongoDBFilterSpecification filter) {
        return stream(subscriptionId, action, filter, ChangeStreamOptions.builder());
    }

    /**
     * Start listening to cloud events persisted to the event store.
     *
     * @param subscriptionId             The id of the subscription, must be unique!
     * @param action                     This action will be invoked for each cloud event that is stored in the EventStore that matches the supplied <code>filter</code>.
     * @param filter                     The filter to apply for this subscription. Only events matching the filter will cause the <code>action</code> to be called.
     * @param changeStreamOptionsBuilder Pass in an {@link ChangeStreamOptionsBuilder} that will be used when subscribing. This is useful for example if you have persisted the current stream position and need to resume from this position on application restart.
     */
    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition> action, MongoDBFilterSpecification filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(changeStreamOptionsBuilder, "ChangeStreamOptionsBuilder cannot be null");
        final ChangeStreamOptions changeStreamOptions = applyFilter(filter, changeStreamOptionsBuilder);
        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            deserializeToCloudEvent(requireNonNull(cloudEventSerializer), raw, timeRepresentation)
                    .map(cloudEvent -> new CloudEventWithStreamPosition(cloudEvent, new MongoDBResumeTokenBasedStreamPosition(resumeToken)))
                    .ifPresent(action);
        };

        ChangeStreamRequestOptions options = new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        final Subscription subscription = messageListenerContainer.register(new ChangeStreamRequest<>(listener, options), Document.class);
        subscriptions.put(subscriptionId, subscription);
        return subscription;
    }

    private static ChangeStreamOptions applyFilter(MongoDBFilterSpecification filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
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
            throw new IllegalArgumentException("Invalid " + MongoDBFilterSpecification.class.getSimpleName());
        }
        return changeStreamOptions;
    }

    public void cancelSubscription(String subscriptionId) {
        Subscription subscription = subscriptions.remove(subscriptionId);
        if (subscription != null) {
            messageListenerContainer.remove(subscription);
        }
    }

    @PreDestroy
    void shutdownSubscribers() {
        subscriptions.clear();
        messageListenerContainer.stop();
    }
}