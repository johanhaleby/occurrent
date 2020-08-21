package se.haleby.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.subscription.CloudEventWithSubscriptionPosition;
import se.haleby.occurrent.subscription.StartAt;
import se.haleby.occurrent.subscription.SubscriptionFilter;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.blocking.PositionAwareBlockingSubscription;
import se.haleby.occurrent.subscription.api.blocking.Subscription;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.applyStartPosition;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.getServerOperationTime;
import static se.haleby.occurrent.subscription.mongodb.spring.internal.ApplyFilterToChangeStreamOptionsBuilder.applyFilter;

/**
 * This is a subscription that uses Spring and its {@link MessageListenerContainer} for MongoDB to listen to changes from an event store.
 * This Subscription doesn't maintain the subscription position, you need to store it yourself in order to continue the stream
 * from where it's left off on application restart/crash etc.
 * <p>
 * Note that this subscription doesn't provide retries if an exception is thrown when handling a {@link io.cloudevents.CloudEvent} (<code>action</code>).
 * This reason for this is that Spring provides retry capabilities (such as spring-retry) that you can easily hook into your <code>action</code>.
 */
public class SpringBlockingSubscriptionForMongoDB implements PositionAwareBlockingSubscription {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, org.springframework.data.mongodb.core.messaging.Subscription> subscriptions;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final MongoOperations mongoOperations;

    /**
     * Create a blocking subscription using Spring
     *
     * @param mongoTemplate      The mongo template to use
     * @param eventCollection    The collection that contains the events
     * @param timeRepresentation How time is represented in the database, must be the same as what's specified for the EventStore that stores the events.
     */
    public SpringBlockingSubscriptionForMongoDB(MongoTemplate mongoTemplate, String eventCollection, TimeRepresentation timeRepresentation) {
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
    public Subscription subscribe(String subscriptionId, SubscriptionFilter filter, Supplier<StartAt> startAtSupplier, Consumer<CloudEventWithSubscriptionPosition> action) {
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
                    .map(cloudEvent -> new CloudEventWithSubscriptionPosition(cloudEvent, new MongoDBResumeTokenBasedSubscriptionPosition(resumeToken)))
                    .ifPresent(action);
        };

        ChangeStreamRequestOptions options = new ChangeStreamRequestOptions(null, eventCollection, changeStreamOptions);
        final org.springframework.data.mongodb.core.messaging.Subscription subscription = messageListenerContainer.register(new ChangeStreamRequest<>(listener, options), Document.class);
        subscriptions.put(subscriptionId, subscription);
        return new MongoDBSpringSubscription(subscriptionId, subscription);
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
    public SubscriptionPosition globalSubscriptionPosition() {
        BsonTimestamp currentOperationTime = getServerOperationTime(mongoOperations.executeCommand(new Document("hostInfo", 1)));
        return new MongoDBOperationTimeBasedSubscriptionPosition(currentOperationTime);
    }
}