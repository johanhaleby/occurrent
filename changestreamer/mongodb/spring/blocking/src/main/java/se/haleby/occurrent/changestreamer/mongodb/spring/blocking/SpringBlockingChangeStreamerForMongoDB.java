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
import se.haleby.occurrent.changestreamer.mongodb.common.DocumentAdapter;
import se.haleby.occurrent.changestreamer.mongodb.common.MongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.common.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.common.MongoDBFilterSpecification.DocumentMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.common.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.common.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;

public class SpringBlockingChangeStreamerForMongoDB {

    private final String eventCollection;
    private final MessageListenerContainer messageListenerContainer;
    private final ConcurrentMap<String, Subscription> subscriptions;
    private final EventFormat cloudEventSerializer;

    public SpringBlockingChangeStreamerForMongoDB(String eventCollection, MessageListenerContainer messageListenerContainer) {
        requireNonNull(eventCollection, "eventCollection cannot be null");
        requireNonNull(messageListenerContainer, "messageListenerContainer cannot be null");

        this.eventCollection = eventCollection;
        this.subscriptions = new ConcurrentHashMap<>();
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        this.messageListenerContainer = messageListenerContainer;
        this.messageListenerContainer.start();
    }

    public Subscription subscribe(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action) {
        return subscribe(subscriptionId, action, null);
    }

    public Subscription subscribe(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action, MongoDBFilterSpecification filter) {
        return subscribe(subscriptionId, action, filter, ChangeStreamOptions.builder());
    }

    public Subscription subscribe(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action, MongoDBFilterSpecification filter, ChangeStreamOptionsBuilder changeStreamOptionsBuilder) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(changeStreamOptionsBuilder, "ChangeStreamOptionsBuilder cannot be null");
        final ChangeStreamOptions changeStreamOptions = applyFilter(filter, changeStreamOptionsBuilder);
        MessageListener<ChangeStreamDocument<Document>, Document> listener = change -> {
            ChangeStreamDocument<Document> raw = change.getRaw();
            BsonDocument resumeToken = requireNonNull(raw).getResumeToken();
            deserializeToCloudEvent(requireNonNull(cloudEventSerializer), raw)
                    .map(cloudEvent -> new CloudEventWithStreamPosition<>(cloudEvent, resumeToken))
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
        } else if (filter instanceof DocumentMongoDBFilterSpecification) {
            Document[] documents = ((DocumentMongoDBFilterSpecification) filter).getDocuments();
            Document[] aggregations = Stream.of(documents).map(d -> new Document("$match", d)).toArray(Document[]::new);
            changeStreamOptions = changeStreamOptionsBuilder.filter(aggregations).build();
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