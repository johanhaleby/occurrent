package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import se.haleby.occurrent.changestreamer.CloudEventWithStreamPosition;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.MongoDBFilterSpecification.JsonMongoDBFilterSpecification;
import se.haleby.occurrent.changestreamer.mongodb.internal.DocumentAdapter;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static se.haleby.occurrent.changestreamer.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.deserializeToCloudEvent;

public class BlockingChangeStreamerForMongoDB {

    private final MongoCollection<Document> eventCollection;
    private final ConcurrentMap<String, Subscription> subscriptions;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final Executor cloudEventDispatcher;

    /**
     * Create a change streamer using the native MongoDB sync driver.
     *
     * @param eventCollection      The collection that contains the events
     * @param timeRepresentation   How time is represented in the databased
     * @param subscriptionExecutor The executor that will be used for the subscription. Typically a dedicated thread will be required per subscription.
     */
    public BlockingChangeStreamerForMongoDB(MongoCollection<Document> eventCollection, TimeRepresentation timeRepresentation, Executor subscriptionExecutor) {
        requireNonNull(eventCollection, "Event collection cannot be null");
        requireNonNull(timeRepresentation, "Time representation cannot be null");
        requireNonNull(subscriptionExecutor, "CloudEventDispatcher be null");
        this.cloudEventDispatcher = subscriptionExecutor;
        this.timeRepresentation = timeRepresentation;
        this.eventCollection = eventCollection;
        this.subscriptions = new ConcurrentHashMap<>();
        this.cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    }

    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action) {
        return stream(subscriptionId, action, null);
    }

    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action, MongoDBFilterSpecification filter) {
        return stream(subscriptionId, action, filter, Function.identity());
    }

    public Subscription stream(String subscriptionId, Consumer<CloudEventWithStreamPosition<BsonDocument>> action, MongoDBFilterSpecification filter,
                               Function<ChangeStreamIterable<Document>, ChangeStreamIterable<Document>> changeStreamConfigurer) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(changeStreamConfigurer, "Change stream configurer cannot be null");

        List<Bson> pipeline = createPipeline(filter);
        ChangeStreamIterable<Document> changeStreamDocuments = eventCollection.watch(pipeline, Document.class);
        ChangeStreamIterable<Document> changeStreamDocumentsAfterConfiguration = changeStreamConfigurer.apply(changeStreamDocuments);
        if (changeStreamDocumentsAfterConfiguration == null) {
            throw new IllegalArgumentException("Change stream configurer is not allowed to return null");
        }
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStreamDocumentsAfterConfiguration.cursor();

        Subscription subscription = new Subscription(subscriptionId, cursor);
        subscriptions.put(subscriptionId, subscription);

        cloudEventDispatcher.execute(() -> cursor.forEachRemaining(changeStreamDocument -> deserializeToCloudEvent(cloudEventSerializer, changeStreamDocument, timeRepresentation)
                .map(cloudEvent -> new CloudEventWithStreamPosition<>(cloudEvent, changeStreamDocument.getResumeToken()))
                .ifPresent(action)));

        return subscription;
    }

    private static List<Bson> createPipeline(MongoDBFilterSpecification filter) {
        final List<Bson> pipeline;
        if (filter == null) {
            pipeline = Collections.emptyList();
        } else if (filter instanceof JsonMongoDBFilterSpecification) {
            pipeline = Collections.singletonList(Document.parse(((JsonMongoDBFilterSpecification) filter).getJson()));
        } else if (filter instanceof BsonMongoDBFilterSpecification) {
            Bson[] aggregationStages = ((BsonMongoDBFilterSpecification) filter).getAggregationStages();
            DocumentAdapter documentAdapter = new DocumentAdapter(MongoClientSettings.getDefaultCodecRegistry());
            pipeline = Stream.of(aggregationStages).map(aggregationStage -> {
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
            }).collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException("Invalid " + MongoDBFilterSpecification.class.getSimpleName());
        }
        return pipeline;
    }

    public void cancelSubscription(String subscriptionId) {
        Subscription subscription = subscriptions.remove(subscriptionId);
        if (subscription != null) {
            subscription.cursor.close();
        }
    }

    @PreDestroy
    public void shutdown() {
        synchronized (subscriptions) {
            subscriptions.keySet().forEach(this::cancelSubscription);
        }
    }
}