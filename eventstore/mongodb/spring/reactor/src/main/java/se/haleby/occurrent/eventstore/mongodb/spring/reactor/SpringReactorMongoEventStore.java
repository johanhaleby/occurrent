package se.haleby.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension;
import se.haleby.occurrent.eventstore.api.common.WriteCondition;
import se.haleby.occurrent.eventstore.api.common.WriteCondition.StreamVersionWriteCondition;
import se.haleby.occurrent.eventstore.api.common.WriteConditionNotFulfilledException;
import se.haleby.occurrent.eventstore.api.reactor.EventStore;
import se.haleby.occurrent.eventstore.api.reactor.EventStream;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.StreamConsistencyGuarantee.None;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.StreamConsistencyGuarantee.Transactional;
import se.haleby.occurrent.eventstore.mongodb.spring.reactor.StreamConsistencyGuarantee.TransactionalAnnotation;

import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static se.haleby.occurrent.eventstore.mongodb.spring.common.internal.ConditionToCriteriaConverter.convertConditionToCriteria;

public class SpringReactorMongoEventStore implements EventStore {

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final ReactiveMongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final StreamConsistencyGuarantee streamConsistencyGuarantee;
    private final EventFormat cloudEventSerializer;

    public SpringReactorMongoEventStore(ReactiveMongoTemplate mongoTemplate, String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee) {
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = eventStoreCollectionName;
        this.streamConsistencyGuarantee = streamConsistencyGuarantee;
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        initializeEventStore(eventStoreCollectionName, streamConsistencyGuarantee, mongoTemplate).block();
    }

    @Override
    public Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }
        return writeInternal(streamId, writeCondition, events);
    }

    @Override
    public Mono<Boolean> exists(String streamId) {
        return mongoTemplate.exists(query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);
    }

    @Override
    public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
        return null;
    }

    @Override
    public Mono<Void> write(String streamId, Flux<CloudEvent> events) {
        return writeInternal(streamId, null, events);
    }

    public Mono<Void> writeInternal(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
        if (streamConsistencyGuarantee instanceof None && writeCondition != null) {
            throw new IllegalArgumentException("Cannot use a " + WriteCondition.class.getSimpleName() + " when streamConsistencyGuarantee is " + None.class.getSimpleName());
        }

        Flux<Document> serializedEvents = convertToDocuments(cloudEventSerializer, streamId, events);

        Mono<Void> result;
        if (streamConsistencyGuarantee instanceof None) {
            result = insertAll(serializedEvents).then();
        } else if (streamConsistencyGuarantee instanceof Transactional) {
            Transactional transactional = (Transactional) this.streamConsistencyGuarantee;
            String streamVersionCollectionName = transactional.streamVersionCollectionName;
            result = transactional.transactionalOperator.execute(transactionStatus -> conditionallyWriteEvents(streamId, streamVersionCollectionName, writeCondition, serializedEvents)).then();
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            String streamVersionCollectionName = ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName;
            result = conditionallyWriteEvents(streamId, streamVersionCollectionName, writeCondition, serializedEvents);
        } else {
            throw new IllegalStateException("Internal error, invalid stream write consistency guarantee");
        }
        return result;
    }

    // Write
    private Mono<Void> conditionallyWriteEvents(String streamId, String streamVersionCollectionName, WriteCondition writeCondition, Flux<Document> serializedEvents) {
        return increaseStreamVersion(streamId, writeCondition, streamVersionCollectionName)
                .thenMany(insertAll(serializedEvents))
                .then();
    }

    // TODO If inserts fail due to duplicate cloud event, remove this event from the documents list
    //  (and all events before this document since they have been successfully inserted) and retry!
    private Flux<Document> insertAll(Flux<Document> documents) {
        return mongoTemplate.insertAll(documents.collectList(), eventStoreCollectionName);
    }

    private Mono<Void> increaseStreamVersion(String streamId, WriteCondition writeCondition, String streamVersionCollectionName) {
        return mongoTemplate.updateFirst(query(generateUpdateCondition(streamId, writeCondition)),
                new Update().inc(VERSION, 1L), streamVersionCollectionName)
                .flatMap(updateResult -> {
                    final Mono<Void> mono;
                    if (updateResult.getMatchedCount() == 0) {
                        mono = Mono
                                .fromSupplier(() -> new Document(new HashMap<String, Object>() {{
                                    put(ID, streamId);
                                    put(VERSION, 1L);
                                }}))
                                .flatMap(data -> mongoTemplate.insert(data, streamVersionCollectionName))
                                .then();
                    } else {
                        mono = mongoTemplate.findOne(query(where(ID).is(streamId)), Document.class, streamVersionCollectionName)
                                .flatMap(document -> {
                                    long eventStreamVersion = document.getLong(VERSION);
                                    return Mono.error(new WriteConditionNotFulfilledException(streamId, eventStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), eventStreamVersion)));
                                });
                    }
                    return mono;
                });
    }

    private static Criteria generateUpdateCondition(String streamId, WriteCondition writeCondition) {
        Criteria streamEq = where(ID).is(streamId);
        if (writeCondition == null) {
            return streamEq;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        StreamVersionWriteCondition c = (StreamVersionWriteCondition) writeCondition;
        return streamEq.andOperator(convertConditionToCriteria(VERSION, c.condition));
    }

    // Initialization
    private static Mono<Void> initializeEventStore(String eventStoreCollectionName, StreamConsistencyGuarantee streamConsistencyGuarantee, ReactiveMongoTemplate mongoTemplate) {
        Mono<MongoCollection<Document>> createEventStoreCollection = createCollection(eventStoreCollectionName, mongoTemplate);
        Mono<String> indexStreamId = createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID), new IndexOptions());

        // Cloud spec defines id + source must be unique!
        Mono<String> indexIdAndSource = createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));

        final Mono<String> additionalIndexes;
        if (streamConsistencyGuarantee instanceof Transactional) {
            // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
            // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
            mongoTemplate.setSessionSynchronization(ALWAYS);
            String streamVersionCollectionName = ((Transactional) streamConsistencyGuarantee).streamVersionCollectionName;

            additionalIndexes = createIndex(streamVersionCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
        } else if (streamConsistencyGuarantee instanceof TransactionalAnnotation) {
            String streamVersionCollectionName = ((TransactionalAnnotation) streamConsistencyGuarantee).streamVersionCollectionName;
            additionalIndexes = createIndex(streamVersionCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending(ID), Indexes.ascending(VERSION)), new IndexOptions().unique(true));
        } else {
            additionalIndexes = Mono.empty();
        }

        return createEventStoreCollection.then(indexStreamId).then(indexIdAndSource).then(additionalIndexes).then();
    }

    private static Mono<String> createIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate, Bson index, IndexOptions indexOptions) {
        return mongoTemplate.getCollection(eventStoreCollectionName).flatMap(collection -> Mono.from(collection.createIndex(index, indexOptions)));
    }

    private static Mono<MongoCollection<Document>> createCollection(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.collectionExists(eventStoreCollectionName).flatMap(exists -> mongoTemplate.createCollection(eventStoreCollectionName));
    }

    // Serialization
    public static Flux<Document> convertToDocuments(EventFormat eventFormat, String streamId, Flux<CloudEvent> cloudEvents) {
        return cloudEvents
                .map(eventFormat::serialize)
                .map(bytes -> {
                    Document cloudEventDocument = Document.parse(new String(bytes, UTF_8));
                    cloudEventDocument.put(STREAM_ID, streamId);
                    return cloudEventDocument;
                });
    }

    public static EventStream<CloudEvent> convertToCloudEvent(EventFormat eventFormat, EventStream<Document> eventStream) {
        return requireNonNull(eventStream)
                .map(Document::toJson)
                .map(eventJsonString -> eventJsonString.getBytes(UTF_8))
                .map(eventFormat::deserialize);
    }
}
