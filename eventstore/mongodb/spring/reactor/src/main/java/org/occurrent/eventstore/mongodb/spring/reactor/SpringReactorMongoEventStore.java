/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.mongodb.internal.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.filter.Filter.TIME;
import static org.springframework.data.domain.Sort.Direction.ASC;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * This is a reactive {@link EventStore} implementation that stores events in MongoDB using
 * Spring's {@link ReactiveMongoTemplate} that is based on <a href="https://projectreactor.io/">project reactor</a>.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts.
 */
public class SpringReactorMongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

    private static final String ID = "_id";

    private final ReactiveMongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final TransactionalOperator transactionalOperator;

    /**
     * Create a new instance of {@code SpringReactorMongoEventStore}
     *
     * @param mongoTemplate The {@link ReactiveMongoTemplate} that the {@code SpringReactorMongoEventStore} will use
     * @param config        The {@link EventStoreConfig} that will be used
     */
    public SpringReactorMongoEventStore(ReactiveMongoTemplate mongoTemplate, EventStoreConfig config) {
        requireNonNull(mongoTemplate, ReactiveMongoTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = config.eventStoreCollectionName;
        this.transactionalOperator = config.transactionalOperator;
        this.timeRepresentation = config.timeRepresentation;
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        initializeEventStore(eventStoreCollectionName, mongoTemplate).block();
    }

    @Override
    public Mono<Void> write(String streamId, Flux<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public Mono<Void> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        return transactionalOperator.execute(transactionStatus -> {
                    Flux<Document> documentFlux = currentStreamVersion(streamId)
                            .flatMap(currentStreamVersion -> {
                                final Mono<Long> result;
                                if (isFulfilled(currentStreamVersion, writeCondition)) {
                                    result = Mono.just(currentStreamVersion);
                                } else {
                                    result = Mono.error(new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion)));
                                }
                                return result;
                            })
                            .flatMapMany(currentStreamVersion ->
                                    infiniteFluxFrom(currentStreamVersion)
                                            .zipWith(events)
                                            .map(streamVersionAndEvent -> {
                                                long streamVersion = streamVersionAndEvent.getT1();
                                                CloudEvent event = streamVersionAndEvent.getT2();
                                                return OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(cloudEventSerializer, timeRepresentation, streamId, streamVersion, event);
                                            }));
                    return insertAll(documentFlux);
                }
        ).then();
    }

    private static Flux<Long> infiniteFluxFrom(Long currentStreamVersion) {
        return Flux.generate(() -> currentStreamVersion, (version, sink) -> {
            long nextVersion = version + 1L;
            sink.next(nextVersion);
            return nextVersion;
        });
    }

    @Override
    public Mono<Boolean> exists(String streamId) {
        return mongoTemplate.exists(streamIdEqualTo(streamId), eventStoreCollectionName);
    }

    @Override
    public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
        Mono<EventStreamImpl> eventStream = transactionalOperator.execute(transactionStatus -> readEventStream(streamId, skip, limit)).single();
        return convertToCloudEvent(cloudEventSerializer, timeRepresentation, eventStream);
    }

    // Read
    private Mono<EventStreamImpl> readEventStream(String streamId, int skip, int limit) {
        return currentStreamVersion(streamId)
                .flatMap(currentStreamVersion -> {
                    Flux<Document> cloudEventDocuments = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC);
                    return Mono.just(new EventStreamImpl(streamId, currentStreamVersion, cloudEventDocuments));
                })
                .switchIfEmpty(Mono.fromSupplier(() -> new EventStreamImpl(streamId, 0, Flux.empty())));
    }

    private Flux<Document> readCloudEvents(Query query, int skip, int limit, SortBy sortBy) {
        if (isSkipOrLimitDefined(skip, limit)) {
            query.skip(skip).limit(limit);
        }

        switch (sortBy) {
            case TIME_ASC:
                query.with(Sort.by(ASC, TIME));
                break;
            case TIME_DESC:
                query.with(Sort.by(DESC, TIME));
                break;
            case NATURAL_ASC:
                break;
            case NATURAL_DESC:
                query.with(Sort.by(DESC, ID));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + sortBy);
        }

        return mongoTemplate.find(query, Document.class, eventStoreCollectionName);
    }

    private Mono<Long> currentStreamVersion(String streamId) {
        Query query = Query.query(where(OccurrentCloudEventExtension.STREAM_ID).is(streamId));
        query.fields().include(OccurrentCloudEventExtension.STREAM_VERSION);
        return mongoTemplate.findOne(query.with(Sort.by(DESC, OccurrentCloudEventExtension.STREAM_VERSION)).limit(1), Document.class, eventStoreCollectionName)
                .map(documentWithLatestStreamVersion -> documentWithLatestStreamVersion.getLong(OccurrentCloudEventExtension.STREAM_VERSION))
                .switchIfEmpty(Mono.just(0L));
    }


    private Flux<Document> insertAll(Flux<Document> documents) {
        return mongoTemplate.insertAll(documents.collectList(), eventStoreCollectionName)
                .onErrorMap(DuplicateKeyException.class, Throwable::getCause)
                .onErrorMap(MongoBulkWriteException.class, MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator::translateToDuplicateCloudEventException);
    }

    private static boolean isFulfilled(long streamVersion, WriteCondition writeCondition) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        WriteCondition.StreamVersionWriteCondition c = (WriteCondition.StreamVersionWriteCondition) writeCondition;
        return LongConditionEvaluator.evaluate(c.condition, streamVersion);
    }

    // Initialization
    private static Mono<Void> initializeEventStore(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        Mono<MongoCollection<Document>> createEventStoreCollection = createCollection(eventStoreCollectionName, mongoTemplate);

        // Stream id
        Mono<String> indexStreamId = createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID), new IndexOptions());

        // Cloud spec defines id + source must be unique!
        Mono<String> indexIdAndSource = createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));

        // Create a streamId + streamVersion index
        Mono<String> indexStreamIdAndStreamVersion = createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID), Indexes.descending(OccurrentCloudEventExtension.STREAM_VERSION)), new IndexOptions().unique(true));

        // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
        // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
        mongoTemplate.setSessionSynchronization(ALWAYS);

        return createEventStoreCollection.then(indexStreamId).then(indexIdAndSource).then(indexStreamIdAndStreamVersion).then();
    }

    private static Mono<String> createIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate, Bson index, IndexOptions indexOptions) {
        return mongoTemplate.getCollection(eventStoreCollectionName).flatMap(collection -> Mono.from(collection.createIndex(index, indexOptions)));
    }

    private static Mono<MongoCollection<Document>> createCollection(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.collectionExists(eventStoreCollectionName).flatMap(exists -> exists ? Mono.empty() : mongoTemplate.createCollection(eventStoreCollectionName));
    }

    public static Mono<EventStream<CloudEvent>> convertToCloudEvent(EventFormat eventFormat, TimeRepresentation timeRepresentation, Mono<EventStreamImpl> eventStream) {
        return eventStream.map(es -> es.map(document -> convertToCloudEvent(eventFormat, timeRepresentation, document)));
    }

    private static CloudEvent convertToCloudEvent(EventFormat eventFormat, TimeRepresentation timeRepresentation, Document document) {
        return OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent(eventFormat, timeRepresentation, document);
    }

    private static boolean isSkipOrLimitDefined(int skip, int limit) {
        return skip != 0 || limit != Integer.MAX_VALUE;
    }

    @Override
    public Mono<Void> deleteEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");

        return mongoTemplate.remove(streamIdEqualTo(streamId), eventStoreCollectionName).then();
    }

    @Override
    public Mono<Void> deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        return mongoTemplate.remove(Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource)), eventStoreCollectionName).then();
    }

    @Override
    public Mono<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        Function<Function<CloudEvent, CloudEvent>, Mono<CloudEvent>> logic = (fn) -> {
            Query cloudEventQuery = cloudEventIdIs(cloudEventId, cloudEventSource);
            return mongoTemplate.findOne(cloudEventQuery, Document.class, eventStoreCollectionName)
                    .log()
                    .flatMap(document -> {
                        CloudEvent currentCloudEvent = convertToCloudEvent(cloudEventSerializer, timeRepresentation, document);
                        CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
                        final Mono<CloudEvent> result;
                        if (updatedCloudEvent == null) {
                            result = Mono.error(new IllegalArgumentException("Cloud event update function is not allowed to return null"));
                        } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                            String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                            long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                            Document updatedDocument = OccurrentCloudEventMongoDBDocumentMapper.convertToDocument(cloudEventSerializer, timeRepresentation, streamId, streamVersion, updatedCloudEvent);
                            updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                            result = mongoTemplate.findAndReplace(cloudEventQuery, updatedDocument, eventStoreCollectionName).thenReturn(updatedCloudEvent);
                        } else {
                            result = Mono.empty();
                        }
                        return result;
                    });
        };

        return transactionalOperator.transactional(logic.apply(updateFunction));
    }

    @Override
    public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        requireNonNull(filter, "Filter cannot be null");
        final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
        return readCloudEvents(query, skip, limit, sortBy)
                .map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    @SuppressWarnings("unused")
    private static class EventStreamImpl implements EventStream<Document> {
        private String id;
        private long version;
        private Flux<Document> events;

        @SuppressWarnings("unused")
        EventStreamImpl() {
        }

        EventStreamImpl(String id, long version, Flux<Document> events) {
            this.id = id;
            this.version = version;
            this.events = events;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Flux<Document> events() {
            return events;
        }
    }

    private static Query streamIdEqualTo(String streamId) {
        return Query.query(where(OccurrentCloudEventExtension.STREAM_ID).is(streamId));
    }

    private static Query cloudEventIdIs(String cloudEventId, URI cloudEventSource) {
        return Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource));
    }
}