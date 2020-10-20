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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.StreamUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.mongodb.internal.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToCloudEvent;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDBDocumentMapper.convertToDocument;
import static org.occurrent.filter.Filter.TIME;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.mapWithIndex;
import static org.springframework.data.domain.Sort.Direction.ASC;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * This is an {@link EventStore} that stores events in MongoDB using Spring's {@link MongoTemplate}.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts.
 */
public class SpringBlockingMongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

    private static final String ID = "_id";

    private final MongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final EventFormat cloudEventSerializer;
    private final TimeRepresentation timeRepresentation;
    private final TransactionTemplate transactionTemplate;

    /**
     * Create a new instance of {@code SpringBlockingMongoEventStore}
     *
     * @param mongoTemplate The {@link MongoTemplate} that the {@code SpringBlockingMongoEventStore} will use
     * @param config        The {@link EventStoreConfig} that will be used
     */
    public SpringBlockingMongoEventStore(MongoTemplate mongoTemplate, EventStoreConfig config) {
        requireNonNull(mongoTemplate, MongoTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(mongoTemplate, EventStoreConfig.class.getSimpleName() + " cannot be null");
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = config.eventStoreCollectionName;
        this.transactionTemplate = config.transactionTemplate;
        this.timeRepresentation = config.timeRepresentation;
        cloudEventSerializer = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
        initializeEventStore(eventStoreCollectionName, mongoTemplate);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        final EventStream<Document> eventStream = transactionTemplate.execute(transactionStatus -> readEventStream(streamId, skip, limit));
        return requireNonNull(eventStream).map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            long currentStreamVersion = currentStreamVersion(streamId);

            if (!isFulfilled(currentStreamVersion, writeCondition)) {
                throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion));
            }

            List<Document> cloudEventDocuments = mapWithIndex(events, currentStreamVersion, pair -> convertToDocument(cloudEventSerializer, timeRepresentation, streamId, pair.t1, pair.t2)).collect(Collectors.toList());

            insertAll(cloudEventDocuments);
        });
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, StreamVersionWriteCondition.any(), events);
    }

    @Override
    public boolean exists(String streamId) {
        return mongoTemplate.exists(Query.query(where(STREAM_ID).is(streamId)), eventStoreCollectionName);
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireNonNull(streamId, "Stream id cannot be null");

        transactionTemplate.executeWithoutResult(
                __ -> mongoTemplate.remove(Query.query(where(STREAM_ID).is(streamId)), eventStoreCollectionName)
        );
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        mongoTemplate.remove(cloudEventIdEqualTo(cloudEventId, cloudEventSource), eventStoreCollectionName);
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        Function<Function<CloudEvent, CloudEvent>, Optional<CloudEvent>> logic = (fn) -> {
            Query cloudEventQuery = cloudEventIdEqualTo(cloudEventId, cloudEventSource);
            Document document = mongoTemplate.findOne(cloudEventQuery, Document.class, eventStoreCollectionName);
            if (document == null) {
                return Optional.empty();
            }

            CloudEvent currentCloudEvent = convertToCloudEvent(cloudEventSerializer, timeRepresentation, document);
            CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
            if (updatedCloudEvent == null) {
                throw new IllegalArgumentException("Cloud event update function is not allowed to return null");
            } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                Document updatedDocument = convertToDocument(cloudEventSerializer, timeRepresentation, streamId, streamVersion, updatedCloudEvent);
                updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                mongoTemplate.findAndReplace(cloudEventQuery, updatedDocument, eventStoreCollectionName);
            }
            return Optional.of(updatedCloudEvent);
        };

        return transactionTemplate.execute(__ -> logic.apply(updateFunction));
    }

    // Queries
    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        requireNonNull(filter, "Filter cannot be null");
        final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
        return readCloudEvents(query, skip, limit, sortBy)
                .map(document -> convertToCloudEvent(cloudEventSerializer, timeRepresentation, document));
    }

    @Override
    public long count(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            //noinspection ConstantConditions
            return mongoTemplate.execute(eventStoreCollectionName, MongoCollection::estimatedDocumentCount);
        } else {
            final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
            return mongoTemplate.count(query, eventStoreCollectionName);
        }
    }

    // Data structures etc
    private static class EventStreamImpl<T> implements EventStream<T> {
        private String _id;
        private long version;
        private Stream<T> events;

        @SuppressWarnings("unused")
        EventStreamImpl() {
        }

        EventStreamImpl(String _id, long version, Stream<T> events) {
            this._id = _id;
            this.version = version;
            this.events = events;
        }

        @Override
        public String id() {
            return _id;
        }

        @Override
        public long version() {
            return version;
        }

        @Override
        public Stream<T> events() {
            return events;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        public void setEvents(Stream<T> events) {
            this.events = events;
        }
    }

    private void insertAll(List<Document> documents) {
        try {
            mongoTemplate.getCollection(eventStoreCollectionName).insertMany(documents);
        } catch (MongoBulkWriteException e) {
            throw translateToDuplicateCloudEventException(e);
        }
    }

    private static boolean isFulfilled(long currentStreamVersion, WriteCondition writeCondition) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        Condition<Long> condition = ((StreamVersionWriteCondition) writeCondition).condition;
        return LongConditionEvaluator.evaluate(condition, currentStreamVersion);
    }

    // Read
    private static Query streamIdEqualTo(String streamId) {
        return Query.query(where(STREAM_ID).is(streamId));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit) {
        long currentStreamVersion = currentStreamVersion(streamId);
        if (currentStreamVersion == 0) {
            return new EventStreamImpl<>(streamId, 0, Stream.empty());
        }

        Stream<Document> stream = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC);
        return new EventStreamImpl<>(streamId, currentStreamVersion, stream);
    }

    private long currentStreamVersion(String streamId) {
        Query query = Query.query(where(STREAM_ID).is(streamId));
        query.fields().include(STREAM_VERSION);
        Document documentWithLatestStreamVersion = mongoTemplate.findOne(query.with(Sort.by(DESC, STREAM_VERSION)).limit(1), Document.class, eventStoreCollectionName);
        final long currentStreamVersion;
        if (documentWithLatestStreamVersion == null) {
            currentStreamVersion = 0;
        } else {
            currentStreamVersion = documentWithLatestStreamVersion.getLong(STREAM_VERSION);
        }
        return currentStreamVersion;
    }

    private Stream<Document> readCloudEvents(Query query, int skip, int limit, SortBy sortBy) {
        if (skip != 0 || limit != Integer.MAX_VALUE) {
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
                query.with(Sort.by(ASC, ID));
                break;
            case NATURAL_DESC:
                query.with(Sort.by(DESC, ID));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + sortBy);
        }

        return StreamUtils.createStreamFromIterator(mongoTemplate.stream(query, Document.class, eventStoreCollectionName));
    }

    // Initialization
    private static void initializeEventStore(String eventStoreCollectionName, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(eventStoreCollectionName)) {
            mongoTemplate.createCollection(eventStoreCollectionName);
        }
        MongoCollection<Document> eventStoreCollection = mongoTemplate.getCollection(eventStoreCollectionName);
        eventStoreCollection.createIndex(Indexes.ascending(STREAM_ID));
        // Cloud spec defines id + source must be unique!
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        // Create a streamId + streamVersion index
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.descending(STREAM_VERSION)), new IndexOptions().unique(true));

        // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
        // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
        mongoTemplate.setSessionSynchronization(ALWAYS);
    }

    private static Query cloudEventIdEqualTo(String cloudEventId, URI cloudEventSource) {
        return Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource));
    }
}