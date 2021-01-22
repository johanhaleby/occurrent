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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.filter.Filter;
import org.occurrent.functionalsupport.internal.FunctionalSupport.Pair;
import org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import static org.occurrent.eventstore.api.WriteCondition.anyStreamVersion;
import static org.occurrent.eventstore.mongodb.internal.MongoBulkWriteExceptionToDuplicateCloudEventExceptionTranslator.translateToDuplicateCloudEventException;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.convertToDocument;
import static org.occurrent.filter.Filter.TIME;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.zip;

/**
 * This is an {@link EventStore} that stores events in MongoDB using the "native" synchronous java driver MongoDB.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts.
 */
public class MongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries {
    private static final String ID = "_id";
    private static final String NATURAL = "$natural";

    private final MongoCollection<Document> eventCollection;
    private final MongoClient mongoClient;
    private final TimeRepresentation timeRepresentation;
    private final TransactionOptions transactionOptions;

    /**
     * Create a new instance of {@code MongoEventStore}
     *
     * @param mongoClient         The mongo client that the {@code MongoEventStore} will use
     * @param databaseName        The name of the database in which events will be persisted
     * @param eventCollectionName The name of the collection in which events will be persisted
     * @param config              The {@link EventStoreConfig} that will be used
     */
    public MongoEventStore(MongoClient mongoClient, String databaseName, String eventCollectionName, EventStoreConfig config) {
        this(requireNonNull(mongoClient, "Mongo client cannot be null"),
                requireNonNull(mongoClient.getDatabase(databaseName), "Database must be defined"),
                mongoClient.getDatabase(databaseName).getCollection(eventCollectionName), config);
    }

    /**
     * Create a new instance of {@code MongoEventStore}
     *
     * @param mongoClient     The mongo client that the {@code MongoEventStore} will use
     * @param database        The database in which events will be persisted
     * @param eventCollection The collection in which events will be persisted
     * @param config          The {@link EventStoreConfig} that will be used
     */
    public MongoEventStore(MongoClient mongoClient, MongoDatabase database, MongoCollection<Document> eventCollection, EventStoreConfig config) {
        requireNonNull(mongoClient, "Mongo client cannot be null");
        requireNonNull(database, "Database must be defined");
        requireNonNull(eventCollection, "Event collection must be defined");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        this.mongoClient = mongoClient;
        this.eventCollection = eventCollection;
        transactionOptions = config.transactionOptions;
        this.timeRepresentation = config.timeRepresentation;
        initializeEventStore(eventCollection, database);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        EventStream<Document> eventStream = readEventStream(streamId, skip, limit, transactionOptions);
        return eventStream.map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, int skip, int limit, TransactionOptions transactionOptions) {
        try (ClientSession clientSession = mongoClient.startSession()) {
            return clientSession.withTransaction(() -> {
                long currentStreamVersion = currentStreamVersion(streamId);
                if (currentStreamVersion == 0) {
                    return new EventStreamImpl<>(streamId, 0, Stream.empty());
                }

                Stream<Document> stream = readCloudEvents(streamIdEqualTo(streamId), skip, limit, SortBy.NATURAL_ASC, clientSession);
                return new EventStreamImpl<>(streamId, currentStreamVersion, stream);
            }, transactionOptions);
        }
    }

    private long currentStreamVersion(String streamId) {
        Document documentWithLatestStreamVersion = eventCollection.find(streamIdEqualTo(streamId)).sort(Sorts.descending(OccurrentCloudEventExtension.STREAM_VERSION)).limit(1).projection(Projections.include(OccurrentCloudEventExtension.STREAM_VERSION)).first();
        final long currentStreamVersion;
        if (documentWithLatestStreamVersion == null) {
            currentStreamVersion = 0;
        } else {
            currentStreamVersion = documentWithLatestStreamVersion.getLong(OccurrentCloudEventExtension.STREAM_VERSION);
        }
        return currentStreamVersion;
    }

    private Stream<Document> readCloudEvents(Bson query, int skip, int limit, SortBy sortBy, ClientSession clientSession) {
        final FindIterable<Document> documentsWithoutSkipAndLimit;
        if (clientSession == null) {
            documentsWithoutSkipAndLimit = eventCollection.find(query);
        } else {
            documentsWithoutSkipAndLimit = eventCollection.find(clientSession, query);
        }

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }

        switch (sortBy) {
            case TIME_ASC:
                documentsWithoutSkipAndLimit.sort(ascending(TIME));
                break;
            case TIME_DESC:
                documentsWithoutSkipAndLimit.sort(descending(TIME));
                break;
            case NATURAL_ASC:
                documentsWithoutSkipAndLimit.sort(ascending(NATURAL));
                break;
            case NATURAL_DESC:
                documentsWithoutSkipAndLimit.sort(descending(NATURAL));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + sortBy);
        }

        return StreamSupport.stream(documentsWithSkipAndLimit.spliterator(), false);
    }

    @Override
    public void write(String streamId, Stream<CloudEvent> events) {
        write(streamId, anyStreamVersion(), events);
    }

    @Override
    public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        try (ClientSession clientSession = mongoClient.startSession()) {
            clientSession.withTransaction(() -> {
                long currentStreamVersion = currentStreamVersion(streamId);

                if (!isFulfilled(currentStreamVersion, writeCondition)) {
                    throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion));
                }

                List<Document> cloudEventDocuments = zip(LongStream.iterate(currentStreamVersion + 1, i -> i + 1).boxed(), events, Pair::new)
                        .map(pair -> convertToDocument(timeRepresentation, streamId, pair.t1, pair.t2))
                        .collect(Collectors.toList());

                if (!cloudEventDocuments.isEmpty()) {
                    try {
                        eventCollection.insertMany(clientSession, cloudEventDocuments);
                    } catch (MongoBulkWriteException e) {
                        throw translateToDuplicateCloudEventException(e);
                    }
                }
                return "";
            }, transactionOptions);
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

    @Override
    public boolean exists(String streamId) {
        return eventCollection.countDocuments(eq(OccurrentCloudEventExtension.STREAM_ID, streamId)) > 0;
    }

    @Override
    public void deleteEventStream(String streamId) {
        eventCollection.deleteMany(eq(OccurrentCloudEventExtension.STREAM_ID, streamId));
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        eventCollection.deleteOne(uniqueCloudEvent(cloudEventId, cloudEventSource));
    }

    @Override
    public void delete(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        final Bson bson = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
        eventCollection.deleteMany(bson);
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireNonNull(updateFunction, "Update function cannot be null");

        Bson cloudEvent = uniqueCloudEvent(cloudEventId, cloudEventSource);
        final Optional<CloudEvent> result;
        try (ClientSession clientSession = mongoClient.startSession()) {
            result = clientSession.withTransaction(
                    () -> updateCloudEvent(updateFunction, () -> eventCollection.find(clientSession, cloudEvent), updatedDocument -> eventCollection.replaceOne(clientSession, cloudEvent, updatedDocument)),
                    transactionOptions);
        }
        return result;
    }

    private Optional<CloudEvent> updateCloudEvent(Function<CloudEvent, CloudEvent> fn, Supplier<FindIterable<Document>> cloudEventFinder, Function<Document, UpdateResult> cloudEventUpdater) {
        Document document = cloudEventFinder.get().first();
        if (document == null) {
            return Optional.empty();
        } else {
            CloudEvent currentCloudEvent = convertToCloudEvent(timeRepresentation, document);
            CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
            if (updatedCloudEvent == null) {
                throw new IllegalArgumentException("Cloud event update function is not allowed to return null");
            } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                Document updatedDocument = convertToDocument(timeRepresentation, streamId, streamVersion, updatedCloudEvent);
                updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                cloudEventUpdater.apply(updatedDocument);
            }
            return Optional.of(updatedCloudEvent);
        }
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        requireNonNull(filter, "Filter cannot be null");
        final Bson query = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
        return readCloudEvents(query, skip, limit, sortBy, null)
                .map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    @Override
    public long count(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            return eventCollection.estimatedDocumentCount();
        } else {
            final Bson query = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
            return eventCollection.countDocuments(query);
        }
    }

    @Override
    public boolean exists(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        return count(filter) > 0;
    }

    private static class EventStreamImpl<T> implements EventStream<T> {
        private final String id;
        private final long version;
        private final Stream<T> events;

        EventStreamImpl(String id, long version, Stream<T> events) {
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
        public Stream<T> events() {
            return events;
        }

    }

    private static void initializeEventStore(MongoCollection<Document> eventStoreCollection, MongoDatabase mongoDatabase) {
        String eventStoreCollectionName = eventStoreCollection.getNamespace().getCollectionName();
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        // Create a streamId index
        eventStoreCollection.createIndex(Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID));
        // Cloud spec defines id + source must be unique!
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        // Create a streamId + streamVersion index
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(OccurrentCloudEventExtension.STREAM_ID), Indexes.descending(OccurrentCloudEventExtension.STREAM_VERSION)), new IndexOptions().unique(true));
    }

    private static boolean collectionExists(MongoDatabase mongoDatabase, String collectionName) {
        for (String listCollectionName : mongoDatabase.listCollectionNames()) {
            if (listCollectionName.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private static Bson streamIdEqualTo(String streamId) {
        return eq(OccurrentCloudEventExtension.STREAM_ID, streamId);
    }

    private static Bson uniqueCloudEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");
        return and(eq("id", cloudEventId), eq("source", cloudEventSource.toString()));
    }
}