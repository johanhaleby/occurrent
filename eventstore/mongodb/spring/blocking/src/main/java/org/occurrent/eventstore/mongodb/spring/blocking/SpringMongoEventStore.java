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

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;
import org.occurrent.eventstore.mongodb.internal.StreamVersionDiff;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.translateException;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.convertToCloudEvent;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.convertToDocument;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.autoClose;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.mapWithIndex;
import static org.occurrent.mongodb.spring.sortconversion.internal.SortConverter.convertToSpringSort;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * This is an {@link EventStore} that stores events in MongoDB using Spring's {@link MongoTemplate}.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts.
 * <p>
 * By default, only stream-based event-store operations are enabled. Configure
 * {@link EventStoreConfig.Builder#eventStoreCapabilities(Set)} to enable DCB, or to enable both stream and DCB
 * operations. Occurrent creates missing indexes for enabled capabilities, but it never removes indexes automatically.
 * For large production collections, create new indexes out-of-band before enabling a new capability.
 */
@NullMarked
public class SpringMongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore {

    private static final String ID = "_id";
    private static final String DCB_TAGS_INDEX_FIELD = "dcbTags";
    private static final String DCB_POSITION_DOCUMENT_ID = "dcb";
    private static final String DCB_COUNTER_POSITION = "position";
    private static final String CHECKPOINT_LAST_POSITION = "lastPosition";

    private final MongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final String dcbPositionCollectionName;
    private final String dcbCheckpointCollectionName;
    private final TimeRepresentation timeRepresentation;
    private final TransactionTemplate transactionTemplate;
    private final Function<Query, Query> queryOptions;
    private final Function<Query, Query> readOptions;
    private final Set<SpringMongoEventStoreCapability> eventStoreCapabilities;

    /**
     * Create a new instance of {@code SpringBlockingMongoEventStore}
     *
     * @param mongoTemplate The {@link MongoTemplate} that the {@code SpringBlockingMongoEventStore} will use
     * @param config        The {@link EventStoreConfig} that will be used
     */
    public SpringMongoEventStore(MongoTemplate mongoTemplate, EventStoreConfig config) {
        requireNonNull(mongoTemplate, MongoTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = config.eventStoreCollectionName;
        this.dcbPositionCollectionName = eventStoreCollectionName + "_dcb_position";
        this.dcbCheckpointCollectionName = eventStoreCollectionName + "_dcb_checkpoints";
        this.transactionTemplate = config.transactionTemplate;
        this.timeRepresentation = config.timeRepresentation;
        this.queryOptions = config.queryOptions;
        this.readOptions = config.readOptions;
        this.eventStoreCapabilities = config.eventStoreCapabilities;
        initializeEventStore(eventStoreCollectionName, dcbPositionCollectionName, dcbCheckpointCollectionName, eventStoreCapabilities, mongoTemplate);
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        requireStreamCapability();
        final EventStream<Document> eventStream = readEventStream(streamId, null, skip, limit);
        return requireNonNull(eventStream).map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
        requireStreamCapability();
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        // This is an (ugly) hack to fix problems when write condition is "any" and we have parallel writes
        // to the same stream. This will cause MongoDB to throw an exception since we're in a transaction.
        // But in this case we should just retry since if the user has specified "any" as stream version
        // he/she will expect that the events are just written to the event store and WriteConditionNotFulfilledException
        // should not be thrown. Since the write method takes a "Stream" of events we can't simply retry since,
        // on the first retry, the stream would already have been consumed. Thus, we preemptively convert the "events"
        // stream into a list when write condition is any. This way, we can retry without errors.
        final BiFunction<Stream<CloudEvent>, Long, List<Document>> convertCloudEventsToDocuments;
        if (writeCondition.isAnyStreamVersion()) {
            List<CloudEvent> cached = events.toList();
            convertCloudEventsToDocuments = (cloudEvents, currentStreamVersion) -> convertCloudEventsToDocuments(streamId, cached.stream(), currentStreamVersion);
        } else {
            convertCloudEventsToDocuments = (cloudEvents, currentStreamVersion) -> convertCloudEventsToDocuments(streamId, cloudEvents, currentStreamVersion);
        }

        // The actual write logic for the cloud events
        TransactionCallback<StreamVersionDiff> writeLogic = transactionStatus -> {
            long currentStreamVersion = currentStreamVersion(streamId);

            if (!isFulfilled(currentStreamVersion, writeCondition)) {
                throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, currentStreamVersion));
            }

            List<Document> cloudEventDocuments = convertCloudEventsToDocuments.apply(events, currentStreamVersion);

            final long newStreamVersion;
            if (!cloudEventDocuments.isEmpty()) {
                insertAll(streamId, currentStreamVersion, writeCondition, cloudEventDocuments);
                newStreamVersion = cloudEventDocuments.get(cloudEventDocuments.size() - 1).getLong(STREAM_VERSION);
            } else {
                newStreamVersion = currentStreamVersion;
            }
            return new StreamVersionDiff(currentStreamVersion, newStreamVersion);
        };

        StreamVersionDiff streamVersion = RetryStrategy.retry()
                .retryIf(e -> e instanceof WriteConditionNotFulfilledException && writeCondition.isAnyStreamVersion())
                .execute(() -> transactionTemplate.execute(writeLogic));
        return new WriteResult(streamId, streamVersion.oldStreamVersion, streamVersion.newStreamVersion);
    }

    @Override
    public WriteResult write(String streamId, Stream<CloudEvent> events) {
        return write(streamId, StreamVersionWriteCondition.any(), events);
    }

    @Override
    public DcbEventStream read(DcbQuery query, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");

        long highWatermark = currentDcbPosition();
        Query mongoQuery = toDcbMongoQuery(query, options.afterSequencePosition().orElse(0), highWatermark);
        mongoQuery.with(Sort.by(Sort.Direction.ASC, DcbCloudEvents.POSITION));
        List<CloudEvent> events = mongoTemplate.find(mongoQuery, Document.class, eventStoreCollectionName).stream()
                .map(document -> convertToCloudEvent(timeRepresentation, document))
                .toList();
        return new DcbEventStream(events, highWatermark);
    }

    @Override
    public DcbAppendResult append(String streamId, List<CloudEvent> events) {
        requireDcbCapability();
        return appendDcb(streamId, events, null);
    }

    @Override
    public DcbAppendResult append(String streamId, List<CloudEvent> events, DcbAppendCondition condition) {
        requireDcbCapability();
        requireNonNull(condition, "Append condition cannot be null");
        return appendDcb(streamId, events, condition);
    }

    @Override
    public boolean exists(String streamId) {
        requireStreamCapability();
        return mongoTemplate.exists(queryOptions.apply(streamIdEqualTo(streamId)), eventStoreCollectionName);
    }

    @Override
    public boolean exists(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            return count() > 0;
        } else {
            final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
            return mongoTemplate.exists(queryOptions.apply(query), eventStoreCollectionName);
        }
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireStreamCapability();
        requireNonNull(streamId, "Stream id cannot be null");

        transactionTemplate.executeWithoutResult(
                __ -> mongoTemplate.remove(Query.query(streamIdEqualToCriteria(streamId)), eventStoreCollectionName)
        );
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireStreamCapability();
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        mongoTemplate.remove(cloudEventIdEqualTo(cloudEventId, cloudEventSource), eventStoreCollectionName);
    }

    @Override
    public void delete(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
        mongoTemplate.remove(query, eventStoreCollectionName);
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireStreamCapability();
        Function<Function<CloudEvent, CloudEvent>, Optional<CloudEvent>> logic = (fn) -> {
            Query cloudEventQuery = cloudEventIdEqualTo(cloudEventId, cloudEventSource);
            Document document = mongoTemplate.findOne(cloudEventQuery, Document.class, eventStoreCollectionName);
            if (document == null) {
                return Optional.empty();
            }

            CloudEvent currentCloudEvent = convertToCloudEvent(timeRepresentation, document);
            CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
            if (updatedCloudEvent == null) {
                throw new IllegalArgumentException("Cloud event update function is not allowed to return null");
            } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                Document updatedDocument = convertToDocument(timeRepresentation, streamId, streamVersion, updatedCloudEvent);
                updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                mongoTemplate.findAndReplace(cloudEventQuery, updatedDocument, eventStoreCollectionName);
            }
            return Optional.of(updatedCloudEvent);
        };

        return requireNonNull(transactionTemplate.execute(__ -> logic.apply(updateFunction)));
    }

    // Queries
    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        requireStreamCapability();
        requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
        requireNonNull(sortBy, SortBy.class.getSimpleName() + " cannot be null");
        final Query query = queryOptions.apply(FilterConverter.convertFilterToQuery(timeRepresentation, filter));
        return readCloudEvents(query, skip, limit, sortBy)
                .map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    @Override
    public long count(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            return mongoTemplate.execute(eventStoreCollectionName, MongoCollection::estimatedDocumentCount);
        } else {
            final Query query = queryOptions.apply(FilterConverter.convertFilterToQuery(timeRepresentation, filter));
            return mongoTemplate.count(query, eventStoreCollectionName);
        }
    }

    private List<Document> convertCloudEventsToDocuments(String streamId, Stream<CloudEvent> cloudEvents, long currentStreamVersion) {
        return mapWithIndex(cloudEvents, currentStreamVersion, pair -> convertToDocument(timeRepresentation, streamId, pair.t1, pair.t2)).toList();
    }

    private DcbAppendResult appendDcb(String streamId, List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        requireNonNull(streamId, "Stream id cannot be null");
        List<CloudEvent> eventsToAppend = validateDcbEvents(events);

        return requireNonNull(transactionTemplate.execute(transactionStatus -> {
            long firstPosition = reserveDcbPositions(eventsToAppend.size());
            long lastPosition = firstPosition + eventsToAppend.size() - 1;
            long currentStreamVersion = eventStoreCapabilities.contains(STREAM) ? currentStreamVersion(streamId) : firstPosition - 1;
            if (condition != null) {
                updateCheckpoints(condition, lastPosition);
            }

            List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition);
            insertAll(streamId, currentStreamVersion, WriteCondition.anyStreamVersion(), documents);
            return new DcbAppendResult(firstPosition, lastPosition, eventsToAppend.size());
        }));
    }

    private List<Document> convertDcbCloudEventsToDocuments(String streamId, List<CloudEvent> cloudEvents, long currentStreamVersion, long firstPosition) {
        List<Document> documents = new ArrayList<>(cloudEvents.size());
        long streamVersion = currentStreamVersion + 1;
        long position = firstPosition;
        for (CloudEvent cloudEvent : cloudEvents) {
            CloudEvent dcbCloudEvent = DcbCloudEvents.withPosition(cloudEvent, position);
            Document document = convertToDocument(timeRepresentation, streamId, streamVersion++, dcbCloudEvent);
            document.put(DcbCloudEvents.POSITION, position);
            document.put(DCB_TAGS_INDEX_FIELD, new ArrayList<>(DcbCloudEvents.getTags(dcbCloudEvent)));
            documents.add(document);
            position++;
        }
        return documents;
    }

    private void updateCheckpoints(DcbAppendCondition condition, long lastPosition) {
        long afterSequencePosition = condition.afterSequencePosition().orElse(0);
        if (mongoTemplate.exists(toDcbMongoQuery(condition.failIfEventsMatch(), afterSequencePosition, Long.MAX_VALUE), eventStoreCollectionName)) {
            throw new DcbAppendConditionNotFulfilledException(condition, currentDcbPosition(), "Append condition was not fulfilled.");
        }
        for (String key : checkpointKeys(condition.failIfEventsMatch())) {
            Query query = new Query(where(ID).is("checkpoint:" + key));
            Document checkpoint = mongoTemplate.findOne(query, Document.class, dcbCheckpointCollectionName);
            long checkpointPosition = checkpoint == null ? 0 : ((Number) checkpoint.get(CHECKPOINT_LAST_POSITION)).longValue();
            if (checkpointPosition > afterSequencePosition) {
                throw new DcbAppendConditionNotFulfilledException(condition, currentDcbPosition(), "Append condition was not fulfilled.");
            }
            Update update = new Update().set(CHECKPOINT_LAST_POSITION, lastPosition);
            mongoTemplate.upsert(query, update, dcbCheckpointCollectionName);
        }
    }

    private static Set<String> checkpointKeys(DcbQuery query) {
        if (query.matchAll()) {
            return Set.of("all");
        }
        java.util.TreeSet<String> keys = new java.util.TreeSet<>();
        for (DcbQueryItem item : query.items()) {
            item.tags().forEach(tag -> keys.add("tag:" + tag));
            if (item.tags().isEmpty()) {
                item.types().forEach(type -> keys.add("type:" + type));
            }
        }
        return keys;
    }

    private long reserveDcbPositions(int eventCount) {
        Query query = new Query(where(ID).is(DCB_POSITION_DOCUMENT_ID));
        Update update = new Update().inc(DCB_COUNTER_POSITION, eventCount);
        FindAndModifyOptions options = FindAndModifyOptions.options().upsert(true).returnNew(true);
        Document updated = mongoTemplate.findAndModify(query, update, options, Document.class, dcbPositionCollectionName);
        long lastPosition = ((Number) requireNonNull(updated, "DCB position document cannot be null").get(DCB_COUNTER_POSITION)).longValue();
        return lastPosition - eventCount + 1;
    }

    private long currentDcbPosition() {
        Document document = mongoTemplate.findById(DCB_POSITION_DOCUMENT_ID, Document.class, dcbPositionCollectionName);
        return document == null ? 0 : ((Number) document.get(DCB_COUNTER_POSITION)).longValue();
    }

    private static List<CloudEvent> validateDcbEvents(List<CloudEvent> events) {
        requireNonNull(events, "Events cannot be null");
        List<CloudEvent> copy = List.copyOf(events);
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("Events cannot be empty");
        }
        return copy.stream()
                .map(event -> DcbCloudEvents.withTags(event, DcbCloudEvents.getTags(event)))
                .toList();
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit) {
        requireStreamCapability();
        requireNonNull(streamId, "Stream id cannot be null");
        requireNonNull(filter, "filter cannot be null");
        final EventStream<Document> eventStream = readEventStream(streamId, filter, skip, limit);
        return requireNonNull(eventStream).map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    @NullUnmarked
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

        @SuppressWarnings("unused")
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

    private void insertAll(String streamId, long streamVersion, WriteCondition writeCondition, List<Document> documents) {
        try {
            mongoTemplate.insert(documents, eventStoreCollectionName);
        } catch (DataAccessException e) {
            final Throwable rootCause = e.getRootCause();
            if (rootCause instanceof MongoException) {
                throw translateException(new WriteContext(streamId, streamVersion, writeCondition), (MongoException) rootCause);
            } else {
                throw e;
            }
        }
    }

    private static boolean isFulfilled(long currentStreamVersion, WriteCondition writeCondition) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof StreamVersionWriteCondition)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        Condition<Long> condition = ((StreamVersionWriteCondition) writeCondition).condition();
        return LongConditionEvaluator.evaluate(condition, currentStreamVersion);
    }

    // Read
    private static Query streamIdEqualTo(String streamId) {
        return Query.query(streamIdEqualToCriteria(streamId));
    }

    private static Query toDcbMongoQuery(DcbQuery query, long afterSequencePosition, long upperSequencePosition) {
        Criteria positionCriteria = where(DcbCloudEvents.POSITION).gt(afterSequencePosition).lte(upperSequencePosition);
        if (query.matchAll()) {
            return new Query(positionCriteria);
        }
        List<Criteria> itemCriteria = query.items().stream()
                .map(SpringMongoEventStore::toCriteria)
                .toList();
        return new Query(new Criteria().andOperator(positionCriteria, new Criteria().orOperator(itemCriteria)));
    }

    private static Criteria toCriteria(DcbQueryItem item) {
        List<Criteria> criteria = new ArrayList<>();
        if (!item.types().isEmpty()) {
            criteria.add(where("type").in(item.types()));
        }
        for (String tag : item.tags()) {
            criteria.add(where(DCB_TAGS_INDEX_FIELD).is(tag));
        }
        return new Criteria().andOperator(criteria);
    }

    private static Criteria streamIdEqualToCriteria(String streamId) {
        return where(STREAM_ID).is(streamId);
    }

    private EventStreamImpl<Document> readEventStream(String streamId, @Nullable StreamReadFilter streamReadFilter, int skip, int limit) {
        long currentStreamVersion = currentStreamVersion(streamId);
        if (currentStreamVersion == 0) {
            return new EventStreamImpl<>(streamId, 0, Stream.empty());
        }

        // We use "lte" currentStreamVersion so that we don't have the start transactions on read. This means that even
        // if another thread has inserted more events after we've read "currentStreamVersion" it doesn't matter.
        Query query = Query.query(streamIdEqualToCriteria(streamId).and(STREAM_VERSION).lte(currentStreamVersion));

        if (streamReadFilter != null) {
            StreamReadFilterValidator.validate(streamReadFilter);
            Filter filter = StreamReadFilterToFilterMapper.map(streamReadFilter);
            var criteria = FilterConverter.convertFilterToCriteria(null, timeRepresentation, filter);
            query.addCriteria(criteria);
        }

        Stream<Document> stream = readCloudEvents(readOptions.apply(query), skip, limit, SortBy.streamVersion(ASCENDING));
        return new EventStreamImpl<>(streamId, currentStreamVersion, stream);
    }

    private long currentStreamVersion(String streamId) {
        Query query = readOptions.apply(streamIdEqualTo(streamId));
        query.fields().include(STREAM_VERSION);
        Document documentWithLatestStreamVersion = mongoTemplate.findOne(queryOptions.apply(query.with(Sort.by(DESC, STREAM_VERSION)).limit(1)), Document.class, eventStoreCollectionName);
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

        Sort sort = convertToSpringSort(sortBy);
        return autoClose(mongoTemplate.stream(query.with(sort), Document.class, eventStoreCollectionName));
    }

    // Initialization
    private static void initializeEventStore(String eventStoreCollectionName, String dcbPositionCollectionName, String dcbCheckpointCollectionName, Set<SpringMongoEventStoreCapability> eventStoreCapabilities, MongoTemplate mongoTemplate) {
        if (!mongoTemplate.collectionExists(eventStoreCollectionName)) {
            mongoTemplate.createCollection(eventStoreCollectionName);
        }
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        if (dcbEnabled && !mongoTemplate.collectionExists(dcbPositionCollectionName)) {
            mongoTemplate.createCollection(dcbPositionCollectionName);
        }
        if (dcbEnabled && !mongoTemplate.collectionExists(dcbCheckpointCollectionName)) {
            mongoTemplate.createCollection(dcbCheckpointCollectionName);
        }

        MongoCollection<Document> eventStoreCollection = mongoTemplate.getCollection(eventStoreCollectionName);
        // Cloud spec defines id + source must be unique!
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(CloudEventV1.ID), Indexes.ascending(CloudEventV1.SOURCE)), new IndexOptions().unique(true));
        if (eventStoreCapabilities.contains(STREAM)) {
            // Create a streamId + streamVersion ascending index (note that we don't need to index stream id separately since it's covered by this compound index)
            // Note also that this index supports sorting both ascending and descending since MongoDB can traverse an index in both directions.
            eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.ascending(STREAM_VERSION)), new IndexOptions().unique(true));
        }
        if (dcbEnabled) {
            eventStoreCollection.createIndex(Indexes.ascending(DcbCloudEvents.POSITION), new IndexOptions().unique(true).sparse(true));
            eventStoreCollection.createIndex(Indexes.ascending(DCB_TAGS_INDEX_FIELD));
        }

        // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
        // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
        mongoTemplate.setSessionSynchronization(ALWAYS);
    }

    private void requireStreamCapability() {
        requireCapability(STREAM);
    }

    private void requireDcbCapability() {
        requireCapability(DCB);
    }

    private void requireCapability(SpringMongoEventStoreCapability capability) {
        if (!eventStoreCapabilities.contains(capability)) {
            throw new UnsupportedOperationException(capability + " capability is not enabled for this SpringMongoEventStore");
        }
    }

    private static Query cloudEventIdEqualTo(String cloudEventId, URI cloudEventSource) {
        return Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource));
    }
}
