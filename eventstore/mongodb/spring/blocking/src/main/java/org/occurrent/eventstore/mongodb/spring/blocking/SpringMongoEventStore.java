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
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.DCB_TAGS_INDEX_FIELD;
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
    private static final String DCB_POSITION_DOCUMENT_ID = "dcb";
    private static final String DCB_COUNTER_POSITION = "position";
    private static final String CHECKPOINT_LAST_POSITION = "lastPosition";
    private static final String CHECKPOINT_VERSION = "version";

    private final MongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final String dcbPositionCollectionName;
    private final String dcbCheckpointCollectionName;
    private final TimeRepresentation timeRepresentation;
    private final TransactionTemplate transactionTemplate;
    private final Function<Query, Query> queryOptions;
    private final Function<Query, Query> readOptions;
    private final Set<SpringMongoEventStoreCapability> eventStoreCapabilities;
    private final DcbStreamIdGenerator dcbStreamIdGenerator;

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
        this.dcbStreamIdGenerator = config.dcbStreamIdGenerator;
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

        // Snapshot the consistency token BEFORE reading the events. If an append commits between these two reads, the
        // events may include it while the token does not, which only makes a later conditional append over-cautious (a
        // false conflict that retries) rather than miss the conflict.
        long consistencyTokenValue = consistencyToken(query);
        long highWatermark = currentDcbPosition();
        long upperBound = Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
        Query mongoQuery = toDcbMongoQuery(query, options.afterSequencePosition().orElse(0), upperBound);
        mongoQuery.with(Sort.by(Sort.Direction.ASC, DcbCloudEvents.POSITION));
        List<CloudEvent> events = mongoTemplate.find(queryOptions.apply(mongoQuery), Document.class, eventStoreCollectionName).stream()
                .map(document -> convertToCloudEvent(timeRepresentation, document))
                .toList();
        return new DcbEventStream(events, highWatermark, DcbConsistencyToken.of(consistencyTokenValue));
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events) {
        requireDcbCapability();
        return appendDcb(events, null);
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
        requireDcbCapability();
        requireNonNull(condition, "Append condition cannot be null");
        return appendDcb(events, condition);
    }

    @Override
    public boolean exists(DcbQuery query, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return mongoTemplate.exists(queryOptions.apply(toDcbMongoQuery(query, lowerBound(options), upperBound(options))), eventStoreCollectionName);
    }

    @Override
    public long count(DcbQuery query, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return mongoTemplate.count(queryOptions.apply(toDcbMongoQuery(query, lowerBound(options), upperBound(options))), eventStoreCollectionName);
    }

    private long lowerBound(DcbReadOptions options) {
        return options.afterSequencePosition().orElse(0);
    }

    private long upperBound(DcbReadOptions options) {
        long highWatermark = currentDcbPosition();
        return Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
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

    private DcbAppendResult appendDcb(List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        List<CloudEvent> eventsToAppend = validateDcbEvents(events);
        // Place by the condition's boundary tags when it constrains on tags, so the same boundary always lands in
        // the same partition regardless of per-event tags. Otherwise (no condition, or a type-only/match-all
        // condition) fall back to the events' tags so tagless boundaries do not all collapse onto one hot partition.
        Set<String> conditionBoundaryTags = condition == null ? Set.of() : DcbCloudEvents.boundaryTags(condition.query());
        Set<String> placementTags = conditionBoundaryTags.isEmpty() ? boundaryTagsOf(eventsToAppend) : conditionBoundaryTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");

        // Reserve the position block once, outside the transaction. The counter findAndModify is a single atomic
        // document update that MongoDB serializes without raising a transaction conflict. The reserved block is reused
        // across transient-transaction-error retries; a doomed or condition-failed append abandons it, so dcbposition
        // may have gaps (DCB permits this, see ADR 0021).
        long firstPosition = reserveDcbPositions(eventsToAppend.size());
        long lastPosition = firstPosition + eventsToAppend.size() - 1;

        return executeWithTransientRetry(() -> requireNonNull(transactionTemplate.execute(transactionStatus -> {
            long currentStreamVersion = currentStreamVersion(streamId);
            if (condition != null) {
                enforceAppendCondition(condition, eventsToAppend, lastPosition);
            } else {
                // An unconditional append still increments its events' markers so a concurrent conditional append on an
                // overlapping tag/type shares a marker and serializes against it, and so the conditional append's
                // consistency-token check observes it. Without this, an unconditional append touches no marker, nothing
                // forces a write-write conflict, and a concurrent conditional append's snapshot can miss this append
                // under MongoDB snapshot isolation (write skew). See ADR 0021.
                incrementConflictMarkers(eventMarkerKeys(eventsToAppend), lastPosition);
            }

            List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition);
            insertAllDcb(streamId, currentStreamVersion, documents);
            return new DcbAppendResult(firstPosition, lastPosition, eventsToAppend.size());
        })));
    }

    /**
     * Retry the append transaction on a MongoDB {@code TransientTransactionError} (the error label is present when two
     * transactions conflict, e.g. a write-write conflict on a shared marker). The full cause chain is walked because
     * Spring wraps the {@link MongoException}. {@link DcbAppendConditionNotFulfilledException} is deliberately NOT
     * retried here: it propagates to the application service, which re-reads and retries the whole command.
     */
    private static <T> T executeWithTransientRetry(java.util.function.Supplier<T> action) {
        // Exponential backoff with generous attempts: several appends placed in the same partition stream serialize on
        // stream_version, so the last writer can need to retry past all the others before it commits.
        // Retry a transient transaction conflict, and also a DuplicateKeyException from two transactions first-creating
        // the same conflict marker at once. Event-insert duplicates are translated to domain exceptions in insertAllDcb
        // and so never reach here, so the only DuplicateKeyException at this layer is that cold-marker race. The retry
        // re-runs the transaction, by which point the marker exists and the upsert updates it.
        return RetryStrategy.exponentialBackoff(java.time.Duration.ofMillis(10), java.time.Duration.ofMillis(500), 2.0f)
                .maxAttempts(15)
                .retryIf(throwable -> isTransientTransactionError(throwable) || isDuplicateKeyError(throwable))
                .execute(action);
    }

    private static boolean isTransientTransactionError(Throwable throwable) {
        // Bounded walk so a cyclic cause chain (self-cause or a longer A -> B -> A cycle) cannot spin forever.
        Throwable cause = throwable;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof MongoException mongoException && mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDuplicateKeyError(Throwable throwable) {
        // A cold-marker race can surface the DuplicateKeyException wrapped rather than at the top, so walk the cause
        // chain the same bounded way as the transient-transaction check. Event-insert duplicates are translated to
        // domain exceptions in insertAllDcb and so never reach a retry predicate, so the only duplicate here is the
        // marker race.
        Throwable cause = throwable;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof org.springframework.dao.DuplicateKeyException) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> boundaryTagsOf(List<CloudEvent> events) {
        return events.stream().flatMap(event -> DcbCloudEvents.getTags(event).stream()).collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
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

    private void enforceAppendCondition(DcbAppendCondition condition, List<CloudEvent> eventsToAppend, long lastPosition) {
        Optional<DcbConsistencyToken> expectedToken = condition.consistencyToken();
        final boolean conflict;
        if (expectedToken.isPresent()) {
            // Token-carrying check: the condition carries the consistency token the command observed when it read the
            // query (DcbEventStream.consistencyToken()). If the query's markers have advanced since, an append matching
            // the query committed after the read, so the condition is not fulfilled. Unlike a position-based existence
            // check this is immune to read-watermark overshoot, because marker versions are bumped inside the append
            // transaction (at commit), not when positions are reserved (ADR 0021).
            conflict = consistencyToken(condition.query()) != expectedToken.get().value();
        } else {
            // No token: an absolute "fail if any matching event exists" guard. Check the live events rather than the
            // marker versions, so this means "currently exists" (matching the in-memory store, and surviving deletes and
            // marker pruning) rather than "ever appended". The marker increments below still serialize concurrent
            // unconditional guards on the same boundary so two of them cannot both pass.
            conflict = mongoTemplate.exists(toDcbMongoQuery(condition.query(), 0, Long.MAX_VALUE), eventStoreCollectionName);
        }
        if (conflict) {
            throw new DcbAppendConditionNotFulfilledException(condition, currentDcbPosition(), "Append condition was not fulfilled.");
        }
        // Increment a marker per key for the union of the query's keys and the appended events' keys. The increments
        // force a write-write conflict that serializes concurrent appends sharing a marker, so the loser re-runs this
        // check against the winner's committed increment. Always increment the query's markers so a concurrent matching
        // append is serialized even when this append's own events do not match the query.
        java.util.TreeSet<String> markerKeys = new java.util.TreeSet<>(queryMarkerKeys(condition.query()));
        markerKeys.addAll(eventMarkerKeys(eventsToAppend));
        incrementConflictMarkers(markerKeys, lastPosition);
    }

    // Increment a conflict marker per key. Two appends that can match a common event share at least one marker (per
    // ADR 0021), so the in-transaction increment forces a MongoDB write-write conflict and they serialize. The
    // monotonically increasing version is also the optimistic-concurrency token: a reader snapshots the versions of a
    // query's markers (see consistencyToken), and an append fails if any of them changed since. The stored lastPosition
    // is informational.
    // The marker collection holds one document per distinct tag and per distinct type that has taken part in an append,
    // and nothing reclaims them automatically, so a high-cardinality tag (a tag per entity) grows the collection without
    // bound. An operator can prune markers during quiescence (a later append recreates any it still needs). Automatic
    // reclamation is deferred: safe reclamation needs proof that no in-flight append references a marker, which is its
    // own concurrency problem and out of proportion to the need today.
    private void incrementConflictMarkers(Set<String> markerKeys, long lastPosition) {
        for (String key : markerKeys) {
            Query query = new Query(where(ID).is("marker:" + key));
            Update update = new Update().inc(CHECKPOINT_VERSION, 1L).set(CHECKPOINT_LAST_POSITION, lastPosition);
            mongoTemplate.upsert(query, update, dcbCheckpointCollectionName);
        }
    }

    // The optimistic-concurrency token for a query: the sum of the versions of its conflict markers. The sum is
    // monotonically increasing (every append increments at least one marker by one), so it changes if and only if some
    // append touched at least one of the query's markers since the reader observed it. Because the versions are bumped
    // inside the append transaction (not when positions are reserved), this token reflects only committed appends and is
    // therefore immune to the read-watermark overshoot that a position-based check suffers (ADR 0021).
    private long consistencyToken(DcbQuery query) {
        Set<String> markerKeys = queryMarkerKeys(query);
        if (markerKeys.isEmpty()) {
            return 0;
        }
        // Read the query's markers in one query so their versions come from a single consistent snapshot. Reading them
        // one by one could tear across a concurrent append and capture a sum that matches a later state, masking a real
        // conflict (ADR 31).
        List<String> markerIds = markerKeys.stream().map(key -> "marker:" + key).toList();
        long sum = 0;
        for (Document marker : mongoTemplate.find(new Query(where(ID).in(markerIds)), Document.class, dcbCheckpointCollectionName)) {
            Number version = (Number) marker.get(CHECKPOINT_VERSION);
            if (version != null) {
                sum += version.longValue();
            }
        }
        return sum;
    }

    private static Set<String> queryMarkerKeys(DcbQuery query) {
        if (query instanceof DcbQuery.MatchAll) {
            return Set.of("all");
        }
        // Decompose into one key per attribute (a key per tag, a key per type) and NEVER combine them into a single
        // "type:X+tag:t" key. Skew-safety (ADR 0021) depends on this: a conflicting event shares a marker via whichever
        // single attribute made it match, and a combined key would share nothing with an event that carries only one
        // of the attributes through a different query.
        java.util.TreeSet<String> keys = new java.util.TreeSet<>();
        for (DcbQueryItem item : dcbQueryItems(query)) {
            item.tags().forEach(tag -> keys.add("tag:" + tag));
            item.types().forEach(type -> keys.add("type:" + type));
        }
        return keys;
    }

    // A query here is either a single DcbQueryItem alternative or an Items list (MatchAll is handled by the callers).
    private static List<DcbQueryItem> dcbQueryItems(DcbQuery query) {
        if (query instanceof DcbQueryItem item) {
            return List.of(item);
        }
        return ((DcbQuery.Items) query).items();
    }

    private static Set<String> eventMarkerKeys(List<CloudEvent> events) {
        java.util.TreeSet<String> keys = new java.util.TreeSet<>();
        for (CloudEvent event : events) {
            DcbCloudEvents.getTags(event).forEach(tag -> keys.add("tag:" + tag));
            keys.add("type:" + event.getType());
        }
        return keys;
    }

    /**
     * Reserves a contiguous block of {@code eventCount} DCB positions by incrementing one global counter document. Every
     * DCB append passes through this single document, so it is a serialization point and an inherent throughput ceiling
     * for the store as a whole under very high append rates. It is kept outside the append transaction (ADR 21) so it
     * does not turn into transaction conflicts, but the global monotonic sequence cannot be sharded away.
     */
    private long reserveDcbPositions(int eventCount) {
        // Retry the cold-start race: when the counter document does not exist yet, concurrent upserts all try to insert
        // it and all but one get a duplicate key. On retry the document exists, so the upsert becomes an update.
        return RetryStrategy.retry()
                .backoff(org.occurrent.retry.Backoff.fixed(20))
                .maxAttempts(5)
                .retryIf(SpringMongoEventStore::isDuplicateKeyError)
                .execute(() -> {
                    Query query = new Query(where(ID).is(DCB_POSITION_DOCUMENT_ID));
                    Update update = new Update().inc(DCB_COUNTER_POSITION, eventCount);
                    FindAndModifyOptions options = FindAndModifyOptions.options().upsert(true).returnNew(true);
                    Document updated = mongoTemplate.findAndModify(query, update, options, Document.class, dcbPositionCollectionName);
                    long lastPosition = ((Number) requireNonNull(updated, "DCB position document cannot be null").get(DCB_COUNTER_POSITION)).longValue();
                    return lastPosition - eventCount + 1;
                });
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

    private void insertAllDcb(String streamId, long streamVersion, List<Document> documents) {
        try {
            mongoTemplate.insert(documents, eventStoreCollectionName);
        } catch (DataAccessException e) {
            final Throwable rootCause = e.getRootCause();
            if (rootCause instanceof MongoException mongoException) {
                // A transient transaction conflict (e.g. two disjoint DCB boundaries that hash to the same partition
                // stream racing on stream_version) must stay transient so executeWithTransientRetry retries it, rather
                // than being mapped to the stream-path WriteConditionNotFulfilledException (which DCB does not use).
                if (isTransientTransactionError(mongoException)) {
                    throw mongoException;
                }
                throw translateException(new WriteContext(streamId, streamVersion, WriteCondition.anyStreamVersion()), mongoException);
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
        if (query instanceof DcbQuery.MatchAll) {
            return new Query(positionCriteria);
        }
        List<Criteria> itemCriteria = dcbQueryItems(query).stream()
                .map(SpringMongoEventStore::toCriteria)
                .toList();
        return new Query(new Criteria().andOperator(positionCriteria, new Criteria().orOperator(itemCriteria)));
    }

    private static Criteria toCriteria(DcbQueryItem item) {
        List<Criteria> criteria = new ArrayList<>();
        if (!item.types().isEmpty()) {
            criteria.add(where("type").in(item.types()));
        }
        if (!item.excludedTypes().isEmpty()) {
            criteria.add(where("type").nin(item.excludedTypes()));
        }
        if (!item.tags().isEmpty()) {
            criteria.add(where(DCB_TAGS_INDEX_FIELD).all(item.tags()));
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
