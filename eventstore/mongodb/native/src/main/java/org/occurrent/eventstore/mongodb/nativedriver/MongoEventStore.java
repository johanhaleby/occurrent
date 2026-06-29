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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;
import org.occurrent.eventstore.mongodb.internal.StreamVersionDiff;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.SortBy.*;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.api.WriteCondition.StreamVersionWriteCondition;
import static org.occurrent.eventstore.api.WriteCondition.anyStreamVersion;
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.translateException;
import static org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper.convertToDocument;
import static org.occurrent.functionalsupport.internal.FunctionalSupport.mapWithIndex;

/**
 * This is an {@link EventStore} that stores events in MongoDB using the "native" synchronous java driver MongoDB.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts.
 * <p>
 * By default, only stream-based event-store operations are enabled. Configure
 * {@link EventStoreConfig.Builder#eventStoreCapabilities(Set)} to enable DCB, or to enable both stream and DCB
 * operations. Occurrent creates missing indexes for enabled capabilities, but it never removes indexes automatically.
 * For large production collections, create new indexes out-of-band before enabling a new capability.
 */
@NullMarked
public class MongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore {
    private static final String ID = "_id";
    private static final String NATURAL = "$natural";

    private final MongoDatabase database;
    private final MongoCollection<Document> eventCollection;
    private final MongoCollection<Document> dcbPositionCollection;
    private final MongoCollection<Document> dcbCheckpointCollection;
    private final MongoClient mongoClient;
    private final TimeRepresentation timeRepresentation;
    private final TransactionOptions transactionOptions;
    private final Function<FindIterable<Document>, FindIterable<Document>> queryOptions;
    private final Set<EventStoreCapability> eventStoreCapabilities;
    private final DcbStreamIdGenerator dcbStreamIdGenerator;

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
        this.database = database;
        this.eventCollection = eventCollection;
        String eventCollectionName = eventCollection.getNamespace().getCollectionName();
        this.dcbPositionCollection = database.getCollection(DcbMarkerModel.positionCollectionName(eventCollectionName));
        this.dcbCheckpointCollection = database.getCollection(DcbMarkerModel.checkpointCollectionName(eventCollectionName));
        transactionOptions = config.transactionOptions;
        this.timeRepresentation = config.timeRepresentation;
        this.queryOptions = config.queryOptions;
        this.eventStoreCapabilities = config.eventStoreCapabilities;
        this.dcbStreamIdGenerator = config.dcbStreamIdGenerator;
        initializeEventStore(eventCollection, database, eventStoreCapabilities, dcbPositionCollection.getNamespace().getCollectionName(), dcbCheckpointCollection.getNamespace().getCollectionName());
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        requireStreamCapability();
        EventStream<Document> eventStream = readEventStream(streamId, null, skip, limit);
        return eventStream.map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit) {
        requireStreamCapability();
        requireNonNull(streamId, "Stream id cannot be null");
        requireNonNull(filter, "filter cannot be null");
        EventStream<Document> eventStream = readEventStream(streamId, filter, skip, limit);
        return eventStream.map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
    }

    private EventStreamImpl<Document> readEventStream(String streamId, @Nullable StreamReadFilter streamReadFilter, int skip, int limit) {
        long currentStreamVersion = currentStreamVersion(streamId, null);
        if (currentStreamVersion == 0) {
            return new EventStreamImpl<>(streamId, 0, Stream.empty());
        }

        // We use "lte" currentStreamVersion so that we don't have the start transactions on read. This means that even
        // if another thread has inserted more events after we've read "currentStreamVersion" it doesn't matter.
        Bson query = streamIdAndStreamVersionLessThanOrEqualTo(streamId, currentStreamVersion);
        if (streamReadFilter != null) {
            StreamReadFilterValidator.validate(streamReadFilter);
            Filter mapped = StreamReadFilterToFilterMapper.map(streamReadFilter);
            Bson streamReadBsonFilter = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, mapped);
            query = and(query, streamReadBsonFilter);
        }
        Stream<Document> documentStream = readCloudEvents(query, skip, limit, SortBy.streamVersion(ASCENDING));
        return new EventStreamImpl<>(streamId, currentStreamVersion, documentStream);
    }

    private long currentStreamVersion(String streamId, @Nullable ClientSession clientSession) {
        Bson streamIdFilter = streamIdEqualTo(streamId);
        final FindIterable<Document> documents;
        if (clientSession == null) {
            documents = eventCollection.find(streamIdFilter);
        } else {
            documents = eventCollection.find(clientSession, streamIdFilter);
        }
        final Document documentWithLatestStreamVersion = queryOptions.apply(documents.sort(descending(STREAM_VERSION)).limit(1).projection(Projections.include(STREAM_VERSION))).first();
        final long currentStreamVersion;
        if (documentWithLatestStreamVersion == null) {
            currentStreamVersion = 0;
        } else {
            currentStreamVersion = documentWithLatestStreamVersion.getLong(STREAM_VERSION);
        }
        return currentStreamVersion;
    }

    private Stream<Document> readCloudEvents(Bson query, int skip, int limit, SortBy sortBy) {
        final FindIterable<Document> documentsWithoutSkipAndLimit = eventCollection.find(query);

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }

        Bson sort = convertToMongoDBSort(sortBy);
        final FindIterable<Document> documentsWithSkipAndLimitAndSort;
        if (sort == null) {
            documentsWithSkipAndLimitAndSort = documentsWithoutSkipAndLimit;
        } else {
            documentsWithSkipAndLimitAndSort = documentsWithSkipAndLimit.sort(sort);
        }

        return StreamSupport.stream(queryOptions.apply(documentsWithSkipAndLimitAndSort).spliterator(), false);
    }

    @Override
    public WriteResult write(String streamId, Stream<CloudEvent> events) {
        return write(streamId, anyStreamVersion(), events);
    }

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

        Supplier<WriteResult> writeEvents = () -> {
            try (ClientSession clientSession = mongoClient.startSession()) {
                StreamVersionDiff streamVersionDiff = clientSession.withTransaction(() -> {
                    long currentStreamVersion = currentStreamVersion(streamId, clientSession);

                    if (!isFulfilled(currentStreamVersion, writeCondition)) {
                        throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, currentStreamVersion));
                    }

                    List<Document> cloudEventDocuments = convertCloudEventsToDocuments.apply(events, currentStreamVersion);

                    if (cloudEventDocuments.isEmpty()) {
                        return StreamVersionDiff.of(currentStreamVersion, currentStreamVersion);
                    } else {
                        try {
                            eventCollection.insertMany(clientSession, cloudEventDocuments);
                        } catch (MongoException e) {
                            throw translateException(new WriteContext(streamId, currentStreamVersion, writeCondition), e);
                        }
                        final long newStreamVersion = cloudEventDocuments.get(cloudEventDocuments.size() - 1).getLong(STREAM_VERSION);
                        return StreamVersionDiff.of(currentStreamVersion, newStreamVersion);
                    }
                }, transactionOptions);
                return new WriteResult(streamId, streamVersionDiff.oldStreamVersion, streamVersionDiff.newStreamVersion);
            }
        };

        return RetryStrategy.retry().retryIf(e -> e instanceof WriteConditionNotFulfilledException && writeCondition.isAnyStreamVersion()).execute(writeEvents);
    }

    private List<Document> convertCloudEventsToDocuments(String streamId, Stream<CloudEvent> cloudEvents, long currentStreamVersion) {
        return mapWithIndex(cloudEvents, currentStreamVersion, pair -> convertToDocument(timeRepresentation, streamId, pair.t1, pair.t2)).toList();
    }

    @Override
    public DcbEventStream read(DcbQuery query, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");

        // Snapshot the consistency token BEFORE reading the events. If an append commits between these two reads, the
        // events may include it while the token does not, which only makes a later conditional append over-cautious (a
        // false conflict that retries) rather than miss the conflict.
        long consistencyTokenValue = consistencyToken(null, query);
        long highWatermark = currentDcbPosition();
        long upperBound = Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
        Bson mongoQuery = toDcbBsonQuery(query, options.afterSequencePosition().orElse(0), upperBound);
        FindIterable<Document> documents = eventCollection.find(mongoQuery).sort(ascending(DcbCloudEvents.POSITION));
        List<CloudEvent> events = StreamSupport.stream(queryOptions.apply(documents).spliterator(), false)
                .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document))
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
        return eventCollection.countDocuments(toDcbBsonQuery(query, lowerBound(options), upperBound(options))) > 0;
    }

    @Override
    public long count(DcbQuery query, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return eventCollection.countDocuments(toDcbBsonQuery(query, lowerBound(options), upperBound(options)));
    }

    private long lowerBound(DcbReadOptions options) {
        return options.afterSequencePosition().orElse(0);
    }

    private long upperBound(DcbReadOptions options) {
        long highWatermark = currentDcbPosition();
        return Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
    }

    private DcbAppendResult appendDcb(List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        List<CloudEvent> eventsToAppend = DcbMarkerModel.validateDcbEvents(events);
        // Place by the condition's boundary tags when it constrains on tags, so the same boundary always lands in
        // the same partition regardless of per-event tags. Otherwise (no condition, or a type-only/match-all
        // condition) fall back to the events' tags so tagless boundaries do not all collapse onto one hot partition.
        Set<String> conditionBoundaryTags = condition == null ? Set.of() : DcbCloudEvents.boundaryTags(condition.query());
        Set<String> placementTags = conditionBoundaryTags.isEmpty() ? DcbMarkerModel.boundaryTagsOf(eventsToAppend) : conditionBoundaryTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");

        // Reserve the position block once, outside the transaction. The counter findAndModify is a single atomic
        // document update that MongoDB serializes without raising a transaction conflict. The reserved block is reused
        // across transient-transaction-error retries, a doomed or condition-failed append abandons it, so dcbposition
        // may have gaps (DCB permits this, see ADR 0021).
        long firstPosition = reserveDcbPositions(eventsToAppend.size());
        long lastPosition = firstPosition + eventsToAppend.size() - 1;

        return executeWithTransientRetry(() -> {
            try (ClientSession clientSession = mongoClient.startSession()) {
                return clientSession.withTransaction(() -> {
                    long currentStreamVersion = currentStreamVersion(streamId, clientSession);
                    if (condition != null) {
                        enforceAppendCondition(clientSession, condition, eventsToAppend, lastPosition);
                    } else {
                        // An unconditional append still increments its events' markers so a concurrent conditional append on an
                        // overlapping tag/type shares a marker and serializes against it, and so the conditional append's
                        // consistency-token check observes it. Without this, an unconditional append touches no marker, nothing
                        // forces a write-write conflict, and a concurrent conditional append's snapshot can miss this append
                        // under MongoDB snapshot isolation (write skew). See ADR 0021.
                        incrementConflictMarkers(clientSession, DcbMarkerModel.eventMarkerKeys(eventsToAppend), lastPosition);
                    }

                    List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition);
                    insertAllDcb(clientSession, streamId, currentStreamVersion, documents);
                    return new DcbAppendResult(firstPosition, lastPosition, eventsToAppend.size());
                }, transactionOptions);
            }
        });
    }

    private List<Document> convertDcbCloudEventsToDocuments(String streamId, List<CloudEvent> cloudEvents, long currentStreamVersion, long firstPosition) {
        List<Document> documents = new ArrayList<>(cloudEvents.size());
        long streamVersion = currentStreamVersion + 1;
        long position = firstPosition;
        for (CloudEvent cloudEvent : cloudEvents) {
            CloudEvent dcbCloudEvent = DcbCloudEvents.withPosition(cloudEvent, position);
            documents.add(DcbDocumentMapper.toDocument(timeRepresentation, streamId, streamVersion++, dcbCloudEvent, position));
            position++;
        }
        return documents;
    }

    private void enforceAppendCondition(ClientSession clientSession, DcbAppendCondition condition, List<CloudEvent> eventsToAppend, long lastPosition) {
        Optional<DcbConsistencyToken> expectedToken = condition.consistencyToken();
        final boolean conflict;
        if (expectedToken.isPresent()) {
            // Token-carrying check: the condition carries the consistency token the command observed when it read the
            // query (DcbEventStream.consistencyToken()). If the query's markers have advanced since, an append matching
            // the query committed after the read, so the condition is not fulfilled. Unlike a position-based existence
            // check this is immune to read-watermark overshoot, because marker versions are bumped inside the append
            // transaction (at commit), not when positions are reserved (ADR 0021).
            conflict = consistencyToken(clientSession, condition.query()) != expectedToken.get().value();
        } else {
            // No token: an absolute "fail if any matching event exists" guard. Check the live events rather than the
            // marker versions, so this means "currently exists" (matching the in-memory store, and surviving deletes and
            // marker pruning) rather than "ever appended". The marker increments below still serialize concurrent
            // unconditional guards on the same boundary so two of them cannot both pass.
            conflict = eventCollection.find(clientSession, toDcbBsonQuery(condition.query(), 0, Long.MAX_VALUE)).limit(1).first() != null;
        }
        if (conflict) {
            throw new DcbAppendConditionNotFulfilledException(condition, currentDcbPosition(), "Append condition was not fulfilled.");
        }
        // Increment a marker per key for the union of the query's keys and the appended events' keys. The increments
        // force a write-write conflict that serializes concurrent appends sharing a marker, so the loser re-runs this
        // check against the winner's committed increment. Always increment the query's markers so a concurrent matching
        // append is serialized even when this append's own events do not match the query.
        java.util.TreeSet<String> markerKeys = new java.util.TreeSet<>(DcbMarkerModel.queryMarkerKeys(condition.query()));
        markerKeys.addAll(DcbMarkerModel.eventMarkerKeys(eventsToAppend));
        incrementConflictMarkers(clientSession, markerKeys, lastPosition);
    }

    // Increment a conflict marker per key. Two appends that can match a common event share at least one marker (per
    // ADR 0021), so the in-transaction increment forces a MongoDB write-write conflict and they serialize. The
    // monotonically increasing version is also the optimistic-concurrency token: a reader snapshots the versions of a
    // query's markers (see consistencyToken), and an append fails if any of them changed since. The stored lastPosition
    // is informational.
    // The marker collection holds one document per distinct tag and per distinct type that has taken part in an append,
    // and nothing reclaims them automatically, so a high-cardinality tag (a tag per entity) grows the collection without
    // bound. An operator can prune markers during quiescence (a later append recreates any it still needs).
    private void incrementConflictMarkers(ClientSession clientSession, Set<String> markerKeys, long lastPosition) {
        for (String key : markerKeys) {
            dcbCheckpointCollection.updateOne(clientSession, eq(ID, DcbMarkerModel.markerId(key)),
                    Updates.combine(Updates.inc(DcbMarkerModel.CHECKPOINT_VERSION, 1L), Updates.set(DcbMarkerModel.CHECKPOINT_LAST_POSITION, lastPosition)),
                    new UpdateOptions().upsert(true));
        }
    }

    // The optimistic-concurrency token for a query: the sum of the versions of its conflict markers. The sum is
    // monotonically increasing (every append increments at least one marker by one), so it changes if and only if some
    // append touched at least one of the query's markers since the reader observed it. Because the versions are bumped
    // inside the append transaction (not when positions are reserved), this token reflects only committed appends and is
    // therefore immune to the read-watermark overshoot that a position-based check suffers (ADR 0021).
    private long consistencyToken(@Nullable ClientSession clientSession, DcbQuery query) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(query);
        if (markerKeys.isEmpty()) {
            return 0;
        }
        // Read the query's markers in one query so their versions come from a single consistent snapshot. Reading them
        // one by one could tear across a concurrent append and capture a sum that matches a later state, masking a real
        // conflict (ADR 31).
        List<String> markerIds = markerKeys.stream().map(DcbMarkerModel::markerId).toList();
        Bson markerFilter = in(ID, markerIds);
        FindIterable<Document> markers = clientSession == null ? dcbCheckpointCollection.find(markerFilter) : dcbCheckpointCollection.find(clientSession, markerFilter);
        long sum = 0;
        for (Document marker : markers) {
            Number version = (Number) marker.get(DcbMarkerModel.CHECKPOINT_VERSION);
            if (version != null) {
                sum += version.longValue();
            }
        }
        return sum;
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
                .backoff(Backoff.fixed(20))
                .maxAttempts(5)
                .retryIf(MongoEventStore::isDuplicateKeyError)
                .execute(() -> {
                    Document updated = dcbPositionCollection.findOneAndUpdate(
                            eq(ID, DcbMarkerModel.DCB_POSITION_DOCUMENT_ID),
                            Updates.inc(DcbMarkerModel.DCB_COUNTER_POSITION, (long) eventCount),
                            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
                    long lastPosition = ((Number) requireNonNull(updated, "DCB position document cannot be null").get(DcbMarkerModel.DCB_COUNTER_POSITION)).longValue();
                    return lastPosition - eventCount + 1;
                });
    }

    private long currentDcbPosition() {
        Document document = dcbPositionCollection.find(eq(ID, DcbMarkerModel.DCB_POSITION_DOCUMENT_ID)).first();
        return document == null ? 0 : ((Number) document.get(DcbMarkerModel.DCB_COUNTER_POSITION)).longValue();
    }

    private void insertAllDcb(ClientSession clientSession, String streamId, long streamVersion, List<Document> documents) {
        try {
            eventCollection.insertMany(clientSession, documents);
        } catch (MongoException e) {
            // A transient transaction conflict (e.g. two disjoint DCB boundaries that hash to the same partition
            // stream racing on stream_version) must stay transient so executeWithTransientRetry retries it, rather
            // than being mapped to the stream-path WriteConditionNotFulfilledException (which DCB does not use).
            if (isTransientTransactionError(e)) {
                throw e;
            }
            throw translateException(new WriteContext(streamId, streamVersion, anyStreamVersion()), e);
        }
    }

    // Retry the append transaction on a MongoDB TransientTransactionError (the error label is present when two
    // transactions conflict, e.g. a write-write conflict on a shared marker). The native driver's withTransaction
    // already retries a transient transaction error on its own, so at this layer the retry mainly covers the
    // cold-marker DuplicateKeyException race where two transactions first-create the same marker at once.
    // DcbAppendConditionNotFulfilledException is deliberately NOT retried here: it propagates to the application
    // service, which re-reads and retries the whole command.
    private static <T> T executeWithTransientRetry(Supplier<T> action) {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(10), Duration.ofMillis(500), 2.0f)
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
        // A cold-marker race can surface the duplicate key wrapped rather than at the top, so walk the cause chain the
        // same bounded way as the transient-transaction check. A DuplicateCloudEventException is a genuine business
        // error (an event whose id and source already exist), not the cold-start race, and the driver duplicate-key
        // exception is its cause, so stop and do not retry once it appears in the chain.
        Throwable cause = throwable;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof DuplicateCloudEventException) {
                return false;
            }
            if (cause instanceof MongoBulkWriteException bulkWriteException) {
                boolean duplicate = bulkWriteException.getWriteErrors().stream()
                        .anyMatch(writeError -> ErrorCategory.fromErrorCode(writeError.getCode()) == ErrorCategory.DUPLICATE_KEY);
                if (duplicate) {
                    return true;
                }
            } else if (cause instanceof MongoException mongoException && mongoException.getCode() == 11000) {
                return true;
            }
        }
        return false;
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

    @Override
    public boolean exists(String streamId) {
        requireStreamCapability();
        return eventCollection.countDocuments(eq(STREAM_ID, streamId)) > 0;
    }

    @Override
    public void deleteEventStream(String streamId) {
        requireStreamCapability();
        eventCollection.deleteMany(eq(STREAM_ID, streamId));
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        requireStreamCapability();
        eventCollection.deleteOne(uniqueCloudEvent(cloudEventId, cloudEventSource));
    }

    @Override
    public void delete(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        final Bson bson = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
        eventCollection.deleteMany(bson);
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        requireStreamCapability();
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
            CloudEvent currentCloudEvent = DcbDocumentMapper.toCloudEvent(timeRepresentation, document);
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
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        final Bson query = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
        return readCloudEvents(query, skip, limit, sortBy)
                .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
    }

    @Override
    public long count(Filter filter) {
        requireStreamCapability();
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
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        return count(filter) > 0;
    }

    private record EventStreamImpl<T>(String id, long version, Stream<T> events) implements EventStream<T> {
    }

    private static void initializeEventStore(MongoCollection<Document> eventStoreCollection, MongoDatabase mongoDatabase, Set<EventStoreCapability> eventStoreCapabilities, String dcbPositionCollectionName, String dcbCheckpointCollectionName) {
        String eventStoreCollectionName = eventStoreCollection.getNamespace().getCollectionName();
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        if (dcbEnabled && !collectionExists(mongoDatabase, dcbPositionCollectionName)) {
            mongoDatabase.createCollection(dcbPositionCollectionName);
        }
        if (dcbEnabled && !collectionExists(mongoDatabase, dcbCheckpointCollectionName)) {
            mongoDatabase.createCollection(dcbCheckpointCollectionName);
        }

        // Cloud spec defines id + source must be unique!
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        if (eventStoreCapabilities.contains(STREAM)) {
            // Create a streamId + streamVersion ascending index (note that we don't need to index stream id separately since it's covered by this compound index)
            // Note also that this index supports when sorting both ascending and descending since MongoDB can traverse an index in both directions.
            eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.ascending(STREAM_VERSION)), new IndexOptions().unique(true));
        }
        if (dcbEnabled) {
            eventStoreCollection.createIndex(Indexes.ascending(DcbCloudEvents.POSITION), new IndexOptions().unique(true).sparse(true));
            eventStoreCollection.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD));
        }
    }

    private static boolean collectionExists(MongoDatabase mongoDatabase, String collectionName) {
        for (String listCollectionName : mongoDatabase.listCollectionNames()) {
            if (listCollectionName.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private void requireStreamCapability() {
        requireCapability(STREAM);
    }

    private void requireDcbCapability() {
        requireCapability(DCB);
    }

    private void requireCapability(EventStoreCapability capability) {
        if (!eventStoreCapabilities.contains(capability)) {
            throw new UnsupportedOperationException(capability + " capability is not enabled for this MongoEventStore");
        }
    }

    private static Bson streamIdEqualTo(String streamId) {
        return eq(STREAM_ID, streamId);
    }

    private static Bson streamIdAndStreamVersionLessThanOrEqualTo(String streamId, long version) {
        return and(streamIdEqualTo(streamId), lte(STREAM_VERSION, version));
    }

    private static Bson toDcbBsonQuery(DcbQuery query, long afterSequencePosition, long upperSequencePosition) {
        Bson positionFilter = and(gt(DcbCloudEvents.POSITION, afterSequencePosition), lte(DcbCloudEvents.POSITION, upperSequencePosition));
        if (query instanceof DcbQuery.MatchAll) {
            return positionFilter;
        }
        List<Bson> itemFilters = DcbMarkerModel.dcbQueryItems(query).stream()
                .map(MongoEventStore::toBsonFilter)
                .toList();
        return and(positionFilter, or(itemFilters));
    }

    private static Bson toBsonFilter(DcbQueryItem item) {
        List<Bson> filters = new ArrayList<>();
        if (!item.types().isEmpty()) {
            filters.add(in("type", item.types()));
        }
        if (!item.excludedTypes().isEmpty()) {
            filters.add(nin("type", item.excludedTypes()));
        }
        if (!item.tags().isEmpty()) {
            filters.add(com.mongodb.client.model.Filters.all(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, item.tags()));
        }
        return and(filters);
    }

    private static Bson uniqueCloudEvent(String cloudEventId, URI cloudEventSource) {
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");
        return and(eq("id", cloudEventId), eq("source", cloudEventSource.toString()));
    }

    @Nullable
    private static Bson convertToMongoDBSort(SortBy sortBy) {
        final Bson sort;
        if (sortBy instanceof Unsorted) {
            sort = null;
        } else if (sortBy instanceof NaturalImpl) {
            sort = ((NaturalImpl) sortBy).direction == ASCENDING ? ascending(NATURAL) : descending(NATURAL);
        } else if (sortBy instanceof SingleFieldImpl singleField) {
            sort = singleField.direction == ASCENDING ? ascending(singleField.fieldName) : descending(singleField.fieldName);
        } else if (sortBy instanceof MultipleSortStepsImpl) {
            sort = ((MultipleSortStepsImpl) sortBy).steps.stream()
                    .map(MongoEventStore::convertToMongoDBSort)
                    .reduce(Sorts::orderBy)
                    .orElseThrow(() -> new IllegalStateException("Internal error: Expecting " + MultipleSortStepsImpl.class.getSimpleName() + " to have at least one step"));
        } else {
            throw new IllegalArgumentException("Internal error: Unrecognized " + SortBy.class.getSimpleName() + " instance: " + sortBy.getClass().getSimpleName());
        }
        return sort;
    }
}
