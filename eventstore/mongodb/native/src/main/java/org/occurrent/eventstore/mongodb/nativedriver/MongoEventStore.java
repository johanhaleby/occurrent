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

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.condition.Condition;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.internal.DcbCatchupReads;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.eventstore.api.internal.PositionBackfillValidator;
import org.occurrent.eventstore.api.internal.UpdateEventRepairValidator;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.api.internal.UpdateEventFunctionValidator;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerVersionsDocuments;
import org.occurrent.eventstore.mongodb.dcb.internal.MarkerVersionsReadFailures;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbTagsIndexCheck;
import org.occurrent.eventstore.mongodb.dcb.internal.PositionDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.UpdateEventDamage;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.eventstore.mongodb.internal.StreamVersionDiff;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterbsonfilterconversion.internal.FilterToBsonFilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.LongSupplier;
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
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.isDuplicateKeyErrorOnStreamVersionIndex;
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
 * <p>
 * A DCB read that asks for marker versions runs in a read-only snapshot transaction of its own, bounded by
 * {@link EventStoreConfig.Builder#markerVersionsCommitTimeout(Duration)}. Inside a session the caller bound with
 * {@link ClientSessionHolder} it reports no marker versions, since that transaction's writes may still abort, and a read
 * after marker versions is refused. The store answers {@link DcbAppendConfirmation} with a majority read on the
 * primary that joins no session.
 */
@NullMarked
public class MongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore, PositionOrderedReader, DcbAppendConfirmation, CatchupReads, DcbCatchupReads {
    private static final Logger log = LoggerFactory.getLogger(MongoEventStore.class);
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
    private final boolean streamPositionEnabled;
    private final boolean requireBackfilledPosition;
    private final boolean resumeFromMarkerVersions;
    private final TransactionOptions markerVersionsTransactionOptions;
    private final MarkerVersionsReadFailures markerVersionsReadFailures = new MarkerVersionsReadFailures();

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
        // Resolve the effective stream-position setting before the store is initialized, so the position index and
        // counter collection are not created when the startup guard disables position for this store.
        this.streamPositionEnabled = resolveStreamPositionEnabled(config, eventCollection);
        this.requireBackfilledPosition = config.requireBackfilledPosition;
        this.resumeFromMarkerVersions = config.resumeFromMarkerVersions;
        this.markerVersionsTransactionOptions = DcbMarkerVersionsDocuments.readOnlySnapshotTransactionOptions(config.markerVersionsCommitTimeout);
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        if (dcbEnabled && !resumeFromMarkerVersions) {
            log.warn("resumeFromMarkerVersions is off for this event store, so DCB reads report no marker versions and DCB reads after marker versions are refused.");
        }
        initializeEventStore(eventCollection, database, eventStoreCapabilities, writesPosition(), dcbPositionCollection.getNamespace().getCollectionName(), dcbCheckpointCollection.getNamespace().getCollectionName());
        // Before the unpositioned check, which throws when requireBackfilledPosition is set. An event whose
        // position updateEvent dropped has no position field either, so that check would fail startup
        // naming the position backfill, and backfilling such an event assigns a wrong position for good.
        // requireRepairedEvents runs the damage check on a store that writes no position too, since the two ways
        // that happens are withoutStreamPosition() and the resolver turning position off over unpositioned
        // history, and an operator who asked to be refused meant both.
        if (writesPosition() || config.requireRepairedEvents) {
            warnOrFailOnEventsDamagedByUpdateEvent(eventCollection, dcbPositionCollection, config.requireRepairedEvents);
        }
        if (writesPosition()) {
            warnOrFailOnUnpositionedEvents(eventCollection, requireBackfilledPosition);
        }
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
        if (skip < 0) {
            throw new IllegalArgumentException("skip cannot be negative");
        }
        // Join the transaction an external executor opened on this thread, if any, so a read of the just-written
        // (still uncommitted) tail issued from inside that transaction sees the events. Without an ambient session
        // this reads non-transactionally, exactly as before.
        long currentStreamVersion = currentStreamVersion(streamId, ClientSessionHolder.get());
        if (currentStreamVersion == 0) {
            return new EventStreamImpl<>(streamId, 0, Stream.empty());
        }

        // Uses "lte" currentStreamVersion instead of a transaction on read, so an event another thread inserts after
        // currentStreamVersion is read does not matter. "skip" is folded into the version bound here, before the
        // filter narrows the query, so it keeps counting stream positions instead of filtered documents.
        Bson query = streamIdAndStreamVersionBetween(streamId, skip, currentStreamVersion);
        if (streamReadFilter != null) {
            StreamReadFilterValidator.validate(streamReadFilter);
            Filter mapped = StreamReadFilterToFilterMapper.map(streamReadFilter);
            Bson streamReadBsonFilter = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, mapped);
            query = and(query, streamReadBsonFilter);
        }
        Stream<Document> documentStream = readCloudEvents(query, 0, limit, SortBy.streamVersion(ASCENDING));
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

    // Find events, joining the transaction an external executor opened on this thread when one is bound (see
    // ClientSessionHolder). With no ambient session this is a plain, non-transactional find, exactly as before.
    private FindIterable<Document> findEvents(Bson query) {
        ClientSession ambientSession = ClientSessionHolder.get();
        return ambientSession == null ? eventCollection.find(query) : eventCollection.find(ambientSession, query);
    }

    // Count through the ambient ClientSession when one is bound (see findEvents), so a read issued from within a
    // synchronous subscription handler observes the uncommitted write. With no ambient session it is a plain,
    // non-transactional count, exactly as before.
    private long countEvents(Bson query) {
        ClientSession ambientSession = ClientSessionHolder.get();
        return ambientSession == null ? eventCollection.countDocuments(query) : eventCollection.countDocuments(ambientSession, query);
    }

    // Existence through the ambient ClientSession when one is bound (see findEvents). A find limited to one document
    // stops at the first match, where countDocuments examines every matching document.
    private boolean anyEventMatches(Bson query) {
        return queryOptions.apply(findEvents(query).projection(Projections.include(ID)).limit(1)).first() != null;
    }

    private Stream<Document> readCloudEvents(Bson query, int skip, int limit, SortBy sortBy) {
        return readCloudEvents(query, skip, limit, sortBy, this::findEvents);
    }

    // The find is passed in so a catch-up read scope can read the same way through its own session
    private Stream<Document> readCloudEvents(Bson query, int skip, int limit, SortBy sortBy, Function<Bson, FindIterable<Document>> find) {
        final FindIterable<Document> documentsWithoutSkipAndLimit = find.apply(query);

        final FindIterable<Document> documentsWithSkipAndLimit;
        if (skip != 0 || limit != Integer.MAX_VALUE) {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit.skip(skip).limit(limit);
        } else {
            documentsWithSkipAndLimit = documentsWithoutSkipAndLimit;
        }

        Bson sort = convertToMongoDBSort(sortBy);
        final FindIterable<Document> documentsWithSkipAndLimitAndSort;
        if (sort == null) {
            documentsWithSkipAndLimitAndSort = documentsWithSkipAndLimit;
        } else {
            documentsWithSkipAndLimitAndSort = documentsWithSkipAndLimit.sort(sort);
        }

        // Built from the FindIterable's own cursor, with onClose wired to it, so closing the returned Stream (a
        // Kotlin `.use { }` included) releases the MongoCursor instead of leaving it open server-side.
        MongoCursor<Document> cursor = queryOptions.apply(documentsWithSkipAndLimitAndSort).iterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor, Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(cursor::close);
    }

    @Override
    public WriteResult write(String streamId, List<CloudEvent> events) {
        return write(streamId, anyStreamVersion(), events);
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, List<CloudEvent> events) {
        requireStreamCapability();
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        rejectDcbTaggedEvents(events);

        // Reserve the position block outside the transaction, like DCB does (see reservePositions), so the shared
        // counter does not become a transaction write-write conflict. The block is reused across retries, and a write
        // that never commits abandons it, so positions may have gaps. Reserve only when the store writes position and
        // there is at least one event.
        final long firstReservedPosition;
        if (streamPositionEnabled && !events.isEmpty()) {
            firstReservedPosition = reservePositions(events.size());
        } else {
            firstReservedPosition = 0;
        }
        // Minted once, outside the retry, and reused across attempts like the reserved position block. Absent when
        // there is nothing to stamp it on, so a call that persists no events reports no append id (ADR 132, decision 4).
        final Optional<AppendId> appendId = events.isEmpty() ? Optional.empty() : Optional.of(AppendId.mint());

        ClientSession ambientSession = ClientSessionHolder.get();
        if (ambientSession != null) {
            // Join the transaction an external executor opened on this thread. The executor owns the session and its
            // commit/abort, so run the write body once directly on the ambient session, without opening a session,
            // starting a transaction, or retrying here.
            StreamVersionDiff streamVersionDiff = writeInSession(ambientSession, streamId, writeCondition, events, firstReservedPosition, appendId);
            return new WriteResult(streamId, streamVersionDiff.oldStreamVersion, streamVersionDiff.newStreamVersion, appendId);
        }

        Supplier<WriteResult> writeEvents = () -> {
            try (ClientSession clientSession = mongoClient.startSession()) {
                StreamVersionDiff streamVersionDiff = clientSession.withTransaction(() -> writeInSession(clientSession, streamId, writeCondition, events, firstReservedPosition, appendId), transactionOptions);
                return new WriteResult(streamId, streamVersionDiff.oldStreamVersion, streamVersionDiff.newStreamVersion, appendId);
            }
        };

        return RetryStrategy.retry().retryIf(e -> e instanceof WriteConditionNotFulfilledException && writeCondition.isAnyStreamVersion()).execute(writeEvents);
    }

    private StreamVersionDiff writeInSession(ClientSession clientSession, String streamId, WriteCondition writeCondition, List<CloudEvent> events, long firstReservedPosition, Optional<AppendId> appendId) {
        long currentStreamVersion = currentStreamVersion(streamId, clientSession);

        if (!isFulfilled(currentStreamVersion, writeCondition)) {
            throw new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition);
        }

        List<Document> cloudEventDocuments = convertCloudEventsToDocuments(streamId, events.stream(), currentStreamVersion, firstReservedPosition, appendId);

        if (cloudEventDocuments.isEmpty()) {
            return StreamVersionDiff.of(currentStreamVersion, currentStreamVersion);
        } else {
            try {
                eventCollection.insertMany(clientSession, cloudEventDocuments);
            } catch (MongoException e) {
                throw translateException(new WriteContext(streamId, currentStreamVersion, writeCondition), e);
            }
            final long newStreamVersion = cloudEventDocuments.getLast().getLong(STREAM_VERSION);
            return StreamVersionDiff.of(currentStreamVersion, newStreamVersion);
        }
    }

    private List<Document> convertCloudEventsToDocuments(String streamId, Stream<CloudEvent> cloudEvents, long currentStreamVersion, long firstReservedPosition, Optional<AppendId> appendId) {
        List<Document> documents = mapWithIndex(cloudEvents, currentStreamVersion, pair -> convertToDocument(timeRepresentation, streamId, pair.t1, pair.t2)).toList();
        if (streamPositionEnabled && !documents.isEmpty()) {
            // Stamp the positions reserved outside the transaction (see write(...)). They may have gaps, like the DCB
            // write path.
            long position = firstReservedPosition;
            for (Document document : documents) {
                PositionDocumentMapper.addPosition(document, position);
                position++;
            }
        }
        if (appendId.isPresent()) {
            String appendIdValue = appendId.get().toString();
            for (Document document : documents) {
                document.put(OccurrentCloudEventExtension.APPEND_ID, appendIdValue);
            }
        }
        return documents;
    }

    @Override
    public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return options.versionsRequested() ? readWithMarkerVersions(criteria, options) : readWithoutMarkerVersions(criteria, options);
    }

    private DcbEventStream readWithoutMarkerVersions(DcbCriteria criteria, DcbReadOptions options) {
        return readWithoutMarkerVersions(criteria, options, () -> consistencyToken(null, criteria), this::currentPosition, this::findEvents);
    }

    // The token, the head and the find are passed in so a catch-up read scope can read all three through its own session
    private DcbEventStream readWithoutMarkerVersions(DcbCriteria criteria, DcbReadOptions options, LongSupplier consistencyToken, LongSupplier currentPosition, Function<Bson, FindIterable<Document>> find) {
        // Snapshot the consistency token BEFORE reading the events. If an append commits between these two reads, the
        // events may include it while the token does not, which only makes a later conditional append over-cautious (a
        // false conflict that retries) rather than miss the conflict.
        long consistencyTokenValue = consistencyToken.getAsLong();
        long highWatermark = currentPosition.getAsLong();
        long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
        Bson mongoQuery = toDcbBsonQuery(criteria, options.afterPosition().orElse(0), upperBound);

        boolean fetchDescending = options.direction() == DcbReadOptions.Direction.BACKWARD;
        FindIterable<Document> documents = find.apply(mongoQuery)
                .sort(fetchDescending ? descending(OccurrentCloudEventExtension.POSITION) : ascending(OccurrentCloudEventExtension.POSITION));
        if (options.skip() > 0) {
            documents = documents.skip(options.skip());
        }
        if (options.limit().isPresent()) {
            documents = documents.limit(options.limit().getAsInt());
        }
        List<CloudEvent> events = StreamSupport.stream(queryOptions.apply(documents).spliterator(), false)
                .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document))
                .toList();
        if (fetchDescending) {
            events = new ArrayList<>(events);
            Collections.reverse(events);
        }
        return new DcbEventStream(events, highWatermark, DcbConsistencyToken.of(consistencyTokenValue));
    }

    // Markers, counter and events come from one point in time, so the reported versions cover exactly the events read
    // and a read after them misses none. This runs in a read-only snapshot transaction of the store's own, never with
    // the transaction options configured for writes, and commits it once by hand. The majority commit makes sure the
    // snapshot was majority-committed. Inside a caller's session it reports no versions, since the session's own
    // increments may still abort and a later append would reuse them. A failure reports no versions instead of failing
    // the read. A read after versions can't fall back to a full read without double-applying events, so it is refused
    // in both cases.
    private DcbEventStream readWithMarkerVersions(DcbCriteria criteria, DcbReadOptions options) {
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        if (!resumeFromMarkerVersions) {
            if (afterVersions.isPresent()) {
                throw new DcbSnapshotInvalidException(criteria, afterVersions.get(), "Resuming from marker versions is turned off for this event store (resumeFromMarkerVersions)");
            }
            return readWithoutMarkerVersions(criteria, options);
        }
        if (criteria instanceof DcbCriteria.MatchAll) {
            afterVersions.ifPresent(saved -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, Map.of()));
            return readWithoutMarkerVersions(criteria, options);
        }

        ClientSession ambientSession = ClientSessionHolder.get();
        if (ambientSession != null) {
            return markerVersionsUnavailable(criteria, options, "The read runs inside a session or transaction the caller owns, whose writes may still abort", null);
        }

        final ClientSession clientSession;
        try {
            clientSession = mongoClient.startSession();
        } catch (RuntimeException e) {
            return markerVersionsReadFailed(criteria, options, "start session", e);
        }
        String failedPhase = "read";
        RuntimeException failure = null;
        try (clientSession) {
            DcbEventStream eventStream = null;
            try {
                clientSession.startTransaction(markerVersionsTransactionOptions);
                eventStream = readMarkerVersionsInSession(clientSession, criteria, options);
            } catch (DcbSnapshotInvalidException e) {
                // Closing the session aborts the read-only transaction
                throw e;
            } catch (RuntimeException e) {
                failure = e;
            }
            if (eventStream != null) {
                try {
                    clientSession.commitTransaction();
                    return eventStream;
                } catch (RuntimeException e) {
                    // A failed commit leaves the session in the committed state, so closing it sends no abort
                    failedPhase = "commit";
                    failure = e;
                }
            }
        }
        return markerVersionsReadFailed(criteria, options, failedPhase, requireNonNull(failure));
    }

    private DcbEventStream readMarkerVersionsInSession(ClientSession clientSession, DcbCriteria criteria, DcbReadOptions options) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(criteria);
        Map<String, Document> markers = DcbMarkerVersionsDocuments.markersByKey(dcbCheckpointCollection.find(clientSession, DcbMarkerVersionsDocuments.markersFilter(markerKeys)));
        Map<String, DcbMarkerVersionsSupport.ObservedMarker> observed = DcbMarkerVersionsDocuments.observe(markers);
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        afterVersions.ifPresent(saved -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed));
        long consistencyTokenValue = DcbMarkerVersionsDocuments.consistencyToken(markerKeys, markers);

        long highWatermark = currentPosition(clientSession);
        long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
        Bson mongoQuery = afterVersions
                .map(saved -> toDcbBsonQuery(criteria, 0, upperBound, item -> DcbMarkerVersionsDocuments.afterVersionsFilter(item, markers, saved)))
                .orElseGet(() -> toDcbBsonQuery(criteria, options.afterPosition().orElse(0), upperBound));

        boolean fetchDescending = options.direction() == DcbReadOptions.Direction.BACKWARD;
        FindIterable<Document> documents = eventCollection.find(clientSession, mongoQuery)
                .sort(fetchDescending ? descending(OccurrentCloudEventExtension.POSITION) : ascending(OccurrentCloudEventExtension.POSITION));
        if (options.skip() > 0) {
            documents = documents.skip(options.skip());
        }
        if (options.limit().isPresent()) {
            documents = documents.limit(options.limit().getAsInt());
        }
        List<CloudEvent> events = StreamSupport.stream(queryOptions.apply(documents).spliterator(), false)
                .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document))
                .toList();
        if (fetchDescending) {
            events = new ArrayList<>(events);
            Collections.reverse(events);
        }
        Optional<DcbMarkerVersions> markerVersions = DcbMarkerVersionsSupport.mayReportVersions(options) ? DcbMarkerVersionsSupport.versionsOf(markerKeys, observed) : Optional.empty();
        return new DcbEventStream(events, highWatermark, DcbConsistencyToken.of(consistencyTokenValue), markerVersions);
    }

    private DcbEventStream markerVersionsReadFailed(DcbCriteria criteria, DcbReadOptions options, String phase, RuntimeException cause) {
        if (markerVersionsReadFailures.recordAndCheckFirst(phase, cause)) {
            log.warn(MarkerVersionsReadFailures.message(phase, cause, options.afterVersions().isPresent()), cause);
        } else if (log.isDebugEnabled()) {
            log.debug(MarkerVersionsReadFailures.message(phase, cause, options.afterVersions().isPresent()), cause);
        }
        return markerVersionsUnavailable(criteria, options, "The event store could not take a consistent snapshot to compare the saved marker versions against (" + phase + " failed)", cause);
    }

    private DcbEventStream markerVersionsUnavailable(DcbCriteria criteria, DcbReadOptions options, String reason, @Nullable Throwable cause) {
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        if (afterVersions.isPresent()) {
            throw new DcbSnapshotInvalidException(criteria, afterVersions.get(), reason, cause);
        }
        return readWithoutMarkerVersions(criteria, options);
    }

    @Override
    public boolean isConfirmed(DcbAppendResult appendResult) {
        requireNonNull(appendResult, DcbAppendResult.class.getSimpleName() + " cannot be null");
        Optional<AppendId> appendId = appendResult.appendId();
        if (appendId.isEmpty()) {
            return false;
        }
        try {
            Bson filter = and(eq(OccurrentCloudEventExtension.APPEND_ID, appendId.get().toString()),
                    gte(OccurrentCloudEventExtension.POSITION, appendResult.firstSequencePosition()),
                    lte(OccurrentCloudEventExtension.POSITION, appendResult.lastSequencePosition()));
            return eventCollection.withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                    .find(filter).projection(Projections.include(ID)).limit(1).first() != null;
        } catch (RuntimeException e) {
            log.debug("Could not confirm DCB append {}, so it counts as unconfirmed", appendId.get(), e);
            return false;
        }
    }

    /**
     * Reads events matching {@code filter} in position order, within {@code range}, clamped to the store's highest
     * written position at read time. The upper bound is the lesser of the range's {@code upToPosition} and that
     * position, so a concurrent append is never partially visible.
     *
     * @throws UnsupportedOperationException if this store does not write a position ({@link #writesPosition()} is
     *                                        {@code false}).
     */
    @Override
    public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        requirePosition();
        return readInPositionOrder(filter, range, this::currentPosition, this::findEvents);
    }

    // The head and the find are passed in so a catch-up read scope can read both through its own session
    private Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range, LongSupplier currentPosition, Function<Bson, FindIterable<Document>> find) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(range, "Range cannot be null");

        long highWatermark = currentPosition.getAsLong();
        long lowerBound = range.afterPosition().orElse(0);
        long upperBound = Math.min(highWatermark, range.upToPosition().orElse(highWatermark));

        Bson positionFilter = and(gt(OccurrentCloudEventExtension.POSITION, lowerBound), lte(OccurrentCloudEventExtension.POSITION, upperBound));
        Bson filterBson = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
        Bson query = and(positionFilter, filterBson);

        FindIterable<Document> documents = find.apply(query).sort(ascending(OccurrentCloudEventExtension.POSITION));
        return StreamSupport.stream(queryOptions.apply(documents).spliterator(), false)
                .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
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
    public boolean exists(DcbCriteria criteria, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return anyEventMatches(toDcbBsonQuery(criteria, lowerBound(options), upperBound(options)));
    }

    @Override
    public long count(DcbCriteria criteria, DcbReadOptions options) {
        requireDcbCapability();
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return countEvents(toDcbBsonQuery(criteria, lowerBound(options), upperBound(options)));
    }

    private long lowerBound(DcbReadOptions options) {
        return options.afterPosition().orElse(0);
    }

    private long upperBound(DcbReadOptions options) {
        long highWatermark = currentPosition();
        return Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
    }

    private DcbAppendResult appendDcb(List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        List<CloudEvent> eventsToAppend = DcbMarkerModel.validateDcbEvents(events);
        // Place by the condition's boundary tags when it constrains tags, so the same boundary always lands
        // in the same partition regardless of per-event tags. Otherwise fall back to the events' tags, so
        // tagless boundaries do not all collapse onto one hot partition.
        Set<Tag> conditionTags = condition == null ? Set.of() : DcbCloudEvents.tagsOf(condition.criteria());
        Set<Tag> placementTags = conditionTags.isEmpty() ? DcbMarkerModel.tagsOf(eventsToAppend) : conditionTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");

        // Reserve the position block once, outside the transaction. The counter findAndModify is a single atomic
        // document update that MongoDB serializes without raising a transaction conflict. The reserved block is reused
        // across transient-transaction-error retries, a doomed or condition-failed append abandons it, so position
        // may have gaps (DCB permits this, see ADR 0021).
        long firstPosition = reservePositions(eventsToAppend.size());
        long lastPosition = firstPosition + eventsToAppend.size() - 1;
        // A DCB append always persists at least one event (validateDcbEvents above refuses an empty list), so this
        // is minted unconditionally, unlike the stream write path.
        AppendId appendId = AppendId.mint();

        ClientSession ambientSession = ClientSessionHolder.get();
        if (ambientSession != null) {
            // Join the transaction an external executor opened on this thread. The executor owns the session and its
            // commit/abort, so run the append body once directly on the ambient session, without opening a session,
            // starting a transaction, or retrying here.
            return appendInSession(ambientSession, streamId, eventsToAppend, condition, firstPosition, lastPosition, appendId);
        }

        return executeWithTransientRetry(() -> {
            try (ClientSession clientSession = mongoClient.startSession()) {
                return clientSession.withTransaction(() -> appendInSession(clientSession, streamId, eventsToAppend, condition, firstPosition, lastPosition, appendId), transactionOptions);
            }
        });
    }

    private DcbAppendResult appendInSession(ClientSession clientSession, String streamId, List<CloudEvent> eventsToAppend, @Nullable DcbAppendCondition condition, long firstPosition, long lastPosition, AppendId appendId) {
        long currentStreamVersion = currentStreamVersion(streamId, clientSession);
        final Map<String, Document> markers;
        if (condition != null) {
            markers = enforceAppendCondition(clientSession, condition, eventsToAppend, lastPosition);
        } else {
            // An unconditional append still increments its events' markers, so a concurrent conditional append
            // on an overlapping tag or type shares a marker, serializes against it, and its consistency-token
            // check observes it. Without this, nothing forces a write-write conflict and a concurrent
            // conditional append's snapshot could miss this append (write skew). See ADR 0021.
            markers = incrementConflictMarkers(clientSession, DcbMarkerModel.eventMarkerKeys(eventsToAppend), lastPosition);
        }

        List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition, appendId);
        DcbMarkerVersionsDocuments.stampMarkerVersions(documents, eventsToAppend, markers);
        insertAllDcb(clientSession, streamId, currentStreamVersion, documents);
        Optional<DcbMarkerVersions> markerVersions = resumeFromMarkerVersions ? DcbMarkerVersionsDocuments.appendedVersions(condition, markers) : Optional.empty();
        return new DcbAppendResult(firstPosition, lastPosition, eventsToAppend.size(), Optional.of(appendId), markerVersions);
    }

    private List<Document> convertDcbCloudEventsToDocuments(String streamId, List<CloudEvent> cloudEvents, long currentStreamVersion, long firstPosition, AppendId appendId) {
        List<Document> documents = new ArrayList<>(cloudEvents.size());
        long streamVersion = currentStreamVersion + 1;
        long position = firstPosition;
        String appendIdValue = appendId.toString();
        for (CloudEvent cloudEvent : cloudEvents) {
            CloudEvent dcbCloudEvent = OccurrentCloudEventExtension.withPosition(cloudEvent, position);
            Document document = DcbDocumentMapper.toDocument(timeRepresentation, streamId, streamVersion++, dcbCloudEvent, position);
            document.put(OccurrentCloudEventExtension.APPEND_ID, appendIdValue);
            documents.add(document);
            position++;
        }
        return documents;
    }

    private Map<String, Document> enforceAppendCondition(ClientSession clientSession, DcbAppendCondition condition, List<CloudEvent> eventsToAppend, long lastPosition) {
        Optional<DcbConsistencyToken> expectedToken = condition.consistencyToken();
        final boolean conflict;
        if (expectedToken.isPresent()) {
            // The condition carries the consistency token the command observed when it read the query
            // (DcbEventStream.consistencyToken()). If the query's markers have advanced since, a matching append
            // committed after the read, so the condition fails. This is immune to read-watermark overshoot,
            // unlike a position-based check, because marker versions bump inside the append transaction at
            // commit, not when positions are reserved (ADR 0021).
            conflict = consistencyToken(clientSession, condition.criteria()) != expectedToken.get().value();
        } else {
            // No token: an absolute "fail if any matching event exists" guard. Checks the live events rather than
            // marker versions, so it means "currently exists" (surviving deletes and marker pruning) rather than
            // "ever appended". The marker increments below still serialize concurrent unconditional guards on the
            // same boundary, so two of them cannot both pass.
            conflict = eventCollection.find(clientSession, toDcbBsonQuery(condition.criteria(), 0, Long.MAX_VALUE)).limit(1).first() != null;
        }
        if (conflict) {
            throw new DcbAppendConditionNotFulfilledException(condition, currentPosition());
        }
        // Increment a marker per key for the union of the query's keys and the appended events' keys. The increment
        // forces a write-write conflict that serializes concurrent appends sharing a marker, so the loser re-runs
        // this check against the winner's committed increment. The query's markers are always incremented, so a
        // concurrent matching append is serialized even when this append's own events do not match the query.
        TreeSet<String> markerKeys = new TreeSet<>(DcbMarkerModel.queryMarkerKeys(condition.criteria()));
        markerKeys.addAll(DcbMarkerModel.eventMarkerKeys(eventsToAppend));
        return incrementConflictMarkers(clientSession, markerKeys, lastPosition);
    }

    // Increment a conflict marker per key. Two appends that can match a common event share at least one marker
    // (ADR 0021), so the in-transaction increment forces a MongoDB write-write conflict and they serialize. The
    // monotonically increasing version is also the optimistic-concurrency token (see consistencyToken): a reader
    // snapshots a query's marker versions, and an append fails if any changed since. The stored lastPosition is
    // informational.
    // The marker collection holds one document per distinct tag and type that has taken part in an append, and
    // nothing reclaims them automatically, so a high-cardinality tag (a tag per entity) grows the collection
    // without bound. An operator can prune markers during quiescence, a later append recreates any it still needs.
    // Returns the incremented markers as this transaction left them, keyed by marker key, so the append stamps its
    // events with the versions it gave them (ADR 148).
    private Map<String, Document> incrementConflictMarkers(ClientSession clientSession, Set<String> markerKeys, long lastPosition) {
        if (markerKeys.isEmpty()) {
            return Map.of();
        }
        // One unordered bulk write rather than one updateOne round trip per key, so a boundary with several tags and
        // types costs one round trip inside the transaction instead of K serial ones. Each marker gets its upsert and
        // an update that gives a marker an older writer created an epoch.
        List<WriteModel<Document>> updates = new ArrayList<>(markerKeys.size() * 2);
        for (String key : markerKeys) {
            updates.add(new UpdateOneModel<>(eq(ID, DcbMarkerModel.markerId(key)), DcbMarkerVersionsDocuments.markerIncrement(lastPosition), new UpdateOptions().upsert(true)));
            updates.add(new UpdateOneModel<>(DcbMarkerVersionsDocuments.markerWithoutEpoch(key), DcbMarkerVersionsDocuments.setEpoch()));
        }
        dcbCheckpointCollection.bulkWrite(clientSession, updates, new BulkWriteOptions().ordered(false));
        return DcbMarkerVersionsDocuments.markersByKey(dcbCheckpointCollection.find(clientSession, DcbMarkerVersionsDocuments.markersFilter(markerKeys)));
    }

    // The optimistic-concurrency token for a query: the sum of the versions of its conflict markers. The sum is
    // monotonically increasing (every append increments at least one marker), so it changes if and only if some
    // append touched one of the query's markers since the reader observed it. Because versions bump inside the
    // append transaction, not when positions are reserved, this token reflects only committed appends and is
    // immune to the read-watermark overshoot a position-based check suffers (ADR 0021).
    private long consistencyToken(@Nullable ClientSession clientSession, DcbCriteria criteria) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(criteria);
        if (markerKeys.isEmpty()) {
            return 0;
        }
        // Read the query's markers in one query so their versions come from a single consistent snapshot. Reading
        // them one by one could tear across a concurrent append and capture a sum that masks a real conflict
        // (ADR 0031).
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
     * Reserves a contiguous block of {@code eventCount} global positions by incrementing one counter document, shared
     * by the DCB and stream write paths so both draw from one sequence. Every positioned append passes through this
     * document, so it serializes writes and caps append throughput under very high load. It is kept outside the append
     * transaction so it does not cause transaction conflicts.
     */
    private long reservePositions(int eventCount) {
        // Retry the cold-start race: when the counter document does not exist yet, concurrent upserts all try to insert
        // it and all but one get a duplicate key. On retry the document exists, so the upsert becomes an update.
        return RetryStrategy.retry()
                .backoff(Backoff.fixed(20))
                .maxAttempts(5)
                .retryIf(MongoEventStore::isDuplicateKeyError)
                .execute(() -> {
                    Document updated = dcbPositionCollection.findOneAndUpdate(
                            eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID),
                            Updates.inc(DcbMarkerModel.COUNTER_POSITION, (long) eventCount),
                            new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
                    long lastPosition = ((Number) requireNonNull(updated, "DCB position document cannot be null").get(DcbMarkerModel.COUNTER_POSITION)).longValue();
                    return lastPosition - eventCount + 1;
                });
    }

    /**
     * The highest position reserved so far, shared by the DCB and stream write paths. Returns {@code 0} when no
     * positioned event has been written yet.
     *
     * @throws UnsupportedOperationException if this store does not write a position ({@link #writesPosition()} is
     *                                        {@code false}).
     */
    @Override
    public long currentPosition() {
        requirePosition();
        Document document = dcbPositionCollection.find(eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
        return document == null ? 0 : ((Number) document.get(DcbMarkerModel.COUNTER_POSITION)).longValue();
    }

    private long currentPosition(ClientSession clientSession) {
        Document document = dcbPositionCollection.find(clientSession, eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
        return document == null ? 0 : ((Number) document.get(DcbMarkerModel.COUNTER_POSITION)).longValue();
    }

    private void insertAllDcb(ClientSession clientSession, String streamId, long streamVersion, List<Document> documents) {
        try {
            eventCollection.insertMany(clientSession, documents);
        } catch (MongoException e) {
            // A transient transaction conflict is retried by executeWithTransientRetry rather than mapped to the
            // stream-path WriteConditionNotFulfilledException, which DCB does not use.
            if (isTransientTransactionError(e)) {
                throw e;
            }
            // Two disjoint DCB boundaries that hash to the same partition stream race on the next stream version,
            // and one loses on the unique streamid+streamversion index. This is not a duplicate CloudEvent, so
            // rethrow the raw duplicate-key error and let executeWithTransientRetry rerun the read-decide-append cycle.
            if (isDuplicateKeyErrorOnStreamVersionIndex(e)) {
                throw e;
            }
            throw translateException(new WriteContext(streamId, streamVersion, anyStreamVersion()), e);
        }
    }

    // Retry the append transaction on a MongoDB TransientTransactionError (the error label is present when two
    // transactions conflict, e.g. a write-write conflict on a shared marker). The native driver's withTransaction
    // already retries a transient transaction error on its own, so at this layer the retry mainly covers the
    // cold-marker DuplicateKeyException race where two transactions first-create the same marker at once, and the
    // partition stream-version collision where two disjoint DCB boundaries hash to the same partition stream.
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
        // A duplicate key can surface wrapped rather than at the top, so walk the cause chain the same bounded way as
        // the transient-transaction check. It is either the cold-marker race or a partition stream-version collision,
        // both retryable. A DuplicateCloudEventException is a genuine business error (an event whose id and source
        // already exist), and the driver duplicate-key exception is its cause, so stop and do not retry once it appears
        // in the chain.
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
        return anyEventMatches(eq(STREAM_ID, streamId));
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
                throw UpdateEventFunctionValidator.updateFunctionReturnedNull();
            }
            updatedCloudEvent = OccurrentCloudEventExtension.preserveStreamIdentity(currentCloudEvent, updatedCloudEvent);
            updatedCloudEvent = OccurrentCloudEventExtension.preserveAppendId(currentCloudEvent, updatedCloudEvent);
            updatedCloudEvent = OccurrentCloudEventExtension.preservePosition(currentCloudEvent, updatedCloudEvent);
            updatedCloudEvent = DcbCloudEvents.preserveTags(currentCloudEvent, updatedCloudEvent);
            if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                Document updatedDocument = convertToDocument(timeRepresentation, streamId, streamVersion, updatedCloudEvent);
                DcbDocumentMapper.preservePositionAndDcbTags(currentCloudEvent, updatedDocument);
                DcbDocumentMapper.preserveMarkerVersions(document, updatedDocument);
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

    /**
     * Internal. Opens a {@link CatchupReadScope}, see {@link CatchupReads#openCatchupReadScope(String)}. The scope
     * reads through a causally consistent session of its own, never the caller's, and every read in it uses read
     * concern majority. The session is closed with the scope.
     */
    @Override
    public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
        return new MongoCatchupReadScope(notBefore);
    }

    /**
     * Internal. Opens a {@link DcbCatchupReadScope}, see {@link DcbCatchupReads#openDcbCatchupReadScope(String)}.
     *
     * @throws UnsupportedOperationException if this store does not have the DCB capability.
     */
    @Override
    public DcbCatchupReadScope openDcbCatchupReadScope(@Nullable String notBefore) {
        requireDcbCapability();
        return new MongoCatchupReadScope(notBefore);
    }

    // One causally consistent session for one replay attempt. The live start is the operation time of a majority read of
    // the position counter on the primary, plus one increment, and every replay read is a majority read in the same
    // session, so each of them sees every write that was majority-committed by then. A read preference configured on the
    // collection is kept for the replay reads, and only the counter is read from the primary.
    private final class MongoCatchupReadScope implements CatchupReadScope, DcbCatchupReadScope {
        private final ClientSession session;
        private final MongoCollection<Document> events = eventCollection.withReadConcern(ReadConcern.MAJORITY);
        private final MongoCollection<Document> positions = dcbPositionCollection.withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary());
        private final @Nullable String liveStart;

        MongoCatchupReadScope(@Nullable String notBefore) {
            this.session = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());
            try {
                if (notBefore != null) {
                    // Advances to just before the live start and not to it, since the cluster time may not have reached it yet
                    // and the replay only needs what committed before it
                    OperationTimeLiveStart.tryParse(notBefore).map(OperationTimeLiveStart::oneIncrementBefore).ifPresent(session::advanceOperationTime);
                }
                positions.find(session, eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
                BsonTimestamp t0 = session.getOperationTime();
                this.liveStart = OperationTimeLiveStart.liveStartAfterRead(t0);
            } catch (RuntimeException e) {
                session.close();
                throw e;
            }
        }

        @Override
        public @Nullable String liveStart() {
            return liveStart;
        }

        @Override
        public long currentPosition() {
            requirePosition();
            Document document = positions.find(session, eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
            return document == null ? 0 : ((Number) document.get(DcbMarkerModel.COUNTER_POSITION)).longValue();
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            requirePosition();
            return MongoEventStore.this.readInPositionOrder(filter, range, this::currentPosition, this::findEvents);
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            requireStreamCapability();
            requireNonNull(filter, "Filter cannot be null");
            Bson query = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
            return readCloudEvents(query, skip, limit, sortBy, this::findEvents)
                    .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            requireDcbCapability();
            requireNonNull(criteria, "Criteria cannot be null");
            DcbCatchupReadScope.requirePositionRange(options);
            return readWithoutMarkerVersions(criteria, options, () -> consistencyToken(session, criteria), this::currentPosition, this::findEvents);
        }

        private FindIterable<Document> findEvents(Bson query) {
            return events.find(session, query);
        }

        @Override
        public void close() {
            session.close();
        }
    }

    @Override
    public long count(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            // estimatedDocumentCount() is fast but has no ClientSession overload and cannot join a transaction, so
            // inside an ambient session fall back to an exact count-all so a synchronous handler sees the uncommitted
            // write. With no session, keep the fast estimate, exactly as before.
            ClientSession ambientSession = ClientSessionHolder.get();
            return ambientSession == null ? eventCollection.estimatedDocumentCount() : eventCollection.countDocuments(ambientSession, new Document());
        } else {
            final Bson query = FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter);
            return countEvents(query);
        }
    }

    @Override
    public boolean exists(Filter filter) {
        requireStreamCapability();
        requireNonNull(filter, "Filter cannot be null");
        return anyEventMatches(FilterToBsonFilterConverter.convertFilterToBsonFilter(timeRepresentation, filter));
    }

    private record EventStreamImpl<T>(String id, long version, Stream<T> events) implements EventStream<T> {
    }

    private static void initializeEventStore(MongoCollection<Document> eventStoreCollection, MongoDatabase mongoDatabase, Set<EventStoreCapability> eventStoreCapabilities, boolean writesPosition, String dcbPositionCollectionName, String dcbCheckpointCollectionName) {
        String eventStoreCollectionName = eventStoreCollection.getNamespace().getCollectionName();
        if (!collectionExists(mongoDatabase, eventStoreCollectionName)) {
            mongoDatabase.createCollection(eventStoreCollectionName);
        }
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        // The position counter collection holds the shared position counter too, so it must exist whenever position is
        // written, not only when DCB is enabled.
        if (writesPosition && !collectionExists(mongoDatabase, dcbPositionCollectionName)) {
            mongoDatabase.createCollection(dcbPositionCollectionName);
        }
        if (dcbEnabled && !collectionExists(mongoDatabase, dcbCheckpointCollectionName)) {
            mongoDatabase.createCollection(dcbCheckpointCollectionName);
        }

        // The CloudEvent spec requires id + source to be unique.
        eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true));
        // streamId + streamVersion uniqueness is a stream-mode invariant, but the DCB append path also looks up
        // the current stream version per partition, so it needs the same compound index to avoid a collection
        // scan. The index stays unique for DCB too, since DCB-only writes assign sequential per-partition stream
        // versions. The only collision is two disjoint DCB boundaries hashing to the same partition stream, which
        // insertAllDcb treats as a retryable transient rather than a duplicate error. One identical unique index
        // for STREAM and DCB also means no capability combination or upgrade ever hits an IndexOptionsConflict.
        if (eventStoreCapabilities.contains(STREAM) || dcbEnabled) {
            // This compound index also covers queries on stream id alone, and MongoDB can traverse it in either
            // direction, so it serves both ascending and descending sorts.
            createStreamVersionIndex(eventStoreCollection, new IndexOptions().unique(true));
        }
        // The position index is created whenever position is written, since DCB and stream writes share the same
        // unique, sparse position field.
        if (writesPosition) {
            eventStoreCollection.createIndex(Indexes.ascending(OccurrentCloudEventExtension.POSITION), new IndexOptions().unique(true).sparse(true));
        }
        if (dcbEnabled && eventStoreCapabilities.contains(STREAM)) {
            // Only DCB events have dcbTags, so with stream events in the collection this index narrows a match-all
            // DcbCriteria to the DCB events. (dcbTags, position) also holds every stream event with a position. On a
            // DCB-only store whose collection holds only DCB events, this index holds the same events as the position
            // index, so it narrows nothing.
            eventStoreCollection.createIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true));
        }
        if (dcbEnabled) {
            // A type-only DcbCriteria has no tags to hit the dcbTags index with, so it falls back to the position
            // index with type checked as a residual FETCH filter, examining every DCB event in the position range.
            // A (type, position) compound index lets the planner satisfy the type equality and position sort
            // directly from the index, so keysExamined tracks nReturned instead of the full position range.
            // Evidence: explain("executionStats") on a 50k/50 skewed dataset showed docsExamined=50050 for
            // nReturned=50 without this index.
            eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending("type"), Indexes.ascending(OccurrentCloudEventExtension.POSITION)), new IndexOptions().sparse(true));
            // The multikey dcbTags index alone cannot provide the position sort order, so a tag boundary read falls
            // back to an in-memory (or, on MongoDB 6.0+, disk-spilling) SORT after fetching every matching document.
            // A (dcbTags, position) compound index lets the planner read matches in position order directly instead.
            // Evidence: explain("executionStats") on a 5,000-of-305,000 skewed dataset (a plausible popular-tag
            // boundary) showed a winning SORT stage over the dcbTags index without this compound index.
            eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), Indexes.ascending(OccurrentCloudEventExtension.POSITION)), new IndexOptions().sparse(true));
            // A read after marker versions selects events by one mv element per criteria item (ADR 148).
            eventStoreCollection.createIndex(DcbMarkerVersionsDocuments.markerVersionsIndex(), new IndexOptions().sparse(true));
        }
        if (dcbEnabled && !eventStoreCapabilities.contains(STREAM)) {
            warnOnPositionedStreamEventsWithoutDcbTagsIndex(eventStoreCollection);
        }
    }

    // Runs after the (dcbTags, position) index exists, since the lookup is hinted on it. A failed check only logs.
    // The indexes are listed only when a stream event exists, so a collection without one needs no listIndexes privilege.
    private static void warnOnPositionedStreamEventsWithoutDcbTagsIndex(MongoCollection<Document> eventStoreCollection) {
        String collectionName = eventStoreCollection.getNamespace().getCollectionName();
        try {
            Document positionedStreamEvent = eventStoreCollection.find(DcbTagsIndexCheck.positionedStreamEvent())
                    .hint(DcbTagsIndexCheck.hint())
                    .min(DcbTagsIndexCheck.min())
                    .max(DcbTagsIndexCheck.max())
                    .limit(1)
                    .projection(Projections.include(ID))
                    .first();
            if (positionedStreamEvent == null) {
                return;
            }
            String warning = DcbTagsIndexCheck.warningFor(collectionName, eventStoreCollection.listIndexes().into(new ArrayList<>()));
            if (warning != null) {
                log.warn(warning);
            }
        } catch (RuntimeException e) {
            log.warn(DcbTagsIndexCheck.checkFailedMessage(collectionName), e);
        }
    }

    // The streamid+streamversion index already exists with options that clash with the unique one Occurrent needs
    // (older MongoDB reports this as error 85, 7.0+ as 86). Occurrent never replaces an index, so fail rather than
    // run without the uniqueness that stream and DCB writes depend on.
    private static void createStreamVersionIndex(MongoCollection<Document> eventStoreCollection, IndexOptions indexOptions) {
        try {
            eventStoreCollection.createIndex(Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.ascending(STREAM_VERSION)), indexOptions);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 85 || e.getErrorCode() == 86) {
                throw new IllegalStateException("The '" + STREAM_ID + "_1_" + STREAM_VERSION + "_1' index already exists with options incompatible with the unique index Occurrent requires. Occurrent does not drop or replace existing indexes automatically, so running with the existing index would silently lose the uniqueness guarantee that stream and DCB writes rely on. Drop and recreate the index as unique out-of-band, then restart.", e);
            } else {
                throw e;
            }
        }
    }

    // Decide at startup whether this store writes stream position. An explicit choice (withStreamPosition() or
    // withoutStreamPosition()) and DCB are honored as-is. When position is only on by default, turn it off if the
    // collection already holds events without a position, so upgrading an existing store does not build the position
    // index over the whole collection at startup. The user gets a warning naming how to turn it on and backfill.
    //
    // Turning position off here means writesPosition() is false, so the un-backfilled checks never run, and the damage
    // check runs only under requireRepairedEvents, where a damaged event fails startup instead of being warned about.
    // Without that setting this warning is the only one the operator sees. An event whose position updateEvent dropped
    // is enough to reach it, so the shared message includes the repair caveat rather than naming the backfill alone.
    private static boolean resolveStreamPositionEnabled(EventStoreConfig config, MongoCollection<Document> eventCollection) {
        if (!config.streamPositionEnabled) {
            return false;
        }
        if (config.eventStoreCapabilities.contains(DCB) || config.streamPositionExplicitlyEnabled) {
            return true;
        }
        if (hasPreExistingUnpositionedEvents(eventCollection)) {
            log.warn(PositionBackfillValidator.positionDisabledByUnpositionedEventsMessage(eventCollection.getNamespace().getCollectionName()));
            return false;
        }
        return true;
    }

    // A cheap probe for an existing un-backfilled store. Backfill assigns positions in _id order, oldest first, so if
    // the oldest event has no position the collection predates position and has not been backfilled.
    private static boolean hasPreExistingUnpositionedEvents(MongoCollection<Document> eventCollection) {
        Document oldestEvent = eventCollection.find().sort(Sorts.ascending(ID)).limit(1).projection(Projections.include(OccurrentCloudEventExtension.POSITION)).first();
        return oldestEvent != null && !oldestEvent.containsKey(OccurrentCloudEventExtension.POSITION);
    }

    // Warns or fails when a position-writing store has pre-existing events without a position, since position-based
    // catch-up would silently skip that history. Uses the position index to find one such event, so it stays cheap on
    // a fully positioned store.
    private static void warnOrFailOnUnpositionedEvents(MongoCollection<Document> eventCollection, boolean requireBackfilledPosition) {
        if (eventCollection.estimatedDocumentCount() == 0) {
            return;
        }
        Bson unpositioned = Filters.exists(OccurrentCloudEventExtension.POSITION, false);
        Document firstUnpositionedEvent = eventCollection.find(unpositioned).limit(1).first();
        if (firstUnpositionedEvent == null) {
            return;
        }
        String collectionName = eventCollection.getNamespace().getCollectionName();
        if (requireBackfilledPosition) {
            throw PositionBackfillValidator.unpositionedEventsExist(collectionName);
        }
        log.warn(PositionBackfillValidator.unpositionedEventsMessage(collectionName));
    }

    // Warns, or fails when requireRepairedEvents is set, when the collection holds events whose position or tag index
    // is wrong. The warning looks for a string position only, what updateEvent wrote before 0.34.0, which reads no index
    // keys on a store that was never damaged. requireRepairedEvents refuses every event whose position is not a
    // positive integer, every event whose tag fields are not what an append writes, a counter no writer would
    // store and a position above the counter, a missing one counting as zero, at the cost of a collection scan.
    private static void warnOrFailOnEventsDamagedByUpdateEvent(MongoCollection<Document> eventCollection, MongoCollection<Document> positionCollection, boolean requireRepairedEvents) {
        Bson damaged = requireRepairedEvents ? UpdateEventDamage.wrongPositionOrTagIndex() : UpdateEventDamage.positionStoredAsString();
        // Whether one exists, not what is in it. Without the projection this pulls a whole stored event, payload and
        // all, into the startup path of an affected store.
        Document firstDamagedEvent = eventCollection.find(damaged).limit(1).projection(Projections.include(ID)).first();
        if (firstDamagedEvent == null && !(requireRepairedEvents && wrongCounter(eventCollection, positionCollection))) {
            return;
        }
        String collectionName = eventCollection.getNamespace().getCollectionName();
        if (requireRepairedEvents) {
            throw UpdateEventRepairValidator.damagedEventsExist(collectionName);
        }
        log.warn(UpdateEventRepairValidator.damagedEventsMessage(collectionName));
    }

    // The highest position is read before the counter, which is what keeps an append in flight from looking like a
    // position above it. UpdateEventDamage.wrongCounter says why.
    private static boolean wrongCounter(MongoCollection<Document> eventCollection, MongoCollection<Document> positionCollection) {
        Document highestPositioned = eventCollection.find(UpdateEventDamage.positionIsANumber())
                .sort(descending(OccurrentCloudEventExtension.POSITION))
                .limit(1)
                .projection(Projections.include(OccurrentCloudEventExtension.POSITION))
                .first();
        Document counter = positionCollection.find(eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
        return UpdateEventDamage.wrongCounter(highestPositioned, counter);
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

    /**
     * Rejects any DCB-tagged event on the stream write path, regardless of which capabilities are enabled. A
     * dcbtags-carrying event written through write(...) would get no derived dcbTags array and no DCB position, so it
     * would be silently invisible to DCB reads. Enforcing this keeps the dcbtags extension and the dcbTags array
     * equivalent, which the capability filter relies on.
     */
    private static void rejectDcbTaggedEvents(List<CloudEvent> events) {
        if (events.stream().anyMatch(DcbCloudEvents::isDcbEvent)) {
            throw new IllegalArgumentException("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");
        }
    }

    private void requireDcbCapability() {
        requireCapability(DCB);
    }

    private void requireCapability(EventStoreCapability capability) {
        if (!eventStoreCapabilities.contains(capability)) {
            throw new UnsupportedOperationException(capability + " capability is not enabled for this MongoEventStore");
        }
    }

    /**
     * Returns whether this store writes a global position, so position-requiring APIs are safe to call.
     */
    @Override
    public boolean writesPosition() {
        return eventStoreCapabilities.contains(DCB) || (eventStoreCapabilities.contains(STREAM) && streamPositionEnabled);
    }

    private void requirePosition() {
        if (!writesPosition()) {
            throw new UnsupportedOperationException("This MongoEventStore does not write a position. Enable DCB, or do not call withoutStreamPosition() on a STREAM-only store, to use position-requiring APIs.");
        }
    }

    private static Bson streamIdEqualTo(String streamId) {
        return eq(STREAM_ID, streamId);
    }

    // "afterVersion" is exclusive, matching ExecuteOptions.fromStreamVersion and the "skip N stream positions"
    // reading of EventStore.read's skip parameter, so a skip of 0 keeps every event and a skip of N drops the
    // first N regardless of whether a StreamReadFilter narrows the result further.
    private static Bson streamIdAndStreamVersionBetween(String streamId, long afterVersion, long uptoAndIncludingVersion) {
        return and(streamIdEqualTo(streamId), gt(STREAM_VERSION, afterVersion), lte(STREAM_VERSION, uptoAndIncludingVersion));
    }

    // Package-private so MongoEventStoreDcbConcurrencyTest can explain the exact query the store sends
    static Bson toDcbBsonQuery(DcbCriteria criteria, long afterPosition, long upperSequencePosition) {
        return toDcbBsonQuery(criteria, afterPosition, upperSequencePosition, null);
    }

    // itemRestriction narrows each item's filter, so a read after marker versions selects every item by its own mv element
    static Bson toDcbBsonQuery(DcbCriteria criteria, long afterPosition, long upperSequencePosition, @Nullable Function<DcbCriterion, Bson> itemRestriction) {
        Bson positionFilter = and(gt(OccurrentCloudEventExtension.POSITION, afterPosition), lte(OccurrentCloudEventExtension.POSITION, upperSequencePosition));
        Bson dcbTagsExistsFilter = Filters.exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD);
        if (criteria instanceof DcbCriteria.MatchAll) {
            return and(positionFilter, dcbTagsExistsFilter);
        }
        List<Bson> itemFilters = DcbMarkerModel.dcbQueryItems(criteria).stream()
                .map(item -> itemRestriction == null ? toBsonFilter(item) : and(toBsonFilter(item), itemRestriction.apply(item)))
                .toList();
        return and(positionFilter, dcbTagsExistsFilter, or(itemFilters));
    }

    private static Bson toBsonFilter(DcbCriterion item) {
        List<Bson> filters = new ArrayList<>();
        if (!item.types().isEmpty()) {
            filters.add(in("type", item.types()));
        }
        if (!item.excludedTypes().isEmpty()) {
            filters.add(nin("type", item.excludedTypes()));
        }
        if (!item.tags().isEmpty()) {
            filters.add(Filters.all(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, item.tags().stream().map(Tag::canonical).toList()));
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
            List<SortBy> steps = ((MultipleSortStepsImpl) sortBy).steps;
            // A natural sort step is already a total ordering, so combining it with other sort steps in a compound
            // sort is incoherent. MongoDB 7.0+ also rejects $natural inside a compound sort server-side (BadValue:
            // "$natural sort cannot be set to a value other than -1 or 1"), so reject it here instead of silently
            // degrading it, which is what older MongoDB (4.x) did by applying pure natural order and ignoring the
            // other keys.
            if (steps.stream().anyMatch(NaturalImpl.class::isInstance)) {
                throw new IllegalArgumentException("A natural sort step cannot be combined with other sort steps, since natural order is already a total ordering. Use natural sort alone.");
            }
            sort = steps.stream()
                    .map(MongoEventStore::convertToMongoDBSort)
                    .reduce(Sorts::orderBy)
                    .orElseThrow(() -> new IllegalStateException("Internal error: Expecting " + MultipleSortStepsImpl.class.getSimpleName() + " to have at least one step"));
        } else {
            throw new IllegalArgumentException("Internal error: Unrecognized " + SortBy.class.getSimpleName() + " instance: " + sortBy.getClass().getSimpleName());
        }
        return sort;
    }
}
