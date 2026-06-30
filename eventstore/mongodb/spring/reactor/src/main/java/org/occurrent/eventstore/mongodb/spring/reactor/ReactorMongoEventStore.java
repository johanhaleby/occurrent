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

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.eventstore.mongodb.internal.StreamVersionDiff;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.translateException;
import static org.occurrent.mongodb.spring.sortconversion.internal.SortConverter.convertToSpringSort;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * This is a reactive {@link EventStore} implementation that stores events in MongoDB using
 * Spring's {@link ReactiveMongoTemplate} that is based on <a href="https://projectreactor.io/">project reactor</a>.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts, and the reactive
 * {@link org.occurrent.eventstore.api.dcb.reactor.DcbEventStore} contract when the {@code DCB} capability is enabled.
 * <p>
 * By default, only stream-based event-store operations are enabled. Configure
 * {@link EventStoreConfig.Builder#eventStoreCapabilities(Set)} to enable DCB, or to enable both stream and DCB
 * operations. Occurrent creates missing indexes for enabled capabilities, but it never removes indexes automatically.
 */
@NullMarked
public class ReactorMongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, org.occurrent.eventstore.api.dcb.reactor.DcbEventStore {

    private static final String ID = "_id";

    private final ReactiveMongoTemplate mongoTemplate;
    private final String eventStoreCollectionName;
    private final String dcbPositionCollectionName;
    private final String dcbCheckpointCollectionName;
    private final TimeRepresentation timeRepresentation;
    private final TransactionalOperator transactionalOperator;
    private final Function<Query, Query> queryOptions;
    private final Function<Query, Query> readOptions;
    private final Set<EventStoreCapability> eventStoreCapabilities;
    private final DcbStreamIdGenerator dcbStreamIdGenerator;

    /**
     * Create a new instance of {@code SpringReactorMongoEventStore}
     *
     * @param mongoTemplate The {@link ReactiveMongoTemplate} that the {@code SpringReactorMongoEventStore} will use
     * @param config        The {@link EventStoreConfig} that will be used
     */
    public ReactorMongoEventStore(ReactiveMongoTemplate mongoTemplate, EventStoreConfig config) {
        requireNonNull(mongoTemplate, ReactiveMongoTemplate.class.getSimpleName() + " cannot be null");
        requireNonNull(config, EventStoreConfig.class.getSimpleName() + " cannot be null");
        this.mongoTemplate = mongoTemplate;
        this.eventStoreCollectionName = config.eventStoreCollectionName;
        this.dcbPositionCollectionName = DcbMarkerModel.positionCollectionName(eventStoreCollectionName);
        this.dcbCheckpointCollectionName = DcbMarkerModel.checkpointCollectionName(eventStoreCollectionName);
        this.transactionalOperator = config.transactionalOperator;
        this.timeRepresentation = config.timeRepresentation;
        this.queryOptions = config.queryOptions;
        this.readOptions = config.readOptions;
        this.eventStoreCapabilities = config.eventStoreCapabilities;
        this.dcbStreamIdGenerator = config.dcbStreamIdGenerator;
        initializeEventStore(eventStoreCollectionName, dcbPositionCollectionName, dcbCheckpointCollectionName, eventStoreCapabilities, mongoTemplate).block();
    }

    @Override
    public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
        return write(streamId, WriteCondition.anyStreamVersion(), events);
    }

    @Override
    public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        if (writeCondition == null) {
            throw new IllegalArgumentException(WriteCondition.class.getSimpleName() + " cannot be null");
        }

        Mono<StreamVersionDiff> operation = currentStreamVersion(streamId)
                .flatMap(currentStreamVersion -> validateWriteCondition(streamId, writeCondition, currentStreamVersion))
                .flatMap(currentStreamVersion -> {
                    Flux<Document> documentFlux = convertEventsToMongoDocuments(streamId, events, currentStreamVersion);
                    Mono<StreamVersionDiff> streamVersionDiffFlux = documentFlux.collectList().flatMap(documents -> {
                        final long newStreamVersion;
                        if (documents.isEmpty()) {
                            newStreamVersion = currentStreamVersion;
                        } else {
                            newStreamVersion = documents.get(documents.size() - 1).getLong(STREAM_VERSION);
                        }
                        return insertAll(streamId, currentStreamVersion, writeCondition, documents)
                                .then(Mono.just(StreamVersionDiff.of(currentStreamVersion, newStreamVersion)));
                    });
                    return streamVersionDiffFlux.switchIfEmpty(Mono.just(StreamVersionDiff.of(currentStreamVersion, currentStreamVersion)));
                });

        return transactionalOperator.transactional(operation)
                .map(streamVersionDiff -> new WriteResult(streamId, streamVersionDiff.oldStreamVersion, streamVersionDiff.newStreamVersion));
    }

    @Override
    public Mono<Boolean> exists(String streamId) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        return mongoTemplate.exists(queryOptions.apply(streamIdEqualTo(streamId)), eventStoreCollectionName);
    }

    @Override
    public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        Mono<EventStreamImpl> eventStream = readEventStream(streamId, null, skip, limit);
        return convertToCloudEvent(timeRepresentation, eventStream);
    }

    @Override
    public Mono<EventStream<CloudEvent>> read(String streamId, StreamReadFilter filter, int skip, int limit) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(streamId, "Stream id cannot be null");
        requireNonNull(filter, "filter cannot be null");
        Mono<EventStreamImpl> eventStream = readEventStream(streamId, filter, skip, limit);
        return convertToCloudEvent(timeRepresentation, eventStream);
    }

    // DCB
    @Override
    public Mono<DcbEventStream> read(DcbQuery query, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        // Snapshot the consistency token BEFORE reading the events. If an append commits between these two reads, the
        // events may include it while the token does not, which only makes a later conditional append over-cautious (a
        // false conflict that retries) rather than miss the conflict.
        return consistencyToken(query).flatMap(token ->
                currentDcbPosition().flatMap(highWatermark -> {
                    long upperBound = Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
                    Query mongoQuery = toDcbMongoQuery(query, options.afterSequencePosition().orElse(0), upperBound);
                    mongoQuery.with(Sort.by(Sort.Direction.ASC, DcbCloudEvents.POSITION));
                    return mongoTemplate.find(queryOptions.apply(mongoQuery), Document.class, eventStoreCollectionName)
                            .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document))
                            .collectList()
                            .map(events -> new DcbEventStream(events, highWatermark, DcbConsistencyToken.of(token)));
                }));
    }

    @Override
    public Mono<Boolean> exists(DcbQuery query, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return currentDcbPosition().flatMap(highWatermark -> {
            long upperBound = Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
            return mongoTemplate.exists(queryOptions.apply(toDcbMongoQuery(query, options.afterSequencePosition().orElse(0), upperBound)), eventStoreCollectionName);
        });
    }

    @Override
    public Mono<Long> count(DcbQuery query, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return currentDcbPosition().flatMap(highWatermark -> {
            long upperBound = Math.min(highWatermark, options.upToSequencePosition().orElse(highWatermark));
            return mongoTemplate.count(queryOptions.apply(toDcbMongoQuery(query, options.afterSequencePosition().orElse(0), upperBound)), eventStoreCollectionName);
        });
    }

    @Override
    public Mono<DcbAppendResult> append(List<CloudEvent> events) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        return appendDcb(events, null);
    }

    @Override
    public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(condition, "Append condition cannot be null");
        return appendDcb(events, condition);
    }

    private Mono<DcbAppendResult> appendDcb(List<CloudEvent> events, @Nullable DcbAppendCondition condition) {
        final List<CloudEvent> eventsToAppend;
        try {
            eventsToAppend = DcbMarkerModel.validateDcbEvents(events);
        } catch (RuntimeException e) {
            return Mono.error(e);
        }
        // Place by the condition's boundary tags when it constrains on tags, so the same boundary always lands in the
        // same partition regardless of per-event tags. Otherwise (no condition, or a type-only/match-all condition)
        // fall back to the events' tags so tagless boundaries do not all collapse onto one hot partition.
        Set<String> conditionBoundaryTags = condition == null ? Set.of() : DcbCloudEvents.boundaryTags(condition.query());
        Set<String> placementTags = conditionBoundaryTags.isEmpty() ? DcbMarkerModel.boundaryTagsOf(eventsToAppend) : conditionBoundaryTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");
        int eventCount = eventsToAppend.size();

        // Reserve the position block once, outside the transaction. The counter findAndModify is a single atomic
        // document update that MongoDB serializes without raising a transaction conflict. The reserved block is reused
        // across transient-transaction-error retries, a doomed or condition-failed append abandons it, so dcbposition
        // may have gaps (DCB permits this, see ADR 0021).
        return reserveDcbPositions(eventCount).flatMap(firstPosition -> {
            long lastPosition = firstPosition + eventCount - 1;
            Mono<DcbAppendResult> transaction = transactionalOperator.transactional(
                    currentStreamVersion(streamId).flatMap(currentStreamVersion -> {
                        final Mono<Void> conditionAndMarkers;
                        if (condition != null) {
                            conditionAndMarkers = enforceAppendCondition(condition, eventsToAppend, lastPosition);
                        } else {
                            // An unconditional append still increments its events' markers so a concurrent conditional append on an
                            // overlapping tag/type shares a marker and serializes against it, and so the conditional append's
                            // consistency-token check observes it. Without this, an unconditional append touches no marker, nothing
                            // forces a write-write conflict, and a concurrent conditional append's snapshot can miss this append
                            // under MongoDB snapshot isolation (write skew). See ADR 0021.
                            conditionAndMarkers = incrementConflictMarkers(DcbMarkerModel.eventMarkerKeys(eventsToAppend), lastPosition);
                        }
                        return conditionAndMarkers.then(Mono.defer(() -> {
                            List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition);
                            return insertAllDcb(streamId, currentStreamVersion, documents).thenReturn(new DcbAppendResult(firstPosition, lastPosition, eventCount));
                        }));
                    }));
            // The driver does not auto-retry a transient transaction conflict reactively, so retry it here, plus a
            // DuplicateKeyException from two transactions first-creating the same conflict marker at once. A
            // DcbAppendConditionNotFulfilledException and a DuplicateCloudEventException are deliberately not retried.
            return transaction.retryWhen(Retry.backoff(15, Duration.ofMillis(10)).maxBackoff(Duration.ofMillis(500))
                    .filter(throwable -> isTransientTransactionError(throwable) || isDuplicateKeyError(throwable))
                    .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
        });
    }

    private Mono<Void> enforceAppendCondition(DcbAppendCondition condition, List<CloudEvent> eventsToAppend, long lastPosition) {
        Optional<DcbConsistencyToken> expectedToken = condition.consistencyToken();
        final Mono<Boolean> conflictMono;
        if (expectedToken.isPresent()) {
            // Token-carrying check: the condition carries the consistency token the command observed when it read the
            // query. If the query's markers have advanced since, an append matching the query committed after the read,
            // so the condition is not fulfilled. Immune to read-watermark overshoot because marker versions are bumped
            // inside the append transaction (ADR 0021).
            conflictMono = consistencyToken(condition.query()).map(actual -> actual != expectedToken.get().value());
        } else {
            // No token: an absolute "fail if any matching event exists" guard. The marker increments below still
            // serialize concurrent unconditional guards on the same boundary so two of them cannot both pass.
            conflictMono = mongoTemplate.exists(toDcbMongoQuery(condition.query(), 0, Long.MAX_VALUE), eventStoreCollectionName);
        }
        return conflictMono.flatMap(conflict -> {
            if (conflict) {
                return currentDcbPosition().flatMap(position -> Mono.<Void>error(new DcbAppendConditionNotFulfilledException(condition, position, "Append condition was not fulfilled.")));
            }
            // Increment a marker per key for the union of the query's keys and the appended events' keys. Always
            // increment the query's markers so a concurrent matching append is serialized even when this append's own
            // events do not match the query.
            TreeSet<String> markerKeys = new TreeSet<>(DcbMarkerModel.queryMarkerKeys(condition.query()));
            markerKeys.addAll(DcbMarkerModel.eventMarkerKeys(eventsToAppend));
            return incrementConflictMarkers(markerKeys, lastPosition);
        });
    }

    private Mono<Void> incrementConflictMarkers(Set<String> markerKeys, long lastPosition) {
        return Flux.fromIterable(markerKeys)
                .concatMap(key -> {
                    Query query = new Query(where(ID).is(DcbMarkerModel.markerId(key)));
                    Update update = new Update().inc(DcbMarkerModel.CHECKPOINT_VERSION, 1L).set(DcbMarkerModel.CHECKPOINT_LAST_POSITION, lastPosition);
                    return mongoTemplate.upsert(query, update, dcbCheckpointCollectionName);
                })
                .then();
    }

    // The optimistic-concurrency token for a query: the sum of the versions of its conflict markers. Read the markers
    // in one query so their versions come from a single consistent snapshot.
    private Mono<Long> consistencyToken(DcbQuery query) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(query);
        if (markerKeys.isEmpty()) {
            return Mono.just(0L);
        }
        List<String> markerIds = markerKeys.stream().map(DcbMarkerModel::markerId).toList();
        return mongoTemplate.find(new Query(where(ID).in(markerIds)), Document.class, dcbCheckpointCollectionName)
                .map(marker -> {
                    Number version = (Number) marker.get(DcbMarkerModel.CHECKPOINT_VERSION);
                    return version == null ? 0L : version.longValue();
                })
                .reduce(0L, Long::sum);
    }

    private Mono<Long> reserveDcbPositions(int eventCount) {
        Query query = new Query(where(ID).is(DcbMarkerModel.DCB_POSITION_DOCUMENT_ID));
        Update update = new Update().inc(DcbMarkerModel.DCB_COUNTER_POSITION, eventCount);
        FindAndModifyOptions options = FindAndModifyOptions.options().upsert(true).returnNew(true);
        return mongoTemplate.findAndModify(query, update, options, Document.class, dcbPositionCollectionName)
                .map(updated -> ((Number) updated.get(DcbMarkerModel.DCB_COUNTER_POSITION)).longValue() - eventCount + 1)
                // Cold-start race: when the counter document does not exist yet, concurrent upserts all try to insert it
                // and all but one get a duplicate key. On retry the document exists and the upsert becomes an update.
                .retryWhen(Retry.fixedDelay(5, Duration.ofMillis(20)).filter(ReactorMongoEventStore::isDuplicateKeyError));
    }

    private Mono<Long> currentDcbPosition() {
        return mongoTemplate.findById(DcbMarkerModel.DCB_POSITION_DOCUMENT_ID, Document.class, dcbPositionCollectionName)
                .map(document -> ((Number) document.get(DcbMarkerModel.DCB_COUNTER_POSITION)).longValue())
                .defaultIfEmpty(0L);
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

    private Mono<Void> insertAllDcb(String streamId, long streamVersion, List<Document> documents) {
        return mongoTemplate.insert(documents, eventStoreCollectionName)
                .onErrorResume(throwable -> {
                    // A transient transaction conflict (e.g. two disjoint DCB boundaries that hash to the same partition
                    // stream racing on stream_version) must stay transient so the append retry retries it, rather than
                    // being mapped to the stream-path WriteConditionNotFulfilledException (which DCB does not use).
                    if (isTransientTransactionError(throwable)) {
                        return Mono.error(throwable);
                    }
                    MongoException mongoException = findMongoException(throwable);
                    if (mongoException != null) {
                        return Mono.error(translateException(new WriteContext(streamId, streamVersion, WriteCondition.anyStreamVersion()), mongoException));
                    }
                    return Mono.error(throwable);
                })
                .then();
    }

    private static UnsupportedOperationException capabilityError(EventStoreCapability capability) {
        return new UnsupportedOperationException(capability + " capability is not enabled for this ReactorMongoEventStore");
    }

    private static boolean isTransientTransactionError(Throwable throwable) {
        Throwable cause = throwable;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof MongoException mongoException && mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDuplicateKeyError(Throwable throwable) {
        // A DuplicateCloudEventException is a genuine business error (an event whose id and source already exist), not
        // the cold-start marker or position race, and the driver duplicate-key exception is its cause, so stop and do
        // not retry once it appears in the chain.
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
            } else if (cause instanceof DuplicateKeyException) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private static MongoException findMongoException(Throwable throwable) {
        Throwable cause = throwable;
        for (int hops = 0; cause != null && hops < 64; cause = cause.getCause(), hops++) {
            if (cause instanceof MongoException mongoException) {
                return mongoException;
            }
        }
        return null;
    }

    private static Query toDcbMongoQuery(DcbQuery query, long afterSequencePosition, long upperSequencePosition) {
        Criteria positionCriteria = where(DcbCloudEvents.POSITION).gt(afterSequencePosition).lte(upperSequencePosition);
        if (query instanceof DcbQuery.MatchAll) {
            return new Query(positionCriteria);
        }
        List<Criteria> itemCriteria = DcbMarkerModel.dcbQueryItems(query).stream()
                .map(ReactorMongoEventStore::toCriteria)
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
            criteria.add(where(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD).all(item.tags()));
        }
        return new Criteria().andOperator(criteria);
    }

    // Read
    private Mono<EventStreamImpl> readEventStream(String streamId, @Nullable StreamReadFilter streamReadFilter, int skip, int limit) {
        return currentStreamVersion(streamId)
                .flatMap(currentStreamVersion -> {
                    // We use "lte" currentStreamVersion so that we don't have the start transactions on read. This means that even
                    // if another thread has inserted more events after we've read "currentStreamVersion" it doesn't matter.
                    Query query = streamIdAndStreamVersionLessThanOrEqualTo(streamId, currentStreamVersion);
                    if (streamReadFilter != null) {
                        StreamReadFilterValidator.validate(streamReadFilter);
                        Filter mapped = StreamReadFilterToFilterMapper.map(streamReadFilter);
                        Criteria criteria = FilterConverter.convertFilterToCriteria(null, timeRepresentation, mapped);
                        query.addCriteria(criteria);
                    }
                    Flux<Document> cloudEventDocuments = readCloudEvents(readOptions.apply(query), skip, limit, SortBy.streamVersion(ASCENDING));
                    return Mono.just(new EventStreamImpl(streamId, currentStreamVersion, cloudEventDocuments));
                })
                .switchIfEmpty(Mono.fromSupplier(() -> new EventStreamImpl(streamId, 0, Flux.empty())));
    }

    private Flux<Document> readCloudEvents(Query query, int skip, int limit, SortBy sortBy) {
        if (isSkipOrLimitDefined(skip, limit)) {
            query.skip(skip).limit(limit);
        }

        Sort sort = convertToSpringSort(sortBy);
        return mongoTemplate.find(query.with(sort), Document.class, eventStoreCollectionName);
    }

    private Mono<Long> currentStreamVersion(String streamId) {
        Query query = readOptions.apply(streamIdEqualTo(streamId));
        query.fields().include(STREAM_VERSION);
        return mongoTemplate.findOne(queryOptions.apply(query.with(Sort.by(DESC, STREAM_VERSION)).limit(1)), Document.class, eventStoreCollectionName)
                .map(documentWithLatestStreamVersion -> documentWithLatestStreamVersion.getLong(STREAM_VERSION))
                .switchIfEmpty(Mono.just(0L));
    }


    private Flux<Document> insertAll(String streamId, long streamVersion, WriteCondition writeCondition, Collection<Document> documents) {
        return mongoTemplate.insert(documents, eventStoreCollectionName)
                .onErrorMap(DuplicateKeyException.class, Throwable::getCause)
                .onErrorMap(DataIntegrityViolationException.class, Throwable::getCause)
                .onErrorMap(MongoException.class, e -> MongoExceptionTranslator.translateException(new WriteContext(streamId, streamVersion, writeCondition), e))
                .onErrorMap(UncategorizedMongoDbException.class, e -> {
                    if (e.getCause() instanceof MongoException) {
                        return MongoExceptionTranslator.translateException(new WriteContext(streamId, streamVersion, writeCondition), (MongoException) e.getCause());
                    } else {
                        return e;
                    }
                });
    }

    private static boolean isFulfilled(long streamVersion, WriteCondition writeCondition) {
        if (writeCondition.isAnyStreamVersion()) {
            return true;
        }

        if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition c)) {
            throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
        }

        return LongConditionEvaluator.evaluate(c.condition(), streamVersion);
    }

    // Initialization
    private static Mono<Void> initializeEventStore(String eventStoreCollectionName, String dcbPositionCollectionName, String dcbCheckpointCollectionName, Set<EventStoreCapability> eventStoreCapabilities, ReactiveMongoTemplate mongoTemplate) {
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);

        // Cloud spec defines id + source must be unique!
        Mono<Void> chain = createCollection(eventStoreCollectionName, mongoTemplate)
                .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true)))
                .then();

        // streamId + streamVersion uniqueness is a stream-mode invariant. DCB correctness rests on the per-attribute
        // markers and the unique dcbposition, not stream version, so a DCB-only store neither needs nor creates it.
        if (eventStoreCapabilities.contains(STREAM)) {
            chain = chain.then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.ascending(STREAM_VERSION)), new IndexOptions().unique(true))).then();
        }

        if (dcbEnabled) {
            chain = chain
                    .then(createCollection(dcbPositionCollectionName, mongoTemplate))
                    .then(createCollection(dcbCheckpointCollectionName, mongoTemplate))
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(DcbCloudEvents.POSITION), new IndexOptions().unique(true).sparse(true)))
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions()))
                    .then();
        }

        // SessionSynchronization need to be "ALWAYS" in order for TransactionTemplate to work with mongo template!
        // See https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
        mongoTemplate.setSessionSynchronization(ALWAYS);

        return chain;
    }

    private static Mono<String> createIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate, Bson index, IndexOptions indexOptions) {
        return mongoTemplate.getCollection(eventStoreCollectionName).flatMap(collection -> Mono.from(collection.createIndex(index, indexOptions)));
    }

    private static Mono<MongoCollection<Document>> createCollection(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.collectionExists(eventStoreCollectionName).flatMap(exists -> exists ? Mono.empty() : mongoTemplate.createCollection(eventStoreCollectionName));
    }

    private static Mono<EventStream<CloudEvent>> convertToCloudEvent(TimeRepresentation timeRepresentation, Mono<EventStreamImpl> eventStream) {
        return eventStream.map(es -> es.map(document -> convertToCloudEvent(timeRepresentation, document)));
    }

    private static CloudEvent convertToCloudEvent(TimeRepresentation timeRepresentation, Document document) {
        // Use the DCB-aware mapper so that DCB storage fields (dcbTags, dcbposition) are handled rather than leaked as
        // CloudEvent extensions when this store reads a collection that a DCB-enabled store also writes to. It is a
        // no-op for plain stream events.
        return DcbDocumentMapper.toCloudEvent(timeRepresentation, document);
    }

    private static boolean isSkipOrLimitDefined(int skip, int limit) {
        return skip != 0 || limit != Integer.MAX_VALUE;
    }

    @Override
    public Mono<Void> deleteEventStream(String streamId) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(streamId, "Stream id cannot be null");

        return mongoTemplate.remove(streamIdEqualTo(streamId), eventStoreCollectionName).then();
    }

    @Override
    public Mono<Void> deleteEvent(String cloudEventId, URI cloudEventSource) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(cloudEventId, "Cloud event id cannot be null");
        requireNonNull(cloudEventSource, "Cloud event source cannot be null");

        return mongoTemplate.remove(Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource)), eventStoreCollectionName).then();
    }

    @Override
    public Mono<Void> delete(Filter filter) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(filter, "Filter cannot be null");
        final Query query = FilterConverter.convertFilterToQuery(timeRepresentation, filter);
        return mongoTemplate.remove(query, eventStoreCollectionName).then();
    }

    @Override
    public Mono<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        Function<Function<CloudEvent, CloudEvent>, Mono<CloudEvent>> logic = (fn) -> {
            Query cloudEventQuery = cloudEventIdIs(cloudEventId, cloudEventSource);
            return mongoTemplate.findOne(cloudEventQuery, Document.class, eventStoreCollectionName)
                    .log()
                    .flatMap(document -> {
                        CloudEvent currentCloudEvent = convertToCloudEvent(timeRepresentation, document);
                        CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
                        final Mono<CloudEvent> result;
                        if (updatedCloudEvent == null) {
                            result = Mono.error(new IllegalArgumentException("Cloud event update function is not allowed to return null"));
                        } else if (!Objects.equals(updatedCloudEvent, currentCloudEvent)) {
                            String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                            long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                            Document updatedDocument = OccurrentCloudEventMongoDocumentMapper.convertToDocument(timeRepresentation, streamId, streamVersion, updatedCloudEvent);
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
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Flux.error(capabilityError(STREAM));
        }
        requireNonNull(filter, "Filter cannot be null");
        final Query query = queryOptions.apply(FilterConverter.convertFilterToQuery(timeRepresentation, filter));
        return readCloudEvents(query, skip, limit, sortBy)
                .map(document -> convertToCloudEvent(timeRepresentation, document));
    }

    @Override
    public Mono<Long> count(Filter filter) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            return mongoTemplate.createMono(eventStoreCollectionName, MongoCollection::estimatedDocumentCount);
        } else {
            final Query query = queryOptions.apply(FilterConverter.convertFilterToQuery(timeRepresentation, filter));
            return mongoTemplate.count(query, eventStoreCollectionName);
        }
    }

    @Override
    public Mono<Boolean> exists(Filter filter) {
        if (!eventStoreCapabilities.contains(STREAM)) {
            return Mono.error(capabilityError(STREAM));
        }
        requireNonNull(filter, "Filter cannot be null");
        if (filter instanceof Filter.All) {
            return count().map(cnt -> cnt > 0);
        } else {
            final Query query = queryOptions.apply(FilterConverter.convertFilterToQuery(timeRepresentation, filter));
            return mongoTemplate.exists(query, eventStoreCollectionName);
        }
    }

    @NullUnmarked
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
        return Query.query(streamIdEqualToCriteria(streamId));
    }

    private static Criteria streamIdEqualToCriteria(String streamId) {
        return where(STREAM_ID).is(streamId);
    }

    private static Query streamIdAndStreamVersionLessThanOrEqualTo(String streamId, long streamVersion) {
        return Query.query(streamIdEqualToCriteria(streamId).and(STREAM_VERSION).lte(streamVersion));
    }

    private static Query cloudEventIdIs(String cloudEventId, URI cloudEventSource) {
        return Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource));
    }

    private static Mono<Long> validateWriteCondition(String streamId, WriteCondition writeCondition, Long currentStreamVersion) {
        final Mono<Long> result;
        if (isFulfilled(currentStreamVersion, writeCondition)) {
            result = Mono.just(currentStreamVersion);
        } else {
            result = Mono.error(new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition, currentStreamVersion)));
        }
        return result;
    }

    private Flux<Document> convertEventsToMongoDocuments(String streamId, Flux<CloudEvent> events, Long currentStreamVersion) {
        return infiniteFluxFrom(currentStreamVersion)
                .zipWith(events)
                .map(streamVersionAndEvent -> {
                    long streamVersion = streamVersionAndEvent.getT1();
                    CloudEvent event = streamVersionAndEvent.getT2();
                    return OccurrentCloudEventMongoDocumentMapper.convertToDocument(timeRepresentation, streamId, streamVersion, event);
                });
    }

    private static Flux<Long> infiniteFluxFrom(Long currentStreamVersion) {
        return Flux.generate(() -> currentStreamVersion, (version, sink) -> {
            long nextVersion = version + 1L;
            sink.next(nextVersion);
            return nextVersion;
        });
    }
}
