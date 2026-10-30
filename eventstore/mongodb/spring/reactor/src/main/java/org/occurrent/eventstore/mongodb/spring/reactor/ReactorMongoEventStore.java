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

import com.mongodb.ClientSessionOptions;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReadScope;
import org.occurrent.eventstore.api.dcb.reactor.internal.DcbCatchupReads;
import org.occurrent.eventstore.api.dcb.reactor.DcbAppendConfirmation;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.internal.PositionBackfillValidator;
import org.occurrent.eventstore.api.internal.UpdateEventRepairValidator;
import org.occurrent.eventstore.api.internal.StreamReadFilterToFilterMapper;
import org.occurrent.eventstore.api.internal.StreamReadFilterValidator;
import org.occurrent.eventstore.api.internal.UpdateEventFunctionValidator;
import org.occurrent.eventstore.api.reactor.*;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.api.reactor.internal.CatchupReads;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerVersionsDocuments;
import org.occurrent.eventstore.mongodb.dcb.internal.MarkerVersionsReadFailures;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbTagsIndexCheck;
import org.occurrent.eventstore.mongodb.dcb.internal.PositionDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.UpdateEventDamage;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator;
import org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.WriteContext;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.eventstore.mongodb.internal.StreamVersionDiff;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.spring.filterqueryconversion.internal.FilterConverter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.ReactiveMongoDatabaseUtils;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveBulkOperations;
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
import java.util.*;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_ID;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.STREAM_VERSION;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.isDuplicateKeyErrorOnStreamVersionIndex;
import static org.occurrent.eventstore.mongodb.internal.MongoExceptionTranslator.translateException;
import static org.occurrent.mongodb.spring.sortconversion.internal.SortConverter.convertToSpringSort;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.SessionSynchronization.ALWAYS;
import static org.springframework.data.mongodb.SessionSynchronization.NEVER;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * This is a reactive {@link EventStore} implementation that stores events in MongoDB using
 * Spring's {@link ReactiveMongoTemplate} that is based on <a href="https://projectreactor.io/">project reactor</a>.
 * It also supports the {@link EventStoreOperations} and {@link EventStoreQueries} contracts, and the reactive
 * {@link DcbEventStore} contract when the {@code DCB} capability is enabled.
 * <p>
 * By default, only stream-based event-store operations are enabled. Configure
 * {@link EventStoreConfig.Builder#eventStoreCapabilities(Set)} to enable DCB, or to enable both stream and DCB
 * operations. Occurrent creates missing indexes for enabled capabilities, but it never removes indexes automatically.
 * <p>
 * A DCB read that asks for marker versions runs in a read-only snapshot transaction of its own, on a session from the
 * template's {@link org.springframework.data.mongodb.ReactiveMongoDatabaseFactory}, bounded by
 * {@link EventStoreConfig.Builder#markerVersionsCommitTimeout(Duration)}. Inside a reactive transaction the subscriber
 * context carries, it reports no marker versions, since that transaction's writes may still abort, and a read after
 * marker versions is refused. The store answers {@link DcbAppendConfirmation} with a
 * majority read on the primary from {@link org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#getMongoDatabase()},
 * which joins no session. A store built on a template from {@link ReactiveMongoTemplate#withSession(ClientSession)}
 * isn't supported for DCB snapshot deciders.
 */
@NullMarked
public class ReactorMongoEventStore implements EventStore, EventStoreOperations, EventStoreQueries, ReadEventStreamWithFilter, DcbEventStore, PositionOrderedReader, DcbAppendConfirmation, CatchupReads, DcbCatchupReads {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactorMongoEventStore.class);
    private static final String ID = "_id";

    private static final Retry TRANSIENT_CONFLICT_RETRY = Retry.backoff(15, Duration.ofMillis(10))
            .maxBackoff(Duration.ofMillis(500))
            .filter(throwable -> isTransientTransactionError(throwable) || isDuplicateKeyError(throwable))
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    private static final Retry COLD_START_COUNTER_RETRY = Retry.fixedDelay(5, Duration.ofMillis(20))
            .filter(ReactorMongoEventStore::isDuplicateKeyError);

    // An "any stream version" write cannot legitimately fail on a version race, so a conflict from a concurrent writer
    // is retried rather than surfaced. Applied only when the condition really is any-version, since a caller who asked
    // for a specific version wants to hear about the conflict. Bounded with backoff rather than the blocking stores'
    // unbounded RetryStrategy.retry() default, because retrying a reactive pipeline with neither a delay nor a limit
    // spins a scheduler thread. Fifteen attempts over a rising delay settle any contention a single stream can produce.
    private static final Retry ANY_STREAM_VERSION_CONFLICT_RETRY = Retry.backoff(15, Duration.ofMillis(10))
            .maxBackoff(Duration.ofMillis(500))
            .filter(throwable -> throwable instanceof WriteConditionNotFulfilledException)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

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
    private final boolean streamPositionEnabled;
    private final boolean requireBackfilledPosition;
    private final boolean requireRepairedEvents;
    private final boolean resumeFromMarkerVersions;
    private final boolean appendCheckMayQueryEvents;
    private final TransactionOptions markerVersionsTransactionOptions;
    private final MarkerVersionsReadFailures markerVersionsReadFailures = new MarkerVersionsReadFailures();

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
        // Resolve the effective stream-position setting before the position collection and index are created, so an
        // upgrade over an existing un-backfilled collection does not build the position index at startup.
        this.streamPositionEnabled = resolveStreamPositionEnabled(config, eventStoreCollectionName, mongoTemplate);
        this.requireBackfilledPosition = config.requireBackfilledPosition;
        this.requireRepairedEvents = config.requireRepairedEvents;
        this.resumeFromMarkerVersions = config.resumeFromMarkerVersions;
        this.markerVersionsTransactionOptions = DcbMarkerVersionsDocuments.readOnlySnapshotTransactionOptions(config.markerVersionsCommitTimeout);
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        if (dcbEnabled && !resumeFromMarkerVersions) {
            LOGGER.warn("resumeFromMarkerVersions is off for this event store, so DCB reads report no marker versions and DCB reads after marker versions are refused.");
        }
        this.appendCheckMayQueryEvents = dcbEnabled && DcbAppendConditionCheck.queryAllowed(command -> mongoTemplate.executeCommand(command).block());
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

        // Collect the events first so the count is known before the transaction, then reserve the position block
        // outside the transaction, like DCB does (see reservePositions), so the shared counter does not become a
        // transaction write-write conflict. The block is reused if the transaction retries, and a write that never
        // commits abandons it, so positions may have gaps. Reserve only when the store writes position and there is
        // at least one event.
        return events.collectList().flatMap(cachedEvents -> {
            if (cachedEvents.stream().anyMatch(DcbCloudEvents::isDcbEvent)) {
                return Mono.error(dcbTaggedEventOnStreamWriteError());
            }
            Mono<Long> firstReservedPosition = writesPosition() && !cachedEvents.isEmpty()
                    ? reservePositions(cachedEvents.size())
                    : Mono.just(0L);
            // A pure local computation, not a store round trip like the position reservation above, so it is minted
            // directly rather than wrapped in a Mono. Absent when there is nothing to stamp it on, so a call that
            // persists no events reports no append id (ADR 132, decision 4).
            Optional<AppendId> appendId = cachedEvents.isEmpty() ? Optional.empty() : Optional.of(AppendId.mint());
            return firstReservedPosition.flatMap(reservedPosition -> {
                Mono<StreamVersionDiff> operation = currentStreamVersion(streamId)
                        .flatMap(currentStreamVersion -> validateWriteCondition(streamId, writeCondition, currentStreamVersion))
                        .flatMap(currentStreamVersion -> {
                            Flux<Document> documentFlux = convertEventsToMongoDocuments(streamId, Flux.fromIterable(cachedEvents), currentStreamVersion);
                            Mono<StreamVersionDiff> streamVersionDiffFlux = documentFlux.collectList().flatMap(documents -> {
                                List<Document> stampedDocuments = stampAppendId(stampStreamPositions(documents, reservedPosition), appendId);
                                final long newStreamVersion;
                                if (stampedDocuments.isEmpty()) {
                                    newStreamVersion = currentStreamVersion;
                                } else {
                                    newStreamVersion = stampedDocuments.getLast().getLong(STREAM_VERSION);
                                }
                                return insertAll(streamId, currentStreamVersion, writeCondition, stampedDocuments)
                                        .then(Mono.just(StreamVersionDiff.of(currentStreamVersion, newStreamVersion)));
                            });
                            return streamVersionDiffFlux.switchIfEmpty(Mono.just(StreamVersionDiff.of(currentStreamVersion, currentStreamVersion)));
                        });

                Mono<StreamVersionDiff> transaction = transactionalOperator.transactional(operation);
                if (writeCondition.isAnyStreamVersion()) {
                    transaction = retryOnlyWhenThisStoreOwnsTheTransaction(transaction, ANY_STREAM_VERSION_CONFLICT_RETRY);
                }
                return transaction
                        .map(streamVersionDiff -> new WriteResult(streamId, streamVersionDiff.oldStreamVersion, streamVersionDiff.newStreamVersion, appendId));
            });
        });
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

    @Override
    public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return options.versionsRequested() ? readWithMarkerVersions(criteria, options) : readWithoutMarkerVersions(criteria, options);
    }

    private Mono<DcbEventStream> readWithoutMarkerVersions(DcbCriteria criteria, DcbReadOptions options) {
        return readWithoutMarkerVersions(criteria, options, consistencyToken(criteria, mongoTemplate), currentPosition(), mongoTemplate, queryOptions);
    }

    // The token, the head, the template and the query options are passed in so a catch-up read scope can read all of them
    // through its own session at its own read concern
    private Mono<DcbEventStream> readWithoutMarkerVersions(DcbCriteria criteria, DcbReadOptions options, Mono<DcbConsistencyToken> consistencyToken, Mono<Long> currentPosition, ReactiveMongoOperations template, Function<Query, Query> readQueryOptions) {
        // Snapshot the consistency token BEFORE reading the events. If an append commits between these two reads,
        // the events may include it while the token does not, which only makes a later conditional append
        // over-cautious (a false conflict that retries) rather than miss the conflict (ADR 0031). The position comes
        // after the token. An append between a position read first and a token read second would be in the token and
        // above the position the events are capped at, so a later conditional append would miss it.
        return consistencyToken.zipWhen(token -> currentPosition)
                .flatMap(tokenAndHighWatermark -> {
                    DcbConsistencyToken token = tokenAndHighWatermark.getT1();
                    long highWatermark = tokenAndHighWatermark.getT2();
                    long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
                    Query mongoQuery = toDcbMongoQuery(criteria, options.afterPosition().orElse(0), upperBound);
                    boolean backward = options.direction() == DcbReadOptions.Direction.BACKWARD;
                    mongoQuery.with(Sort.by(backward ? Sort.Direction.DESC : Sort.Direction.ASC, OccurrentCloudEventExtension.POSITION));
                    mongoQuery.skip(options.skip());
                    options.limit().ifPresent(mongoQuery::limit);
                    Flux<CloudEvent> cloudEvents = template.find(readQueryOptions.apply(mongoQuery), Document.class, eventStoreCollectionName)
                            .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
                    Mono<List<CloudEvent>> events = backward
                            ? cloudEvents.collectList().map(list -> {
                                Collections.reverse(list);
                                return list;
                            })
                            : cloudEvents.collectList();
                    return events.map(list -> new DcbEventStream(list, highWatermark, token));
                });
    }

    // Markers, counter and events come from one point in time, so the reported versions cover exactly the events read
    // and a read after them misses none. This runs in a read-only snapshot transaction on a session of the store's own,
    // never through the configured TransactionalOperator, and commits it once by hand. The majority commit makes sure
    // the snapshot was majority-committed. Inside a reactive transaction it reports no versions, since the
    // transaction's own increments may still abort and a later append would reuse them. A failure reports no versions
    // instead of failing the read. A read after versions can't fall back to a full read without double-applying
    // events, so it is refused in both cases.
    private Mono<DcbEventStream> readWithMarkerVersions(DcbCriteria criteria, DcbReadOptions options) {
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        if (!resumeFromMarkerVersions) {
            return afterVersions.isPresent()
                    ? Mono.error(new DcbSnapshotInvalidException(criteria, afterVersions.get(), "Resuming from marker versions is turned off for this event store (resumeFromMarkerVersions)"))
                    : readWithoutMarkerVersions(criteria, options);
        }
        if (criteria instanceof DcbCriteria.MatchAll) {
            return Mono.defer(() -> {
                afterVersions.ifPresent(saved -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, Map.of()));
                return readWithoutMarkerVersions(criteria, options);
            });
        }
        return ReactiveMongoDatabaseUtils.isTransactionActive(mongoTemplate.getMongoDatabaseFactory()).flatMap(callerTransactionActive -> {
            if (callerTransactionActive) {
                return markerVersionsUnavailable(criteria, options, "The read runs inside a transaction the caller owns, whose writes may still abort", null);
            }
            Mono<DcbEventStream> ownTransaction = Mono.usingWhen(
                    mongoTemplate.getMongoDatabaseFactory().getSession(ClientSessionOptions.builder().build())
                            .onErrorMap(RuntimeException.class, e -> new MarkerVersionsReadFailure("start session", e)),
                    clientSession -> Mono.defer(() -> {
                        clientSession.startTransaction(markerVersionsTransactionOptions);
                        return readMarkerVersionsWith(mongoTemplate.withSession(clientSession), criteria, options);
                    }).onErrorMap(e -> e instanceof RuntimeException && !(e instanceof DcbSnapshotInvalidException), e -> new MarkerVersionsReadFailure("read", e))
                            // A failed commit leaves the session in the committed state, so closing it sends no abort
                            .flatMap(eventStream -> Mono.from(clientSession.commitTransaction())
                                    .onErrorMap(RuntimeException.class, e -> new MarkerVersionsReadFailure("commit", e))
                                    .thenReturn(eventStream)),
                    // Closing the session aborts a read-only transaction that is still open
                    clientSession -> Mono.fromRunnable(clientSession::close),
                    (clientSession, error) -> Mono.fromRunnable(clientSession::close),
                    clientSession -> Mono.fromRunnable(clientSession::close));
            return ownTransaction.onErrorResume(MarkerVersionsReadFailure.class, failure -> markerVersionsReadFailed(criteria, options, failure.phase, requireNonNull(failure.getCause())));
        });
    }

    private Mono<DcbEventStream> readMarkerVersionsWith(ReactiveMongoOperations template, DcbCriteria criteria, DcbReadOptions options) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(criteria);
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        return template.find(markersQuery(markerKeys), Document.class, dcbCheckpointCollectionName).collectList()
                .map(DcbMarkerVersionsDocuments::markersByKey)
                .flatMap(markers -> {
                    Map<String, DcbConsistencyToken.MarkerObservation> observed = DcbMarkerVersionsDocuments.observe(markerKeys, markers);
                    afterVersions.ifPresent(saved -> DcbMarkerVersionsSupport.requireResumable(criteria, saved, observed));
                    DcbConsistencyToken token = DcbMarkerVersionsDocuments.consistencyToken(criteria, markers);
                    return template.findById(DcbMarkerModel.POSITION_DOCUMENT_ID, Document.class, dcbPositionCollectionName)
                            .map(counter -> ((Number) counter.get(DcbMarkerModel.COUNTER_POSITION)).longValue())
                            .defaultIfEmpty(0L)
                            .flatMap(highWatermark -> {
                                long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
                                Query mongoQuery = afterVersions
                                        .map(saved -> toDcbMongoQuery(criteria, 0, upperBound, item -> afterVersionsCriteria(item, markers, saved)))
                                        .orElseGet(() -> toDcbMongoQuery(criteria, options.afterPosition().orElse(0), upperBound));
                                boolean backward = options.direction() == DcbReadOptions.Direction.BACKWARD;
                                mongoQuery.with(Sort.by(backward ? Sort.Direction.DESC : Sort.Direction.ASC, OccurrentCloudEventExtension.POSITION));
                                mongoQuery.skip(options.skip());
                                options.limit().ifPresent(mongoQuery::limit);
                                Optional<DcbMarkerVersions> markerVersions = DcbMarkerVersionsSupport.mayReportVersions(options) ? DcbMarkerVersionsSupport.versionsOf(markerKeys, observed) : Optional.empty();
                                return template.find(queryOptions.apply(mongoQuery), Document.class, eventStoreCollectionName)
                                        .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document))
                                        .collectList()
                                        .map(list -> {
                                            if (backward) {
                                                Collections.reverse(list);
                                            }
                                            return new DcbEventStream(list, highWatermark, token, markerVersions);
                                        });
                            });
                });
    }

    private Mono<DcbEventStream> markerVersionsReadFailed(DcbCriteria criteria, DcbReadOptions options, String phase, Throwable cause) {
        if (markerVersionsReadFailures.recordAndCheckFirst(phase, cause)) {
            LOGGER.warn(MarkerVersionsReadFailures.message(phase, cause, options.afterVersions().isPresent()), cause);
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(MarkerVersionsReadFailures.message(phase, cause, options.afterVersions().isPresent()), cause);
        }
        return markerVersionsUnavailable(criteria, options, "The event store could not take a consistent snapshot to compare the saved marker versions against (" + phase + " failed)", cause);
    }

    private Mono<DcbEventStream> markerVersionsUnavailable(DcbCriteria criteria, DcbReadOptions options, String reason, @Nullable Throwable cause) {
        Optional<DcbMarkerVersions> afterVersions = options.afterVersions();
        return afterVersions.isPresent()
                ? Mono.error(new DcbSnapshotInvalidException(criteria, afterVersions.get(), reason, cause))
                : readWithoutMarkerVersions(criteria, options);
    }

    // Carries the phase a read with marker versions failed in to the fallback, and never leaves this class
    private static final class MarkerVersionsReadFailure extends RuntimeException {
        private final String phase;

        private MarkerVersionsReadFailure(String phase, Throwable cause) {
            super(phase, cause, false, false);
            this.phase = phase;
        }
    }

    @Override
    public Mono<Boolean> isConfirmed(DcbAppendResult appendResult) {
        requireNonNull(appendResult, DcbAppendResult.class.getSimpleName() + " cannot be null");
        Optional<AppendId> appendId = appendResult.appendId();
        if (appendId.isEmpty()) {
            return Mono.just(false);
        }
        Bson filter = Filters.and(
                Filters.eq(OccurrentCloudEventExtension.APPEND_ID, appendId.get().toString()),
                Filters.gte(OccurrentCloudEventExtension.POSITION, appendResult.firstSequencePosition()),
                Filters.lte(OccurrentCloudEventExtension.POSITION, appendResult.lastSequencePosition()));
        return Mono.defer(() -> mongoTemplate.getMongoDatabaseFactory().getMongoDatabase())
                .flatMap(database -> Mono.from(database.getCollection(eventStoreCollectionName)
                        .withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                        .find(filter).projection(Projections.include(ID)).limit(1).first()))
                .hasElement()
                .onErrorResume(e -> {
                    LOGGER.debug("Could not confirm DCB append {}, so it counts as unconfirmed", appendId.get(), e);
                    return Mono.just(false);
                });
    }

    private static Query markersQuery(Set<String> markerKeys) {
        return new Query(where(ID).in(markerKeys.stream().map(DcbMarkerModel::markerId).toList()));
    }

    private static Criteria afterVersionsCriteria(DcbCriterion item, Map<String, Document> markers, DcbMarkerVersions saved) {
        return markerVersionsCriteria(DcbMarkerVersionsDocuments.selections(item, markers, saved));
    }

    private static Criteria markerVersionsCriteria(List<DcbMarkerVersionsDocuments.Selection> selections) {
        List<Criteria> elements = selections.stream()
                .map(selection -> where(DcbMarkerModel.EVENT_MARKER_VERSIONS).elemMatch(
                        where(DcbMarkerModel.EVENT_MARKER_VERSIONS_KEY).is(selection.key())
                                .and(DcbMarkerModel.EVENT_MARKER_VERSIONS_EPOCH).is(selection.epoch())
                                .and(DcbMarkerModel.EVENT_MARKER_VERSIONS_VERSION).gt(selection.version())))
                .toList();
        return elements.size() == 1 ? elements.getFirst() : new Criteria().orOperator(elements);
    }

    @Override
    public Mono<Boolean> exists(DcbCriteria criteria, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return currentPosition().flatMap(highWatermark -> {
            long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
            return mongoTemplate.exists(queryOptions.apply(toDcbMongoQuery(criteria, options.afterPosition().orElse(0), upperBound)), eventStoreCollectionName);
        });
    }

    @Override
    public Mono<Long> count(DcbCriteria criteria, DcbReadOptions options) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return currentPosition().flatMap(highWatermark -> {
            long upperBound = Math.min(highWatermark, options.upToPosition().orElse(highWatermark));
            return mongoTemplate.count(queryOptions.apply(toDcbMongoQuery(criteria, options.afterPosition().orElse(0), upperBound)), eventStoreCollectionName);
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
        // Place by the condition's boundary tags when it constrains tags, so the same boundary always lands
        // in the same partition regardless of per-event tags. Otherwise fall back to the events' tags, so
        // tagless boundaries do not all collapse onto one hot partition.
        Set<Tag> conditionTags = condition == null ? Set.of() : DcbCloudEvents.tagsOf(condition.criteria());
        Set<Tag> placementTags = conditionTags.isEmpty() ? DcbMarkerModel.tagsOf(eventsToAppend) : conditionTags;
        String streamId = requireNonNull(dcbStreamIdGenerator.generateStreamId(placementTags), "DcbStreamIdGenerator returned a null stream id");
        int eventCount = eventsToAppend.size();

        // Reserve the position block once, before the transaction body. When this store owns the transaction the
        // counter findAndModify runs outside it, as a single atomic document update MongoDB serializes without raising
        // a transaction conflict, and the reserved block is reused across transient-transaction-error retries. When an
        // outer transaction is already active the operator joins it, so the counter update joins it too and the counter
        // document becomes a conflict point shared by every concurrent append in that transaction. Either way a doomed
        // or condition-failed append abandons its block, so position may have gaps (DCB permits this, see ADR 0021).
        return Mono.defer(() -> {
            // A DCB append always persists at least one event (validateDcbEvents above refuses an empty list), so this
            // is minted unconditionally, unlike the stream write path. Minted inside this defer, per subscription, so
            // a reused publisher gets a fresh id for every append execution instead of reusing the one from its first.
            AppendId appendId = AppendId.mint();

            return reservePositions(eventCount).flatMap(firstPosition -> {
                long lastPosition = firstPosition + eventCount - 1;
                Mono<DcbAppendResult> transaction = transactionalOperator.transactional(
                        currentStreamVersion(streamId).flatMap(currentStreamVersion -> {
                            final Mono<Map<String, Document>> conditionAndMarkers;
                            if (condition != null) {
                                conditionAndMarkers = enforceAppendCondition(condition, eventsToAppend, lastPosition);
                            } else {
                                // An unconditional append still increments its events' markers, so a concurrent conditional append whose
                                // criteria match one of these events, other than DcbCriteria.all() (ADR 0030), shares a marker with it,
                                // serializes against it, and its check observes it. Without this, nothing forces a write-write conflict
                                // and a concurrent conditional append's snapshot could miss this append (write skew). See ADR 0021.
                                conditionAndMarkers = incrementConflictMarkers(DcbMarkerModel.eventMarkerKeys(eventsToAppend), lastPosition);
                            }
                            return conditionAndMarkers.flatMap(markers -> {
                                List<Document> documents = convertDcbCloudEventsToDocuments(streamId, eventsToAppend, currentStreamVersion, firstPosition, appendId);
                                DcbMarkerVersionsDocuments.stampMarkerVersions(documents, eventsToAppend, markers);
                                Optional<DcbMarkerVersions> markerVersions = resumeFromMarkerVersions ? DcbMarkerVersionsDocuments.appendedVersions(condition, markers) : Optional.empty();
                                return insertAllDcb(streamId, currentStreamVersion, documents).thenReturn(new DcbAppendResult(firstPosition, lastPosition, eventCount, Optional.of(appendId), markerVersions));
                            });
                        }));
                // The driver does not auto-retry a transient transaction conflict reactively, so retry it here, plus a
                // DuplicateKeyException from two transactions first-creating the same conflict marker at once. A
                // DcbAppendConditionNotFulfilledException and a DuplicateCloudEventException are deliberately not retried.
                return retryOnlyWhenThisStoreOwnsTheTransaction(transaction, TRANSIENT_CONFLICT_RETRY);
            });
        });
    }

    private Mono<Map<String, Document>> enforceAppendCondition(DcbAppendCondition condition, List<CloudEvent> eventsToAppend, long lastPosition) {
        Optional<DcbConsistencyToken> expectedToken = condition.consistencyToken();
        final Mono<Boolean> conflictMono;
        if (expectedToken.isPresent()) {
            // The condition carries the consistency token the command observed when it read the query. The check
            // compares each of the query's markers with what the read saw of it, and then queries the events above those
            // versions for an item that needs it (ADR 0149). Immune to read-watermark overshoot because marker versions
            // bump inside the append transaction.
            conflictMono = queryMarkers(condition.criteria(), mongoTemplate).flatMap(markers -> {
                Map<String, DcbConsistencyToken.MarkerObservation> current = DcbMarkerVersionsDocuments.observe(DcbMarkerModel.queryMarkerKeys(condition.criteria()), markers);
                return switch (DcbAppendConditionCheck.check(expectedToken.get(), condition.criteria(), current, appendCheckMayQueryEvents)) {
                    case DcbAppendConditionCheck.Pass ignored -> Mono.just(false);
                    case DcbAppendConditionCheck.Refuse ignored -> Mono.just(true);
                    case DcbAppendConditionCheck.QueryEvents query -> mongoTemplate.exists(new Query(new Criteria().orOperator(query.items().stream()
                            .map(item -> new Criteria().andOperator(toCriteria(item.item()), markerVersionsCriteria(DcbMarkerVersionsDocuments.selections(item, markers))))
                            .toList())), eventStoreCollectionName);
                };
            });
        } else {
            // No token: an absolute "fail if any matching event exists" guard. The marker increments below still
            // serialize concurrent unconditional guards on the same boundary so two of them cannot both pass.
            conflictMono = mongoTemplate.exists(toDcbMongoQuery(condition.criteria(), 0, Long.MAX_VALUE), eventStoreCollectionName);
        }
        return conflictMono.flatMap(conflict -> {
            if (conflict) {
                return currentPosition().flatMap(position -> Mono.<Map<String, Document>>error(new DcbAppendConditionNotFulfilledException(condition, position)));
            }
            // Increment a marker per key for the union of the query's keys and the appended events' keys. Always
            // increment the query's markers so a concurrent append whose events match the query is serialized even
            // when this append's own events do not match it. A query with DcbCriteria.all() is the exception, since no
            // event has the match-all key among its own keys (ADR 0030).
            TreeSet<String> markerKeys = new TreeSet<>(DcbMarkerModel.queryMarkerKeys(condition.criteria()));
            markerKeys.addAll(DcbMarkerModel.eventMarkerKeys(eventsToAppend));
            return incrementConflictMarkers(markerKeys, lastPosition);
        });
    }

    // Emits the incremented markers as this transaction left them, keyed by marker key, so the append stamps its
    // events with the versions it gave them (ADR 148).
    private Mono<Map<String, Document>> incrementConflictMarkers(Set<String> markerKeys, long lastPosition) {
        if (markerKeys.isEmpty()) {
            return Mono.just(Map.of());
        }
        // One unordered bulk write rather than one round trip per key (previously sequential via concatMap), so a
        // boundary with several tags and types costs one round trip inside the transaction instead of K serial ones.
        // Each marker gets its upsert and an update that gives a marker an older writer created an epoch. In either
        // order the two leave it with one epoch.
        ReactiveBulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, dcbCheckpointCollectionName);
        for (String key : markerKeys) {
            Query query = new Query(where(ID).is(DcbMarkerModel.markerId(key)));
            Update update = new Update()
                    .inc(DcbMarkerModel.CHECKPOINT_VERSION, 1L)
                    .inc(DcbMarkerModel.CHECKPOINT_STAMPED, 1L)
                    .set(DcbMarkerModel.CHECKPOINT_LAST_POSITION, lastPosition)
                    .setOnInsert(DcbMarkerModel.CHECKPOINT_EPOCH, new ObjectId());
            bulkOperations.upsert(query, update);
            Query withoutEpoch = new Query(where(ID).is(DcbMarkerModel.markerId(key)).and(DcbMarkerModel.CHECKPOINT_EPOCH).exists(false));
            bulkOperations.updateOne(withoutEpoch, new Update().set(DcbMarkerModel.CHECKPOINT_EPOCH, new ObjectId()));
        }
        return bulkOperations.execute()
                .then(mongoTemplate.find(markersQuery(markerKeys), Document.class, dcbCheckpointCollectionName).collectList())
                .map(DcbMarkerVersionsDocuments::markersByKey);
    }

    // The optimistic-concurrency token for a query: what the read saw of each of its conflict markers
    private Mono<DcbConsistencyToken> consistencyToken(DcbCriteria criteria, ReactiveMongoOperations template) {
        return queryMarkers(criteria, template).map(markers -> DcbMarkerVersionsDocuments.consistencyToken(criteria, markers));
    }

    // Read the markers in one query, which narrows the window a concurrent append can tear across and doesn't close
    // it. A find can yield between documents, and through a mongos it reads each shard on its own (ADR 0149).
    private Mono<Map<String, Document>> queryMarkers(DcbCriteria criteria, ReactiveMongoOperations template) {
        Set<String> markerKeys = DcbMarkerModel.queryMarkerKeys(criteria);
        if (markerKeys.isEmpty()) {
            return Mono.just(Map.of());
        }
        return template.find(markersQuery(markerKeys), Document.class, dcbCheckpointCollectionName).collectList().map(DcbMarkerVersionsDocuments::markersByKey);
    }

    private Mono<Long> reservePositions(int eventCount) {
        Query query = new Query(where(ID).is(DcbMarkerModel.POSITION_DOCUMENT_ID));
        Update update = new Update().inc(DcbMarkerModel.COUNTER_POSITION, eventCount);
        FindAndModifyOptions options = FindAndModifyOptions.options().upsert(true).returnNew(true);
        return mongoTemplate.findAndModify(query, update, options, Document.class, dcbPositionCollectionName)
                .map(updated -> ((Number) updated.get(DcbMarkerModel.COUNTER_POSITION)).longValue() - eventCount + 1)
                // Cold-start race: when the counter document does not exist yet, concurrent upserts all try to insert it
                // and all but one get a duplicate key. On retry the document exists and the upsert becomes an update.
                // Like the append retry this only runs when the store owns the transaction, because a duplicate inside
                // a joined transaction aborts it and no further attempt could commit.
                .transform(reserve -> retryOnlyWhenThisStoreOwnsTheTransaction(reserve, COLD_START_COUNTER_RETRY));
    }

    /**
     * Applies the retry spec only when this store owns the transaction. When one is already active the operator joins
     * it, a conflict aborts it, and every further attempt fails on its first read with {@code NoSuchTransaction}, so
     * retrying could never commit. Route every retry on the write path through here so the ownership check cannot be
     * forgotten. See ADR 0074.
     */
    private <T> Mono<T> retryOnlyWhenThisStoreOwnsTheTransaction(Mono<T> action, Retry retry) {
        return ReactiveMongoDatabaseUtils.isTransactionActive(mongoTemplate.getMongoDatabaseFactory())
                .flatMap(transactionAlreadyActive -> transactionAlreadyActive ? action : action.retryWhen(retry));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Mono<Long> currentPosition() {
        if (!writesPosition()) {
            return Mono.error(positionError());
        }
        return mongoTemplate.findById(DcbMarkerModel.POSITION_DOCUMENT_ID, Document.class, dcbPositionCollectionName)
                .map(document -> ((Number) document.get(DcbMarkerModel.COUNTER_POSITION)).longValue())
                .defaultIfEmpty(0L);
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

    private Mono<Void> insertAllDcb(String streamId, long streamVersion, List<Document> documents) {
        return mongoTemplate.insert(documents, eventStoreCollectionName)
                .onErrorResume(throwable -> {
                    // A transient transaction conflict is retried by the append retry rather than mapped to the
                    // stream-path WriteConditionNotFulfilledException, which DCB does not use.
                    if (isTransientTransactionError(throwable)) {
                        return Mono.error(throwable);
                    }
                    MongoException mongoException = findMongoException(throwable);
                    if (mongoException != null) {
                        // Two disjoint DCB boundaries that hash to the same partition stream race on the next stream
                        // version and one loses on the unique streamid+streamversion index. This is not a duplicate
                        // CloudEvent, so rethrow the raw duplicate-key error and let the append retry rerun the
                        // read-decide-append cycle.
                        if (isDuplicateKeyErrorOnStreamVersionIndex(mongoException)) {
                            return Mono.error(throwable);
                        }
                        return Mono.error(translateException(new WriteContext(streamId, streamVersion, WriteCondition.anyStreamVersion()), mongoException));
                    }
                    return Mono.error(throwable);
                })
                .then();
    }

    private static UnsupportedOperationException capabilityError(EventStoreCapability capability) {
        return new UnsupportedOperationException(capability + " capability is not enabled for this ReactorMongoEventStore");
    }

    /**
     * Error surfaced when a DCB-tagged event is written through the stream write path, regardless of which capabilities
     * are enabled. A dcbtags-carrying event written through write(...) would get no derived dcbTags array and no DCB
     * position, so it would be silently invisible to DCB reads. Enforcing this keeps the dcbtags extension and the
     * dcbTags array equivalent, which the capability filter relies on.
     */
    private static IllegalArgumentException dcbTaggedEventOnStreamWriteError() {
        return new IllegalArgumentException("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");
    }

    /**
     * Returns whether this store writes a global position, so position-requiring APIs are safe to call.
     */
    @Override
    public boolean writesPosition() {
        return eventStoreCapabilities.contains(DCB) || (eventStoreCapabilities.contains(STREAM) && streamPositionEnabled);
    }

    private static UnsupportedOperationException positionError() {
        return new UnsupportedOperationException("This ReactorMongoEventStore does not write a position. Enable DCB, or do not call withoutStreamPosition() on a STREAM-only store, to use position-requiring APIs.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(range, "Range cannot be null");
        if (!writesPosition()) {
            return Flux.error(positionError());
        }
        return readInPositionOrder(filter, range, currentPosition(), mongoTemplate, queryOptions);
    }

    // The head, the template and the query options are passed in so a catch-up read scope can read through its own
    // session at its own read concern
    private Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range, Mono<Long> currentPosition, ReactiveMongoOperations template, Function<Query, Query> readQueryOptions) {
        return currentPosition.flatMapMany(highWatermark -> {
            long upperBound = Math.min(highWatermark, range.upToPosition().orElse(highWatermark));
            long lowerBound = range.afterPosition().orElse(0L);
            Criteria positionCriteria = where(OccurrentCloudEventExtension.POSITION).gt(lowerBound).lte(upperBound);
            Criteria filterCriteria = FilterConverter.convertFilterToCriteria(null, timeRepresentation, filter);
            Query query = new Query(new Criteria().andOperator(positionCriteria, filterCriteria))
                    .with(Sort.by(Sort.Direction.ASC, OccurrentCloudEventExtension.POSITION));
            return template.find(readQueryOptions.apply(query), Document.class, eventStoreCollectionName)
                    .map(document -> DcbDocumentMapper.toCloudEvent(timeRepresentation, document));
        });
    }

    /**
     * Internal. Opens a {@link CatchupReadScope}, see {@link CatchupReads#openCatchupReadScope(String)}. The scope
     * reads through a causally consistent session of its own, never the caller's transaction, and every read in it uses
     * read concern majority. The session is closed with the scope.
     */
    @Override
    public Mono<CatchupReadScope> openCatchupReadScope(@Nullable String notBefore) {
        return openScope(notBefore).cast(CatchupReadScope.class);
    }

    /**
     * Internal. Opens a {@link DcbCatchupReadScope}, see {@link DcbCatchupReads#openDcbCatchupReadScope(String)}.
     * Errors with an {@link UnsupportedOperationException} if this store does not have the DCB capability.
     */
    @Override
    public Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore) {
        if (!eventStoreCapabilities.contains(DCB)) {
            return Mono.error(capabilityError(DCB));
        }
        return openScope(notBefore).cast(DcbCatchupReadScope.class);
    }

    private Mono<ReactorCatchupReadScope> openScope(@Nullable String notBefore) {
        return mongoTemplate.getMongoDatabaseFactory().getSession(ClientSessionOptions.builder().causallyConsistent(true).build())
                .flatMap(session -> {
                    ReactiveMongoOperations template = mongoTemplate.withSession(session);
                    // Never the session of a transaction the caller has open, since that transaction may abort
                    if (!(template instanceof ReactiveMongoTemplate sessionTemplate)) {
                        session.close();
                        return Mono.error(new IllegalStateException("Cannot open a catch-up read scope, because the session-bound template is a " + template.getClass().getName() + " and not a ReactiveMongoTemplate, so it can't be kept out of a transaction the caller has open."));
                    }
                    sessionTemplate.setSessionSynchronization(NEVER);
                    if (notBefore != null) {
                        // Advances to just before the live start and not to it, since the cluster time may not have reached it yet
                        // and the replay only needs what committed before it
                        OperationTimeLiveStart.tryParse(notBefore).map(OperationTimeLiveStart::oneIncrementBefore).ifPresent(session::advanceOperationTime);
                    }
                    // The read only moves the session's operation time to the one the live start is taken from
                    return counterRead(template)
                            .then(Mono.fromSupplier(() -> {
                                BsonTimestamp t0 = session.getOperationTime();
                                return new ReactorCatchupReadScope(session, template, OperationTimeLiveStart.liveStartAfterRead(t0));
                            }))
                            .doOnError(__ -> session.close())
                            .doOnCancel(session::close);
                });
    }

    private Mono<Document> counterRead(ReactiveMongoOperations template) {
        Query counter = new Query(where(ID).is(DcbMarkerModel.POSITION_DOCUMENT_ID)).withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary());
        return template.findOne(counter, Document.class, dcbPositionCollectionName);
    }

    // One causally consistent session for one replay attempt. The live start is the operation time of a majority read of
    // the position counter on the primary, plus one increment, and every replay read is a majority read in the same
    // session, so each of them sees every write that was majority-committed by then. The read concern is set after the
    // configured query options, so they can't lower it, and a read preference they set is kept for the replay reads.
    private final class ReactorCatchupReadScope implements CatchupReadScope, DcbCatchupReadScope {
        private final ClientSession session;
        private final ReactiveMongoOperations template;
        private final @Nullable String liveStart;

        private ReactorCatchupReadScope(ClientSession session, ReactiveMongoOperations template, @Nullable String liveStart) {
            this.session = session;
            this.template = template;
            this.liveStart = liveStart;
        }

        private Query majority(Query query) {
            return queryOptions.apply(query).withReadConcern(ReadConcern.MAJORITY);
        }

        @Override
        public @Nullable String liveStart() {
            return liveStart;
        }

        @Override
        public Mono<Long> currentPosition() {
            if (!writesPosition()) {
                return Mono.error(positionError());
            }
            return counterRead(template)
                    .map(document -> ((Number) document.get(DcbMarkerModel.COUNTER_POSITION)).longValue())
                    .defaultIfEmpty(0L);
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            requireNonNull(filter, "Filter cannot be null");
            requireNonNull(range, "Range cannot be null");
            if (!writesPosition()) {
                return Flux.error(positionError());
            }
            return ReactorMongoEventStore.this.readInPositionOrder(filter, range, currentPosition(), template, this::majority);
        }

        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            requireNonNull(criteria, "Criteria cannot be null");
            return Mono.defer(() -> {
                org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope.requirePositionRange(options);
                return readWithoutMarkerVersions(criteria, options, consistencyToken(criteria, template), currentPosition(), template, this::majority);
            });
        }

        @Override
        public Mono<Void> close() {
            return Mono.fromRunnable(session::close);
        }
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
        // the cold-start marker or position race and not a partition stream-version collision, and the driver
        // duplicate-key exception is its cause, so stop and do not retry once it appears in the chain.
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

    private static Query toDcbMongoQuery(DcbCriteria criteria, long afterPosition, long upperSequencePosition) {
        return toDcbMongoQuery(criteria, afterPosition, upperSequencePosition, null);
    }

    // itemRestriction narrows each item's criteria, so a read after marker versions selects every item by its own mv element
    private static Query toDcbMongoQuery(DcbCriteria criteria, long afterPosition, long upperSequencePosition, @Nullable Function<DcbCriterion, Criteria> itemRestriction) {
        Criteria positionCriteria = where(OccurrentCloudEventExtension.POSITION).gt(afterPosition).lte(upperSequencePosition);
        Criteria dcbEventCriteria = where(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD).exists(true);
        if (criteria instanceof DcbCriteria.MatchAll) {
            return new Query(new Criteria().andOperator(positionCriteria, dcbEventCriteria));
        }
        List<Criteria> itemCriteria = DcbMarkerModel.dcbQueryItems(criteria).stream()
                .map(item -> itemRestriction == null ? toCriteria(item) : new Criteria().andOperator(toCriteria(item), itemRestriction.apply(item)))
                .toList();
        return new Query(new Criteria().andOperator(positionCriteria, dcbEventCriteria, new Criteria().orOperator(itemCriteria)));
    }

    private static Criteria toCriteria(DcbCriterion item) {
        List<Criteria> criteria = new ArrayList<>();
        if (!item.types().isEmpty()) {
            criteria.add(where("type").in(item.types()));
        }
        if (!item.excludedTypes().isEmpty()) {
            criteria.add(where("type").nin(item.excludedTypes()));
        }
        if (!item.tags().isEmpty()) {
            criteria.add(where(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD).all(item.tags().stream().map(Tag::canonical).toList()));
        }
        return new Criteria().andOperator(criteria);
    }

    private Mono<EventStreamImpl> readEventStream(String streamId, @Nullable StreamReadFilter streamReadFilter, int skip, int limit) {
        if (skip < 0) {
            return Mono.error(new IllegalArgumentException("skip cannot be negative"));
        }
        return currentStreamVersion(streamId)
                .flatMap(currentStreamVersion -> {
                    // Uses "lte" currentStreamVersion instead of a transaction on read, so an event another thread
                    // inserts after currentStreamVersion is read does not matter. "skip" is folded into the version
                    // bound here, before the filter narrows the query, so it keeps counting stream positions
                    // instead of filtered documents.
                    Query query = streamIdAndStreamVersionBetween(streamId, skip, currentStreamVersion);
                    if (streamReadFilter != null) {
                        StreamReadFilterValidator.validate(streamReadFilter);
                        Filter mapped = StreamReadFilterToFilterMapper.map(streamReadFilter);
                        Criteria criteria = FilterConverter.convertFilterToCriteria(null, timeRepresentation, mapped);
                        query.addCriteria(criteria);
                    }
                    Flux<Document> cloudEventDocuments = readCloudEvents(readOptions.apply(query), 0, limit, SortBy.streamVersion(ASCENDING));
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

    // Decide at startup whether this store writes stream position. An explicit choice (withStreamPosition() or
    // withoutStreamPosition()) and DCB are honored as-is. When position is only on by default, turn it off if the
    // collection already holds events without a position, so upgrading an existing store does not build the position
    // index over the whole collection at startup. The constructor already blocks, so the probe blocks too.
    //
    // Turning position off here means writesPosition() is false, so the un-backfilled checks never run, and the damage
    // check runs only under requireRepairedEvents, where a damaged event fails startup instead of being warned about.
    // Without that setting this warning is the only one the operator sees. An event whose position updateEvent dropped
    // is enough to reach it, so the shared message includes the repair caveat rather than naming the backfill alone.
    private static boolean resolveStreamPositionEnabled(EventStoreConfig config, String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        if (!config.streamPositionEnabled) {
            return false;
        }
        if (config.eventStoreCapabilities.contains(DCB) || config.streamPositionExplicitlyEnabled) {
            return true;
        }
        if (hasPreExistingUnpositionedEvents(eventStoreCollectionName, mongoTemplate)) {
            LOGGER.warn(PositionBackfillValidator.positionDisabledByUnpositionedEventsMessage(eventStoreCollectionName));
            return false;
        }
        return true;
    }

    // A cheap probe for an existing un-backfilled store. Backfill assigns positions in _id order, oldest first, so if
    // the oldest event has no position the collection predates position and has not been backfilled. Sort by _id, which
    // is always indexed, so the probe does not need the position index.
    private static boolean hasPreExistingUnpositionedEvents(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        Boolean exists = mongoTemplate.collectionExists(eventStoreCollectionName).block();
        if (exists == null || !exists) {
            return false;
        }
        Query oldest = new Query().with(Sort.by(Sort.Direction.ASC, ID)).limit(1);
        oldest.fields().include(OccurrentCloudEventExtension.POSITION);
        Document oldestEvent = mongoTemplate.findOne(oldest, Document.class, eventStoreCollectionName).block();
        return oldestEvent != null && !oldestEvent.containsKey(OccurrentCloudEventExtension.POSITION);
    }

    private Mono<Void> initializeEventStore(String eventStoreCollectionName, String dcbPositionCollectionName, String dcbCheckpointCollectionName, Set<EventStoreCapability> eventStoreCapabilities, ReactiveMongoTemplate mongoTemplate) {
        boolean dcbEnabled = eventStoreCapabilities.contains(DCB);
        boolean writesPosition = writesPosition();

        // The CloudEvent spec requires id + source to be unique.
        Mono<Void> chain = createCollection(eventStoreCollectionName, mongoTemplate)
                .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending("id"), Indexes.ascending("source")), new IndexOptions().unique(true)))
                .then();

        // streamId + streamVersion uniqueness is a stream-mode invariant, but the DCB append path also looks up
        // the current stream version per partition, so it needs the same compound index to avoid a collection
        // scan. The index stays unique for DCB too, since DCB-only writes assign sequential per-partition stream
        // versions. The only collision is two disjoint DCB boundaries hashing to the same partition stream, which
        // the DCB append path treats as a retryable transient rather than a duplicate error. One identical unique
        // index for STREAM and DCB also means no capability combination or upgrade ever hits an IndexOptionsConflict.
        if (eventStoreCapabilities.contains(STREAM) || dcbEnabled) {
            chain = chain.then(createStreamVersionIndex(eventStoreCollectionName, mongoTemplate, new IndexOptions().unique(true))).then();
        }

        // The position counter collection and index are shared by DCB and by STREAM when position is enabled. The
        // index is sparse because events without a position (opt-out or not-yet-backfilled) carry no position field.
        if (writesPosition) {
            chain = chain
                    .then(createCollection(dcbPositionCollectionName, mongoTemplate))
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(OccurrentCloudEventExtension.POSITION), new IndexOptions().unique(true).sparse(true)))
                    .then();
        }

        if (dcbEnabled && eventStoreCapabilities.contains(STREAM)) {
            // Only DCB events have dcbTags, so with stream events in the collection this index narrows a match-all
            // DcbCriteria to the DCB events. (dcbTags, position) also holds every stream event with a position. On a
            // DCB-only store whose collection holds only DCB events, this index holds the same events as the position
            // index, so it narrows nothing.
            chain = chain.then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), new IndexOptions().sparse(true))).then();
        }

        if (dcbEnabled) {
            chain = chain
                    .then(createCollection(dcbCheckpointCollectionName, mongoTemplate))
                    // A type-only DcbCriteria has no tags to hit the dcbTags index with, so it falls back to the
                    // position index with type checked as a residual FETCH filter, examining every DCB event in the
                    // position range. A (type, position) compound index lets the planner satisfy the type equality
                    // and position sort directly from the index, so keysExamined tracks nReturned instead of the
                    // full position range. Evidence: explain("executionStats") on a 50k/50 skewed dataset showed
                    // docsExamined=50050 for nReturned=50 without this index.
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending("type"), Indexes.ascending(OccurrentCloudEventExtension.POSITION)), new IndexOptions().sparse(true)))
                    // The multikey dcbTags index alone cannot provide the position sort order, so a tag boundary read
                    // falls back to an in-memory (or, on MongoDB 6.0+, disk-spilling) SORT after fetching every
                    // matching document. A (dcbTags, position) compound index lets the planner read matches in
                    // position order directly instead. Evidence: explain("executionStats") on a 5,000-of-305,000
                    // skewed dataset (a plausible popular-tag boundary) showed a winning SORT stage over the dcbTags
                    // index without this compound index.
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, Indexes.compoundIndex(Indexes.ascending(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD), Indexes.ascending(OccurrentCloudEventExtension.POSITION)), new IndexOptions().sparse(true)))
                    // A read after marker versions selects events by one mv element per criteria item (ADR 148).
                    .then(createIndex(eventStoreCollectionName, mongoTemplate, DcbMarkerVersionsDocuments.markerVersionsIndex(), new IndexOptions().sparse(true)))
                    .then();
        }

        if (dcbEnabled && !eventStoreCapabilities.contains(STREAM)) {
            chain = chain.then(warnOnPositionedStreamEventsWithoutDcbTagsIndex(eventStoreCollectionName, mongoTemplate));
        }

        // Damage check first. The unpositioned check errors when requireBackfilledPosition is set, and an event
        // whose position updateEvent dropped has no position field either, so it would fail startup naming the
        // position backfill, and backfilling such an event assigns a wrong position for good.
        // requireRepairedEvents runs the damage check on a store that writes no position too, since the two ways
        // that happens are withoutStreamPosition() and the resolver turning position off over unpositioned history,
        // and an operator who asked to be refused meant both.
        if (writesPosition || requireRepairedEvents) {
            chain = chain.then(warnOrFailOnEventsDamagedByUpdateEvent(eventStoreCollectionName, mongoTemplate));
        }
        if (writesPosition) {
            chain = chain.then(warnIfUnpositionedEventsExist(eventStoreCollectionName, mongoTemplate));
        }

        // SessionSynchronization must be ALWAYS for TransactionTemplate to work with MongoTemplate. See
        // https://docs.spring.io/spring-data/mongodb/docs/current/reference/html/#mongo.transactions.transaction-template
        mongoTemplate.setSessionSynchronization(ALWAYS);

        return chain;
    }

    // Runs after the (dcbTags, position) index exists, since the lookup is hinted on it. A failed check only logs.
    // The indexes are listed only when a stream event exists, so a collection without one needs no listIndexes privilege.
    private static Mono<Void> warnOnPositionedStreamEventsWithoutDcbTagsIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.getCollection(eventStoreCollectionName)
                .flatMap(collection -> Mono.from(collection.find(DcbTagsIndexCheck.positionedStreamEvent())
                                .hint(DcbTagsIndexCheck.hint())
                                .min(DcbTagsIndexCheck.min())
                                .max(DcbTagsIndexCheck.max())
                                .limit(1)
                                .projection(Projections.include(ID))
                                .first())
                        .flatMap(positionedStreamEvent -> Flux.from(collection.listIndexes()).collectList())
                        .flatMap(indexes -> Mono.justOrEmpty(DcbTagsIndexCheck.warningFor(eventStoreCollectionName, indexes))))
                .doOnNext(LOGGER::warn)
                .then()
                .onErrorResume(RuntimeException.class, e -> {
                    LOGGER.warn(DcbTagsIndexCheck.checkFailedMessage(eventStoreCollectionName), e);
                    return Mono.empty();
                });
    }

    // Startup guard: when this store writes position but the event collection already has events without one, those
    // events are invisible to position-based catch-up. Warns, or fails when configured, so nobody silently runs with
    // un-backfilled history.
    private Mono<Void> warnIfUnpositionedEventsExist(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        Query unpositionedQuery = new Query(where(OccurrentCloudEventExtension.POSITION).exists(false));
        return mongoTemplate.exists(new Query(), eventStoreCollectionName).flatMap(collectionNonEmpty -> {
            if (!collectionNonEmpty) {
                return Mono.empty();
            }
            return mongoTemplate.exists(unpositionedQuery, eventStoreCollectionName).flatMap(hasUnpositionedEvents -> {
                if (!hasUnpositionedEvents) {
                    return Mono.empty();
                }
                if (requireBackfilledPosition) {
                    return Mono.error(PositionBackfillValidator.unpositionedEventsExist(eventStoreCollectionName));
                }
                LOGGER.warn(PositionBackfillValidator.unpositionedEventsMessage(eventStoreCollectionName));
                return Mono.empty();
            });
        });
    }

    // Warns, or errors when requireRepairedEvents is set, when the collection holds events whose position or tag
    // index is wrong. The warning looks for a string position only, what updateEvent wrote before 0.34.0, which reads no
    // index keys on a store that was never damaged. requireRepairedEvents refuses every event whose position is not a
    // positive integer, every event whose tag fields are not what an append writes, a counter no writer would
    // store and a position above the counter, a missing one counting as zero, at the cost of a collection scan.
    private Mono<Void> warnOrFailOnEventsDamagedByUpdateEvent(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        Bson damaged = requireRepairedEvents ? UpdateEventDamage.wrongPositionOrTagIndex() : UpdateEventDamage.positionStoredAsString();
        return mongoTemplate.execute(eventStoreCollectionName, collection ->
                        collection.find(damaged).limit(1).projection(Projections.include(ID)).first()).hasElements()
                .flatMap(hasDamagedEvents -> hasDamagedEvents || !requireRepairedEvents ? Mono.just(hasDamagedEvents) : wrongCounter(eventStoreCollectionName, mongoTemplate))
                .flatMap(hasDamagedEvents -> {
            if (!hasDamagedEvents) {
                return Mono.<Void>empty();
            }
            if (requireRepairedEvents) {
                return Mono.<Void>error(UpdateEventRepairValidator.damagedEventsExist(eventStoreCollectionName));
            }
            LOGGER.warn(UpdateEventRepairValidator.damagedEventsMessage(eventStoreCollectionName));
            return Mono.<Void>empty();
        });
    }

    // Reads the highest position before the counter, which is what keeps an append in flight from looking like a
    // position above it. UpdateEventDamage.wrongCounter says why.
    private Mono<Boolean> wrongCounter(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.execute(eventStoreCollectionName, collection ->
                        collection.find(UpdateEventDamage.positionIsANumber())
                                .sort(com.mongodb.client.model.Sorts.descending(OccurrentCloudEventExtension.POSITION))
                                .limit(1)
                                .projection(Projections.include(OccurrentCloudEventExtension.POSITION))
                                .first())
                .next()
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(highestPositioned -> mongoTemplate.findById(DcbMarkerModel.POSITION_DOCUMENT_ID, Document.class, dcbPositionCollectionName)
                        .map(Optional::of)
                        .defaultIfEmpty(Optional.empty())
                        .map(counter -> UpdateEventDamage.wrongCounter(highestPositioned.orElse(null), counter.orElse(null))));
    }

    private static Mono<String> createIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate, Bson index, IndexOptions indexOptions) {
        return mongoTemplate.getCollection(eventStoreCollectionName).flatMap(collection -> Mono.from(collection.createIndex(index, indexOptions)));
    }

    // The streamid+streamversion index already exists with options that clash with the unique one Occurrent needs
    // (older MongoDB reports this as error 85, 7.0+ as 86). Occurrent never replaces an index, so fail rather than
    // run without the uniqueness that stream and DCB writes depend on.
    private static Mono<String> createStreamVersionIndex(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate, IndexOptions indexOptions) {
        Bson index = Indexes.compoundIndex(Indexes.ascending(STREAM_ID), Indexes.ascending(STREAM_VERSION));
        return createIndex(eventStoreCollectionName, mongoTemplate, index, indexOptions)
                .onErrorResume(MongoCommandException.class, e -> {
                    if (e.getErrorCode() == 85 || e.getErrorCode() == 86) {
                        return Mono.error(new IllegalStateException("The '" + STREAM_ID + "_1_" + STREAM_VERSION + "_1' index already exists with options incompatible with the unique index Occurrent requires. Occurrent does not drop or replace existing indexes automatically, so running with the existing index would silently lose the uniqueness guarantee that stream and DCB writes rely on. Drop and recreate the index as unique out-of-band, then restart.", e));
                    }
                    return Mono.error(e);
                });
    }

    private static Mono<MongoCollection<Document>> createCollection(String eventStoreCollectionName, ReactiveMongoTemplate mongoTemplate) {
        return mongoTemplate.collectionExists(eventStoreCollectionName).flatMap(exists -> exists ? Mono.empty() : mongoTemplate.createCollection(eventStoreCollectionName));
    }

    private static Mono<EventStream<CloudEvent>> convertToCloudEvent(TimeRepresentation timeRepresentation, Mono<EventStreamImpl> eventStream) {
        return eventStream.map(es -> es.map(document -> convertToCloudEvent(timeRepresentation, document)));
    }

    private static CloudEvent convertToCloudEvent(TimeRepresentation timeRepresentation, Document document) {
        // Use the DCB-aware mapper so DCB storage fields (dcbTags, position) are not leaked as CloudEvent extensions
        // when reading a collection a DCB store also writes to. A no-op for plain stream events.
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
                    .flatMap(document -> {
                        CloudEvent currentCloudEvent = convertToCloudEvent(timeRepresentation, document);
                        CloudEvent updatedCloudEvent = fn.apply(currentCloudEvent);
                        final Mono<CloudEvent> result;
                        if (updatedCloudEvent == null) {
                            result = Mono.error(UpdateEventFunctionValidator.updateFunctionReturnedNull());
                        } else {
                            CloudEvent streamIdentityPreservedCloudEvent = OccurrentCloudEventExtension.preserveStreamIdentity(currentCloudEvent, updatedCloudEvent);
                            CloudEvent appendIdPreservedCloudEvent = OccurrentCloudEventExtension.preserveAppendId(currentCloudEvent, streamIdentityPreservedCloudEvent);
                            CloudEvent positionPreservedCloudEvent = OccurrentCloudEventExtension.preservePosition(currentCloudEvent, appendIdPreservedCloudEvent);
                            CloudEvent preservedUpdatedCloudEvent = DcbCloudEvents.preserveTags(currentCloudEvent, positionPreservedCloudEvent);
                            if (!Objects.equals(preservedUpdatedCloudEvent, currentCloudEvent)) {
                                String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
                                long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
                                Document updatedDocument = OccurrentCloudEventMongoDocumentMapper.convertToDocument(timeRepresentation, streamId, streamVersion, preservedUpdatedCloudEvent);
                                DcbDocumentMapper.preservePositionAndDcbTags(currentCloudEvent, updatedDocument);
                                DcbDocumentMapper.preserveMarkerVersions(document, updatedDocument);
                                updatedDocument.put(ID, document.get(ID)); // Insert the Mongo ObjectID
                                result = mongoTemplate.findAndReplace(cloudEventQuery, updatedDocument, eventStoreCollectionName).thenReturn(preservedUpdatedCloudEvent);
                            } else {
                                result = Mono.just(preservedUpdatedCloudEvent);
                            }
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

    // "afterVersion" is exclusive, matching ExecuteOptions.fromStreamVersion and the "skip N stream positions"
    // reading of EventStore.read's skip parameter, so a skip of 0 keeps every event and a skip of N drops the
    // first N regardless of whether a StreamReadFilter narrows the result further.
    private static Query streamIdAndStreamVersionBetween(String streamId, long afterVersion, long uptoAndIncludingVersion) {
        return Query.query(streamIdEqualToCriteria(streamId).and(STREAM_VERSION).gt(afterVersion).lte(uptoAndIncludingVersion));
    }

    private static Query cloudEventIdIs(String cloudEventId, URI cloudEventSource) {
        return Query.query(where("id").is(cloudEventId).and("source").is(cloudEventSource));
    }

    private static Mono<Long> validateWriteCondition(String streamId, WriteCondition writeCondition, Long currentStreamVersion) {
        final Mono<Long> result;
        if (isFulfilled(currentStreamVersion, writeCondition)) {
            result = Mono.just(currentStreamVersion);
        } else {
            result = Mono.error(new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition));
        }
        return result;
    }

    // Stamps the positions reserved outside the transaction (see write(...)) onto each stream document, so stream and
    // DCB events share one sequence. A no-op when the store does not write position or there is nothing to stamp.
    private List<Document> stampStreamPositions(List<Document> documents, long firstReservedPosition) {
        if (documents.isEmpty() || !writesPosition()) {
            return documents;
        }
        long position = firstReservedPosition;
        for (Document document : documents) {
            PositionDocumentMapper.addPosition(document, position++);
        }
        return documents;
    }

    // Stamps the append id minted for this call (see write(...)) onto every document it persists. A no-op when the
    // call persisted nothing, since an empty append reports no append id (ADR 132, decision 4).
    private static List<Document> stampAppendId(List<Document> documents, Optional<AppendId> appendId) {
        if (documents.isEmpty() || appendId.isEmpty()) {
            return documents;
        }
        String appendIdValue = appendId.get().toString();
        for (Document document : documents) {
            document.put(OccurrentCloudEventExtension.APPEND_ID, appendIdValue);
        }
        return documents;
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
