/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.dsl.saga.mongodb.spring;

import com.mongodb.MongoServerException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * A {@link SagaStateStore} backed by MongoDB. Each saga instance is one document keyed by its {@code _id} (the saga id),
 * with one top-level field for each field of {@link SagaCodec}. A top-level indexed {@code nextTimerFiresAt}, the
 * earliest due time across the pending timers, lets {@link #findWithDueTimers(Instant, int)} both find and order the
 * due instances from the index.
 * <p>
 * State serialization: a core saga's state is written with the application's {@code MongoConverter}, like the snapshot
 * store. A flow saga's state is written by {@link SagaCodec}, field by field, with its received events as CloudEvent JSON.
 * <p>
 * {@link #compareAndSave} is atomic: a new instance is inserted (a duplicate {@code _id} loses), and an update replaces
 * the document only when its stored {@code version} still equals the expected one, via a single {@code findAndReplace}.
 * {@link #compareAndSaveWithoutState} is a {@code findAndModify} that sets or removes the fields of
 * {@link SagaCodec#fieldsExceptState()} and does not touch the state, so an instance whose state no longer decodes can
 * still be quarantined and still has that state afterwards. {@link #findWithoutState} is a projection that excludes the
 * state.
 * <p>
 * {@link #findReleasingSagaIds(String, int)} finds the instances that have {@code failureReleasedAt} set. The inputs a
 * quarantined instance skips are stored one document each, in a collection next to the instances named after it with
 * {@code _skipped_inputs} appended. Each holds {@code sagaId}, {@code quarantineId}, {@code seq} and {@code key}, with a
 * unique index on {@code sagaId}, {@code quarantineId} and {@code key}, and an index on {@code sagaId},
 * {@code quarantineId} and {@code seq} for the order a release reads them in. The constructor refuses a collection name
 * that ends in {@code _skipped_inputs}, because a store on {@code orders} keeps its skipped inputs in
 * {@code orders_skipped_inputs}, and a store on that name would share the collection.
 * <p>
 * An instance quarantined with a quarantine id is stored with the status {@value #QUARANTINED_RECORDING} rather than
 * {@code QUARANTINED}, and both read back as {@link SagaStatus#QUARANTINED}. In the same way an active instance that holds
 * stream marks is stored with the status {@value #ACTIVE_MARKED}, and reads back as {@link SagaStatus#ACTIVE}. A document
 * written before 0.35.0 has none of the fields a release adds, and reads back as not releasing, with no quarantine id
 * and no count.
 *
 * @param <S> the user state type
 */
@NullMarked
public final class SpringMongoSagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S>, SagaStateStoreSkippedInputs {

    private static final Logger log = LoggerFactory.getLogger(SpringMongoSagaStateStore.class);

    private static final String SKIPPED_INPUTS_SUFFIX = "_skipped_inputs";
    // How long each attempt of a skipped-input write waits for a journaled majority before it fails. A client timeoutMS
    // above 0 bounds the attempt instead
    private static final Duration SKIPPED_INPUT_WRITE_TIMEOUT = Duration.ofSeconds(10);
    // With j: true a majority has the write in its on-disk journal before it acknowledges, whatever
    // writeConcernMajorityJournalDefault says
    private static final WriteConcern SKIPPED_INPUT_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true)
            .withWTimeout(SKIPPED_INPUT_WRITE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    // The refusal of j: true by a WiredTiger server that runs without journaling (BadValue). Other refusals share the
    // code, so the message is matched too
    private static final int JOURNAL_NOT_SUPPORTED_ERROR_CODE = 2;
    private static final String JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE = "cannot use 'j' option when a host does not have journaling enabled";
    static final String JOURNALING_REQUIRED_MESSAGE = "SpringMongoSagaStateStore needs a MongoDB server with journaling to record the inputs a quarantined saga instance skips. "
                                                      + "The server refused j: true because it runs without journaling, and a crash could then lose a recorded input that a release has to replay.";
    private static final String ID = "_id";
    private static final String STATE = SagaCodec.STATE;
    private static final String STATUS = SagaCodec.STATUS;
    private static final String VERSION = SagaCodec.VERSION;
    private static final String NEXT_TIMER_FIRES_AT = SagaCodec.NEXT_TIMER_FIRES_AT;
    private static final String UPDATED_AT = SagaCodec.UPDATED_AT;
    private static final String FAILURE_RELEASED_AT = SagaCodec.FAILURE_RELEASED_AT;
    private static final String FAILURE_QUARANTINE_ID = SagaCodec.FAILURE_QUARANTINE_ID;

    /** The status of an instance quarantined with a quarantine id, see {@link SagaCodec#QUARANTINED_RECORDING}. */
    public static final String QUARANTINED_RECORDING = SagaCodec.QUARANTINED_RECORDING;

    /** The status of an active instance that holds stream marks, see {@link SagaCodec#ACTIVE_MARKED}. */
    public static final String ACTIVE_MARKED = SagaCodec.ACTIVE_MARKED;

    // Fields of a document in the skipped inputs collection
    private static final String SKIPPED_SAGA_ID = "sagaId";
    private static final String SKIPPED_QUARANTINE_ID = "quarantineId";
    private static final String SKIPPED_SEQ = "seq";
    private static final String SKIPPED_KEY = "key";

    /**
     * How many times a read or a write calls MongoDB before it gives up, 10, the same number
     * {@code MongoAppliedAppendStore} uses. This is a count of attempts and not a length of time. Ten failures that
     * come back at once take about 11 seconds on the backoff below, while ten against a server that is not answering
     * each spend the driver's own server selection timeout, 30 seconds by default. Set a timeout on the client when
     * the wall clock is what matters.
     */
    static final int DEFAULT_MAX_ATTEMPTS = 10;

    private static final EventFormat CLOUD_EVENT_JSON_FORMAT = Objects.requireNonNull(
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE), "CloudEvents JSON format must be on the classpath");
    private final MongoOperations mongoOperations;
    private final String collectionName;
    private final String skippedInputsCollectionName;
    private final Class<S> stateType;
    private final boolean flowSaga;
    private final RetryStrategy retryStrategy;
    private final SagaStateCodec<S> stateCodec = new SagaStateCodec<>() {
        @Override
        public Object encode(S state) {
            // SagaCodec refuses a null here, with the saga id named
            return mongoOperations.getConverter().convertToMongoType(state);
        }

        @SuppressWarnings("unchecked")
        @Override
        public S decode(Object encoded) {
            MongoConverter converter = mongoOperations.getConverter();
            if (encoded instanceof Document stateDocument) {
                return converter.read(stateType, stateDocument);
            }
            return (S) converter.getConversionService().convert(encoded, stateType);
        }
    };

    /**
     * Creates a store for a core saga, whose state serializes with the application's {@code MongoConverter}.
     * A failing call to MongoDB is retried with exponential backoff from 100 ms up to 2 seconds, giving up after
     * {@value #DEFAULT_MAX_ATTEMPTS} attempts.
     *
     * @param mongoOperations the {@link MongoOperations} used to read and write instance documents
     * @param collectionName  the collection the instances are stored in, which must not end in {@code _skipped_inputs}
     * @param stateType       the user state type, needed to read the stored state back into an object
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType) {
        this(mongoOperations, collectionName, stateType, null);
    }

    /**
     * Creates a store for a flow saga when {@code cloudEventConverter} is supplied, and for a core saga when it is
     * {@code null}. The saga runner writes a flow saga's received events with its own converter, so the one passed here
     * only marks the store as a flow saga's.
     *
     * @param mongoOperations     the {@link MongoOperations} used to read and write instance documents
     * @param collectionName      the collection the instances are stored in, which must not end in {@code _skipped_inputs}
     * @param stateType           the user state type, needed to read the stored state back into an object
     * @param cloudEventConverter the converter of a flow saga's application, or {@code null} for a core saga
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType, @Nullable CloudEventConverter<?> cloudEventConverter) {
        this(mongoOperations, collectionName, stateType, cloudEventConverter, defaultRetryStrategy());
    }

    /**
     * Creates a store that retries a failing call to MongoDB with {@code retryStrategy} instead of the default.
     * Pass {@link RetryStrategy#none()} to have every failure reach the caller on the first attempt.
     *
     * @param mongoOperations     the {@link MongoOperations} used to read and write instance documents
     * @param collectionName      the collection the instances are stored in, which must not end in {@code _skipped_inputs}
     * @param stateType           the user state type, needed to read the stored state back into an object
     * @param cloudEventConverter the converter of a flow saga's application, or {@code null} for a core saga
     * @param retryStrategy       the strategy used when a read or a write fails
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType, @Nullable CloudEventConverter<?> cloudEventConverter, RetryStrategy retryStrategy) {
        this.retryStrategy = Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName cannot be null");
        if (collectionName.endsWith(SKIPPED_INPUTS_SUFFIX)) {
            String sharedWith = collectionName.substring(0, collectionName.length() - SKIPPED_INPUTS_SUFFIX.length());
            throw new IllegalArgumentException("collectionName '" + collectionName + "' ends in '" + SKIPPED_INPUTS_SUFFIX + "', and a store on '" + sharedWith
                                               + "' keeps its skipped inputs in that collection, so the two would share it. Pick a name that does not end in '" + SKIPPED_INPUTS_SUFFIX + "'.");
        }
        this.skippedInputsCollectionName = collectionName + SKIPPED_INPUTS_SUFFIX;
        this.stateType = Objects.requireNonNull(stateType, "stateType cannot be null");
        this.flowSaga = cloudEventConverter != null;
        if (stateType == FlowState.class && cloudEventConverter == null) {
            // A hand-built store that omits the converter would have no way to tell the runner it holds a flow saga, so fail
            // loud here instead.
            throw new IllegalArgumentException("a CloudEventConverter is required to store a flow saga's FlowState; use the four-argument constructor and pass the application's CloudEventConverter");
        }
        // Retried like every other call this store makes, so a MongoDB error that a second attempt would clear does not
        // fail the saga's registration while an application is starting. Only the unique index has an option, and no
        // setting changes it, so nothing a user configures can make MongoDB refuse one permanently. An index an operator
        // has already created with different options fails the constructor after the attempt limit rather than being
        // retried forever. The unique index on the skipped inputs is what makes recording a key twice store one document.
        retryStrategy.execute(() -> {
            mongoOperations.getCollection(collectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(NEXT_TIMER_FIRES_AT)));
            mongoOperations.getCollection(collectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(UPDATED_AT)));
            mongoOperations.getCollection(skippedInputsCollectionName).createIndex(
                    Indexes.compoundIndex(Indexes.ascending(SKIPPED_SAGA_ID), Indexes.ascending(SKIPPED_QUARANTINE_ID), Indexes.ascending(SKIPPED_KEY)),
                    new IndexOptions().unique(true));
            mongoOperations.getCollection(skippedInputsCollectionName).createIndex(
                    Indexes.compoundIndex(Indexes.ascending(SKIPPED_SAGA_ID), Indexes.ascending(SKIPPED_QUARANTINE_ID), Indexes.ascending(SKIPPED_SEQ), Indexes.ascending(SKIPPED_KEY)));
        });
    }

    /**
     * Package private and returning {@link RetryStrategy.Retry} so a test can swap the backoff for a fast one and
     * still exercise the attempt limit this store actually ships with, the same reason
     * {@code MongoAppliedAppendStore.defaultRetryStrategy} is.
     */
    static RetryStrategy.Retry defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f).maxAttempts(DEFAULT_MAX_ATTEMPTS);
    }

    @Override
    public SagaStateCodec<S> stateCodec() {
        return stateCodec;
    }

    @Override
    public Optional<StoredSaga> find(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        // Decoding happens outside the retried supplier, and in the caller, so a stored state that no longer reads back
        // throws once instead of being attempted ten times.
        Supplier<@Nullable Document> read = () -> mongoOperations.findOne(primary(Query.query(where(ID).is(sagaId))), Document.class, collectionName);
        return Optional.ofNullable(retryStrategy.execute(read)).map(this::toStoredSaga);
    }

    @Override
    public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
        Objects.requireNonNull(saga, "saga cannot be null");
        Document document = toDocument(saga);
        if (expectedVersion == 0) {
            // Caught inside the retried supplier, because another writer getting there first is an outcome and
            // retrying it would lose the same race nine more times. A failure arriving after MongoDB already stored
            // the document hits this catch on the next attempt and reports the save as lost, which the caller answers
            // by re-reading and skipping an input its watermarks already cover. An input that has no redelivery key
            // has no watermark, so it re-dispatches, which is the multiplicity a lost compare-and-set already has.
            Supplier<Boolean> insert = () -> {
                try {
                    mongoOperations.insert(document, collectionName);
                    return true;
                } catch (DuplicateKeyException e) {
                    return false;
                }
            };
            return retryStrategy.execute(insert);
        }
        Query query = Query.query(where(ID).is(saga.sagaId()).and(VERSION).is(expectedVersion));
        // A lost compare-and-set is a null document rather than an exception, so the retry never sees it and the
        // answer is read outside the supplier.
        Supplier<@Nullable Document> replace = () -> mongoOperations.findAndReplace(query, document, collectionName);
        return retryStrategy.execute(replace) != null;
    }

    @Override
    public Optional<StoredSaga> findWithoutState(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        Query query = Query.query(where(ID).is(sagaId));
        project(query, SagaCodec.fieldsWithoutState());
        Supplier<@Nullable Document> read = () -> mongoOperations.findOne(primary(query), Document.class, collectionName);
        return Optional.ofNullable(retryStrategy.execute(read)).map(this::toStoredSaga);
    }

    @Override
    public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
        Objects.requireNonNull(saga, "saga cannot be null");
        if (expectedVersion == 0) {
            throw new IllegalArgumentException("compareAndSaveWithoutState cannot insert saga " + saga.sagaId() + ", since it holds no state. Insert it with compareAndSave.");
        }
        Document document = toDocument(saga);
        Update update = new Update();
        for (String field : SagaCodec.fieldsExceptState()) {
            if (document.containsKey(field)) {
                update.set(field, document.get(field));
            } else {
                // Unset rather than left alone, so a field the new instance has no value for goes away instead of keeping
                // whatever the previous write left there. An instance failing on an event from a store that assigns no
                // global position replaces a record that had one, and that record's failurePosition has to go with it.
                update.unset(field);
            }
        }
        Query query = Query.query(where(ID).is(saga.sagaId()).and(VERSION).is(expectedVersion));
        Supplier<@Nullable Document> modify = () -> mongoOperations.findAndModify(query, update, Document.class, collectionName);
        return retryStrategy.execute(modify) != null;
    }

    @Override
    public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
        Objects.requireNonNull(now, "now cannot be null");
        // Sorted on the field it filters on, so the (status, nextTimerFiresAt) index serves the order as well as the match.
        // ACTIVE is two strings, and Mongo merges the two index ranges in nextTimerFiresAt order.
        Query query = Query.query(statusIs(SagaStatus.ACTIVE).and(NEXT_TIMER_FIRES_AT).lte(now.toEpochMilli()))
                .with(Sort.by(Sort.Direction.ASC, NEXT_TIMER_FIRES_AT))
                .limit(limit);
        // Project only the fields every enumeration needs. This deliberately excludes the state (a flow saga's received
        // log can be large), so the poll never pays to read it. The executor re-loads the full document with find(sagaId)
        // before it processes a timer, which is the authoritative read the fire acts on.
        project(query, SagaCodec.instanceFields());
        Supplier<List<Document>> read = () -> mongoOperations.find(primary(query), Document.class, collectionName);
        return retryStrategy.execute(read).stream().map(this::toStoredSaga).toList();
    }

    @Override
    public List<StoredSaga> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        Objects.requireNonNull(status, "status cannot be null");
        Objects.requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (limit < 1) {
            // A Mongo limit of 0 means "no limit", so a caller passing 0 would get the whole collection instead of
            // nothing. Reject it here rather than let that through as a surprise full scan.
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Query query = Query.query(statusIs(status).and(UPDATED_AT).lt(updatedBefore.toEpochMilli()))
                .with(Sort.by(Sort.Direction.ASC, UPDATED_AT))
                .limit(limit);
        // Projected exactly like the due-timer query, so enumerating an instance whose state no longer decodes still works.
        project(query, SagaCodec.instanceFields());
        Supplier<List<Document>> read = () -> mongoOperations.find(primary(query), Document.class, collectionName);
        return retryStrategy.execute(read).stream().map(this::toStoredSaga).toList();
    }

    @Override
    public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
        if (limit < 1) {
            // A Mongo limit of 0 means no limit
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Criteria releasing = statusIs(SagaStatus.QUARANTINED).and(FAILURE_RELEASED_AT).exists(true);
        if (after != null) {
            releasing = releasing.and(ID).gt(after);
        }
        Query query = Query.query(releasing)
                .with(Sort.by(Sort.Direction.ASC, ID))
                .limit(limit);
        query.fields().include(ID);
        Supplier<List<Document>> read = () -> mongoOperations.find(primary(query), Document.class, collectionName);
        return retryStrategy.execute(read).stream().map(document -> document.getString(ID)).toList();
    }

    // ACTIVE and QUARANTINED are each stored as either of two strings, see SagaCodec.storedStatuses
    private static Criteria statusIs(SagaStatus status) {
        return where(STATUS).in(SagaCodec.storedStatuses(status));
    }

    @Override
    public void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        Objects.requireNonNull(input, "input cannot be null");
        Document filter = skippedInputFilter(sagaId, quarantineId, input.key());
        // Two writers that upsert the same key race to insert it, and the server retries the loser's duplicate-key error
        // itself, since the filter is an equality on exactly the fields of the unique index. The loser then finds the
        // record the winner stored and keeps it, the outcome it wanted. This store opens no session or transaction of its
        // own. Inside a transaction a caller opened, the server doesn't retry it, and the error ends that transaction, so
        // catching it here couldn't save the write either.
        writeSkippedInputs(collection -> collection.updateOne(filter, Updates.setOnInsert(SKIPPED_SEQ, input.seq()), new UpdateOptions().upsert(true)));
    }

    @Override
    public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
        if (limit < 1) {
            // A Mongo limit of 0 means no limit
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Query query = Query.query(where(SKIPPED_SAGA_ID).is(Objects.requireNonNull(sagaId, "sagaId cannot be null"))
                        .and(SKIPPED_QUARANTINE_ID).is(Objects.requireNonNull(quarantineId, "quarantineId cannot be null")))
                .with(Sort.by(Sort.Direction.ASC, SKIPPED_SEQ, SKIPPED_KEY))
                .limit(limit);
        Supplier<List<Document>> read = () -> mongoOperations.find(primary(query), Document.class, skippedInputsCollectionName);
        return retryStrategy.execute(read).stream().map(SpringMongoSagaStateStore::toSkippedInput).toList();
    }

    private static SkippedInput toSkippedInput(Document document) {
        return new SkippedInput(((Number) document.get(SKIPPED_SEQ)).longValue(), document.getString(SKIPPED_KEY));
    }

    @Override
    public void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head) {
        Objects.requireNonNull(head, "head cannot be null");
        Document filter = skippedInputFilter(sagaId, quarantineId, head.key());
        // A race to insert the same record is retried by the server, as recordSkippedInput says
        writeSkippedInputs(collection -> collection.updateOne(filter, Updates.set(SKIPPED_SEQ, head.seq()), new UpdateOptions().upsert(true)));
    }

    @Override
    public void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
        Objects.requireNonNull(input, "input cannot be null");
        Document filter = skippedInputFilter(sagaId, quarantineId, input.key()).append(SKIPPED_SEQ, input.seq());
        writeSkippedInputs(collection -> collection.deleteOne(filter));
    }

    @Override
    public void deleteSkippedInputs(String sagaId, String quarantineId) {
        Document filter = new Document(SKIPPED_SAGA_ID, Objects.requireNonNull(sagaId, "sagaId cannot be null"))
                .append(SKIPPED_QUARANTINE_ID, Objects.requireNonNull(quarantineId, "quarantineId cannot be null"));
        writeSkippedInputs(collection -> collection.deleteMany(filter));
    }

    private static Document skippedInputFilter(String sagaId, String quarantineId, String key) {
        return new Document(SKIPPED_SAGA_ID, Objects.requireNonNull(sagaId, "sagaId cannot be null"))
                .append(SKIPPED_QUARANTINE_ID, Objects.requireNonNull(quarantineId, "quarantineId cannot be null"))
                .append(SKIPPED_KEY, Objects.requireNonNull(key, "key cannot be null"));
    }

    // Every write and delete of a skipped-input record waits for a journaled majority, whatever write concern the
    // caller's MongoOperations or collection has. A skip is acknowledged once its record is written, so a record a
    // failover rolled back would leave a skipped input that no release replays. execute(..) still translates exceptions
    private void writeSkippedInputs(java.util.function.Function<MongoCollection<Document>, ?> write) {
        retryStrategy.execute(() -> {
            try {
                return mongoOperations.execute(skippedInputsCollectionName, collection -> write.apply(skippedInputWrites(collection)));
            } catch (RuntimeException e) {
                if (isJournalNotSupported(e)) {
                    throw new IllegalStateException(JOURNALING_REQUIRED_MESSAGE, e);
                }
                throw e;
            }
        });
    }

    private static MongoCollection<Document> skippedInputWrites(MongoCollection<Document> collection) {
        MongoCollection<Document> majority = collection.withWriteConcern(SKIPPED_INPUT_WRITE_CONCERN);
        // A client timeoutMS makes the driver drop the wtimeout, and a timeoutMS of 0 waits forever
        Long timeoutMillis = majority.getTimeout(TimeUnit.MILLISECONDS);
        return timeoutMillis != null && timeoutMillis == 0 ? majority.withTimeout(SKIPPED_INPUT_WRITE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) : majority;
    }

    private static boolean isJournalNotSupported(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof MongoServerException serverException && serverException.getCode() == JOURNAL_NOT_SUPPORTED_ERROR_CODE
                && serverException.getMessage() != null && serverException.getMessage().contains(JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE)) {
                return true;
            }
        }
        return false;
    }

    private static void project(Query query, List<String> fields) {
        query.fields().include(ID);
        fields.forEach(field -> query.fields().include(field));
    }

    @Override
    public void delete(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        Query query = Query.query(where(ID).is(sagaId));
        query.fields().include(FAILURE_QUARANTINE_ID);
        Document removed = retryStrategy.execute(() -> mongoOperations.findAndRemove(query, Document.class, collectionName));
        // After the instance, so a crash in between leaves records no instance reads rather than an instance whose
        // records are gone. Only the records of the instance removed here, so a later instance with the same id, created
        // and quarantined in between, keeps the ones it wrote under a quarantine id of its own. A retried remove that
        // finds the instance gone already returns nothing, and its records stay, read by nothing
        String quarantineId = removed == null ? null : removed.getString(FAILURE_QUARANTINE_ID);
        if (quarantineId != null) {
            deleteSkippedInputs(sagaId, quarantineId);
        }
    }

    private static Document toDocument(StoredSaga saga) {
        Document document = new Document(ID, saga.sagaId());
        saga.encoded().fields().forEach((field, value) -> document.append(field, STATE.equals(field) ? stateToBson(value) : toBson(value)));
        return document;
    }

    private StoredSaga toStoredSaga(Document document) {
        Map<String, Object> fields = new LinkedHashMap<>();
        document.forEach((field, value) -> {
            if (!ID.equals(field)) {
                fields.put(field, STATE.equals(field) ? stateFromBson(value) : fromBson(value));
            }
        });
        EncodedSaga encoded = new EncodedSaga(fields);
        return new StoredSaga(document.getString(ID), ((Number) document.get(VERSION)).longValue(), encoded, SagaCodec.indexOf(encoded));
    }

    // What a state codec returned is kept as it is, apart from a plain map, which a flow saga's state is
    private static @Nullable Object stateToBson(@Nullable Object state) {
        return state instanceof Map<?, ?> && !(state instanceof Document) ? toBson(state) : state;
    }

    // A flow saga's received events are stored as CloudEvent JSON strings and read back as CloudEvent values. Only a flow
    // saga's state has them, so the state of a core saga is never touched.
    private @Nullable Object stateFromBson(@Nullable Object state) {
        if (!flowSaga || !(state instanceof Map<?, ?> map) || !(map.get(SagaCodec.FLOW_RECEIVED) instanceof List<?> received)) {
            return state;
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        map.forEach((key, entry) -> fields.put((String) key, entry));
        List<Object> events = new ArrayList<>();
        received.forEach(json -> events.add(CLOUD_EVENT_JSON_FORMAT.deserialize(((String) json).getBytes(StandardCharsets.UTF_8))));
        fields.put(SagaCodec.FLOW_RECEIVED, events);
        return fields;
    }

    // The tree has an Instant and a CloudEvent where this store has always written a Date and a JSON string, and no
    // other value MongoDB lacks a type for
    private static @Nullable Object toBson(@Nullable Object value) {
        if (value instanceof Instant instant) {
            return Date.from(instant);
        }
        if (value instanceof CloudEvent cloudEvent) {
            return new String(CLOUD_EVENT_JSON_FORMAT.serialize(cloudEvent), StandardCharsets.UTF_8);
        }
        if (value instanceof Map<?, ?> map) {
            Document document = new Document();
            map.forEach((key, entry) -> document.append((String) key, toBson(entry)));
            return document;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(SpringMongoSagaStateStore::toBson).toList();
        }
        return value;
    }

    private static @Nullable Object fromBson(@Nullable Object value) {
        if (value instanceof Date date) {
            return date.toInstant();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> fields = new LinkedHashMap<>();
            map.forEach((key, entry) -> fields.put((String) key, fromBson(entry)));
            return fields;
        }
        if (value instanceof List<?> list) {
            List<Object> values = new ArrayList<>();
            list.forEach(entry -> values.add(fromBson(entry)));
            return values;
        }
        return value;
    }

    // Every read is from the primary, whatever read preference the MongoOperations has. A release turns an instance
    // ACTIVE once its read of the skipped-input records finds none, and a secondary that lags can find none while a
    // record is written, so that input, acknowledged when it was skipped, would never be applied. The instance is read
    // from the primary too, so it and its records come from the same server
    private static Query primary(Query query) {
        return query.withReadPreference(ReadPreference.primary());
    }
}
