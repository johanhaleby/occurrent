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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaReadRefusedException;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.mongodb.deployment.internal.MongoDeployment;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
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
 * the document only when its stored {@code version} still equals the expected one, in a single {@code replaceOne}.
 * {@link #compareAndSaveWithoutState} is an {@code updateOne} that sets or removes the fields of
 * {@link SagaCodec#fieldsExceptState()} and does not touch the state, so an instance whose state no longer decodes can
 * still be quarantined and still has that state afterwards. {@link #findWithoutState} is a projection that excludes the
 * state.
 * <p>
 * Every write waits until a majority of the replica set has it in its on-disk journal ({@code w: majority} and
 * {@code j: true}), whatever write concern the {@code MongoTemplate} has, because the saga acknowledges its input as soon
 * as the write returns. A replica set that cannot reach a majority, such as a primary, a secondary and an arbiter with
 * the secondary down, makes each write, and the commit of each transaction the store runs, fail after 10 seconds. On a
 * client with a {@code timeoutMS} above 0 that {@code timeoutMS} bounds them instead. On a client with a
 * {@code timeoutMS} of 0 a single write still fails after 10 seconds, and a commit waits without limit. A server without
 * journaling refuses {@code j: true}, and the store throws an {@link IllegalStateException}.
 * <p>
 * Every other read the saga acts on ({@link #find}, {@link #findWithoutState}, {@link #findWithDueTimers},
 * {@link #findByStatus}, {@link #findReleasingSagaIds} and {@link #findSkippedInputs}) goes to the primary at read
 * concern {@code majority}, so it returns only writes a majority of the replica set has. A write that reached the primary
 * but not a majority reads as absent until it reaches a majority or a failover rolls it back, so the saga never
 * acknowledges an input on the strength of it. Right after a failover, a majority read on the new primary can miss a
 * write the old primary acknowledged, so {@link #find} and {@link #findWithoutState} read an instance they don't find
 * again at read concern {@code linearizable}. MongoDB documents that a linearizable read on the primary returns every
 * write a majority acknowledged before the read started, in
 * <a href="https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/">read concern "linearizable"</a>.
 * A standalone server has no failover and refuses that read concern, so there the store skips the second read.
 * The transactions below read at read concern {@code snapshot}, everything the primary has when the transaction
 * starts, and their commit at {@code majority} returns only once a majority has all of it. A release ends the
 * quarantine of an instance only in such a transaction, one that finds no skipped input left or completes the instance.
 * <p>
 * A release reads with {@link #findWithFirstSkippedInput}, which reads the instance and its first skipped input in one
 * transaction of the store's at read concern {@code snapshot}, so both are what the primary had at the same moment, on
 * a sharded cluster too when the two are on different shards. An instance that read doesn't find is read again at read
 * concern {@code linearizable}, as {@link #find} does. When that read finds it, the store reads both again in a new
 * transaction that starts after the linearizable read, so it has every write that read saw, also on a new primary right
 * after an election. A standalone server refuses transactions, and so every write of a skipped input there, so the
 * store reads the instance and then its first skipped input at read concern {@code majority}.
 * <p>
 * {@link #findWithFirstSkippedInput} throws a {@link SagaReadRefusedException} inside a MongoDB transaction of the
 * caller's, and inside a Spring transaction of another resource that synchronizes its resources, before it reads
 * anything, so a release can't run in either. In the second a template with {@code SessionSynchronization.ALWAYS} starts
 * a MongoDB transaction at the release's first write, and the store can't tell how the template synchronizes. A
 * transaction at read concern {@code local} or
 * {@code majority} on a sharded cluster reads each shard as it is at its own moment, and could return an instance with a
 * record that a later version of it already deleted. The store can't tell the read concern of a transaction it joins,
 * so it refuses one at read concern {@code snapshot} too.
 * <p>
 * The three writes of {@link SagaStateStoreSkippedInputs} and the {@link #delete(String)} of a quarantined instance write
 * the instance and its skipped inputs in one MongoDB transaction, through a {@link MongoTransactionManager} on the
 * template's {@link MongoDatabaseFactory}. So quarantine needs a replica set or a sharded cluster, and the store throws an
 * {@link IllegalStateException} when the server refuses a transaction. A transaction that fails with the
 * {@code TransientTransactionError} label is run again from the start with the store's {@link RetryStrategy}, and a
 * commit that fails with the {@code UnknownTransactionCommitResult} label is committed again. Any other failure of such a
 * transaction is thrown at once, and so is a server's refusal of a transaction or of journaling on any call, whatever
 * the strategy retries, a {@code RetryStrategy} you implement yourself included.
 * <p>
 * The constructor asks the server for its deployment once, with {@code hello}. On a standalone server
 * {@link #requireAtomicTransitions()} throws, so a saga with quarantine on refuses to start. With quarantine off a saga
 * runs there, as long as the server journals its writes, since every write asks for {@code j: true}. The delivery of
 * an input that an instance quarantined earlier, such as one in data restored from a replica set, has to record as
 * skipped fails there, so the input is delivered again until the instance is moved back to a replica set
 * or resolved by hand. {@code SagaSubscription.release(sagaId)} throws there before it reads or writes anything, since
 * the write that starts a release asks for no transaction and the server would refuse the write that applies a replayed
 * input only after the release dispatched its commands. The timer poller replays nothing there, so an instance marked releasing stays
 * as it is. A {@link #delete(String)} of such an instance writes in a transaction, which the server refuses with the
 * replica set message.
 * <p>
 * Called inside a MongoDB transaction of the caller's on the same {@link MongoDatabaseFactory}, those writes join it.
 * Inside such a transaction the store sends each read and write once, and a failure reaches the caller as MongoDB
 * reported it, since MongoDB has aborted the transaction on the server and the same operation sent again in it would
 * fail too. Only the caller can run the transaction again. That is a Spring transaction that synchronizes its resources, as a {@link MongoTransactionManager}
 * does by default, a Spring transaction of another resource in which a template with
 * {@code SessionSynchronization.ALWAYS} has started a MongoDB transaction, or a transaction on the session of a template
 * from {@link MongoTemplate#withSession}. The commit of that transaction then decides whether they survive a failover,
 * with the write concern it commits with. When a write fails inside it, abort the transaction, since a commit after the
 * failure may keep what the store wrote before it. A {@code MongoTransactionManager} with
 * {@code SYNCHRONIZATION_NEVER} binds its transaction with synchronization off, so a template hands out a database bound
 * to no session. In such a transaction each call of the store takes part in it with the store's own transaction, which
 * turns synchronization on for the call, so the call reads and writes in that transaction too. The single-document
 * writes and the scans go through the template, so a template with {@code SessionSynchronization.NEVER} sends them
 * outside a {@code MongoTransactionManager} transaction, and the store doesn't retry them there either, whatever a
 * {@code RetryStrategy} you implement yourself retries.
 * <p>
 * Inside a transaction of the caller's, {@link #find} and {@link #findWithoutState} read at the transaction's snapshot,
 * which misses an instance written after the snapshot began. When the read finds no instance, the store inserts a
 * document that holds only the saga's id and deletes it again in the same transaction, and the server decides whether
 * that insert conflicts. An instance another writer wrote after the snapshot began makes the insert fail with a
 * {@code WriteConflict} that has the {@code TransientTransactionError} label, and a caller that runs its transaction
 * again on that label reads the instance. An instance the transaction deleted itself reads as absent. The two writes
 * leave no document behind when the transaction commits, and a change stream on the collection may see them. The
 * collection must accept a document that holds only {@code _id}, so a validator that requires other fields makes such a
 * read of an absent instance fail. In a Spring transaction of another resource where no MongoDB transaction has started,
 * the two methods read as they do outside a transaction, while {@link #findWithFirstSkippedInput} refuses to read.
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
    // How long each attempt of a write waits for a journaled majority before it fails. A client timeoutMS above 0 bounds
    // the attempt instead
    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(10);
    // With j: true a majority has the write in its on-disk journal before it acknowledges, whatever
    // writeConcernMajorityJournalDefault says
    private static final WriteConcern DURABLE_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true)
            .withWTimeout(WRITE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    // The refusal of j: true by a WiredTiger server that runs without journaling (BadValue). Other refusals share the
    // code, so the message is matched too
    private static final int JOURNAL_NOT_SUPPORTED_ERROR_CODE = 2;
    private static final String JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE = "cannot use 'j' option when a host does not have journaling enabled";
    static final String JOURNALING_REQUIRED_MESSAGE = "SpringMongoSagaStateStore needs a MongoDB server with journaling, since it writes every saga instance and every input a quarantined instance skips with j: true. "
                                                      + "The server refused j: true because it runs without journaling, and a crash could then lose a write the saga has acknowledged its input on.";
    // The refusal of a transaction by a standalone server (IllegalOperation), matched as the driver itself matches it in
    // CommandOperationHelper.transformWriteException
    private static final int TRANSACTIONS_NOT_SUPPORTED_ERROR_CODE = 20;
    private static final String TRANSACTIONS_NOT_SUPPORTED_ERROR_MESSAGE = "Transaction numbers";
    private static final String QUARANTINE_NEEDS_TRANSACTIONS = "SpringMongoSagaStateStore needs a MongoDB replica set or sharded cluster for quarantine, since it writes a quarantined saga instance and the inputs it skips in one transaction. ";
    private static final String WAYS_TO_RUN_THE_SAGA = "Run MongoDB as a replica set, which can have a single member, or as a sharded cluster, or turn quarantine off for the saga with occurrent.saga.quarantine-enabled=false on Spring Boot "
                                                       + "or SagaRunnerConfig.disableQuarantine() on a SagaRunner.";
    static final String REPLICA_SET_REQUIRED_MESSAGE = QUARANTINE_NEEDS_TRANSACTIONS + "The server refused the transaction. " + WAYS_TO_RUN_THE_SAGA;
    static final String STANDALONE_SERVER_MESSAGE = QUARANTINE_NEEDS_TRANSACTIONS + "The server is a standalone server, which refuses transactions. " + WAYS_TO_RUN_THE_SAGA;
    // A transaction at read concern local or majority on a sharded cluster reads each shard at its own moment, so the
    // instance and its records could come from two. Only snapshot reads every shard at one moment, and the store can't
    // see the read concern of a transaction it joins
    static final String CALLERS_TRANSACTION_MESSAGE = "SpringMongoSagaStateStore releases a quarantined saga instance outside a transaction of the caller's, and was called inside one, a MongoDB transaction or a Spring transaction of another resource, in which a template with SessionSynchronization.ALWAYS starts a MongoDB transaction at the first write. "
                                                      + "A release reads the instance and its first skipped input in a transaction of its own at read concern snapshot, since a transaction at another read concern can read the two from different moments on a sharded cluster. "
                                                      + "Call SagaSubscription.release outside the transaction.";
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
    private static final String SKIPPED_EVENT = "event";
    private static final String SKIPPED_DATA = "data";
    private static final String SKIPPED_DATA_DOCUMENT = "dataDocument";
    private static final String SKIPPED_EXTENSIONS = "extensions";
    private static final String EXTENSION_TYPE = "type";
    private static final String EXTENSION_VALUE = "value";
    private static final String EXTENSION_URI = "uri";
    private static final String EXTENSION_TIMESTAMP = "timestamp";
    private static final String EXTENSION_BYTE = "byte";
    private static final String EXTENSION_SHORT = "short";
    private static final String EXTENSION_FLOAT = "float";
    private static final String EXTENSION_BIG_INTEGER = "bigInteger";
    private static final String EXTENSION_BIG_DECIMAL = "bigDecimal";

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
    private final MongoDatabaseFactory databaseFactory;
    private final TransactionTemplate transactions;
    private final String collectionName;
    private final String skippedInputsCollectionName;
    private final Class<S> stateType;
    private final boolean flowSaga;
    private final RetryStrategy retryStrategy;
    private final MongoDeployment deployment;
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
     * {@value #DEFAULT_MAX_ATTEMPTS} attempts. A call inside a MongoDB transaction isn't retried, as the class javadoc
     * says.
     *
     * @param mongoOperations the {@link MongoOperations} used to read and write instance documents
     * @param collectionName  the collection the instances are stored in, which must not end in {@code _skipped_inputs}
     * @param stateType       the user state type, needed to read the stored state back into an object
     * @throws IllegalArgumentException when {@code mongoOperations} is not a {@link MongoTemplate}, since the store writes a
     *                                  quarantined instance and its skipped inputs in a transaction on the template's
     *                                  {@link MongoDatabaseFactory}, or when {@code collectionName} ends in
     *                                  {@code _skipped_inputs}. Pass the {@code MongoTemplate} bean.
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
     * @throws IllegalArgumentException when {@code mongoOperations} is not a {@link MongoTemplate}, since the store writes a
     *                                  quarantined instance and its skipped inputs in a transaction on the template's
     *                                  {@link MongoDatabaseFactory}, or when {@code collectionName} ends in
     *                                  {@code _skipped_inputs}. Pass the {@code MongoTemplate} bean.
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType, @Nullable CloudEventConverter<?> cloudEventConverter) {
        this(mongoOperations, collectionName, stateType, cloudEventConverter, defaultRetryStrategy());
    }

    /**
     * Creates a store that retries a failing call to MongoDB with {@code retryStrategy} instead of the default.
     * Pass {@link RetryStrategy#none()} to have every failure reach the caller on the first attempt. A call inside a
     * MongoDB transaction isn't retried, as the class javadoc says.
     *
     * @param mongoOperations     the {@link MongoOperations} used to read and write instance documents
     * @param collectionName      the collection the instances are stored in, which must not end in {@code _skipped_inputs}
     * @param stateType           the user state type, needed to read the stored state back into an object
     * @param cloudEventConverter the converter of a flow saga's application, or {@code null} for a core saga
     * @param retryStrategy       the strategy used when a read or a write fails
     * @throws IllegalArgumentException when {@code mongoOperations} is not a {@link MongoTemplate}, since the store writes a
     *                                  quarantined instance and its skipped inputs in a transaction on the template's
     *                                  {@link MongoDatabaseFactory}, or when {@code collectionName} ends in
     *                                  {@code _skipped_inputs}. Pass the {@code MongoTemplate} bean.
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType, @Nullable CloudEventConverter<?> cloudEventConverter, RetryStrategy retryStrategy) {
        this.retryStrategy = Objects.requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        if (!(mongoOperations instanceof MongoTemplate mongoTemplate)) {
            throw new IllegalArgumentException("SpringMongoSagaStateStore needs a MongoTemplate, and was given a " + mongoOperations.getClass().getName() + ". "
                                               + "It writes a quarantined saga instance and the inputs it skips in one transaction, on the MongoDatabaseFactory of the template. Pass the MongoTemplate bean.");
        }
        this.databaseFactory = mongoTemplate.getMongoDatabaseFactory();
        // A transaction reads a snapshot of what a majority has, which holds while it commits at majority
        // A client timeoutMS makes the driver drop the wtimeout, so there the commit waits as long as timeoutMS allows,
        // and with a timeoutMS of 0 without limit. MongoTransactionManager passes no transaction timeout to the driver,
        // and the driver sends no maxCommitTime under a timeoutMS of 0
        TransactionOptions transactionOptions = TransactionOptions.builder().writeConcern(DURABLE_WRITE_CONCERN).readConcern(ReadConcern.SNAPSHOT).readPreference(ReadPreference.primary()).build();
        this.transactions = new TransactionTemplate(new CommitRetryingTransactionManager(databaseFactory, transactionOptions, retryStrategy));
        // Joins a transaction the caller started on the same MongoDatabaseFactory
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
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
        // The deployment is asked once here, like the indexes, and a failure of the probe other than a server that does
        // not know hello propagates once the attempts are used. A Runnable, like the indexes, so a caller's strategy
        // tells setup apart from the store's reads and writes
        AtomicReference<MongoDeployment> detected = new AtomicReference<>();
        retryStrategy.execute(() -> detected.set(MongoDeployment.detect(mongoOperations::executeCommand)));
        this.deployment = Objects.requireNonNull(detected.get(), "deployment");
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
    public void requireAtomicTransitions() {
        if (deployment == MongoDeployment.OTHER) {
            throw new IllegalStateException(STANDALONE_SERVER_MESSAGE);
        }
    }

    @Override
    public SagaStateCodec<S> stateCodec() {
        return stateCodec;
    }

    @Override
    public Optional<StoredSaga> find(String sagaId) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(sagaId, "sagaId cannot be null");
            // Decoding happens outside the retried supplier, and in the caller, so a stored state that no longer reads back
            // throws once instead of being attempted ten times.
            return Optional.ofNullable(findInstance(sagaId, () -> Query.query(where(ID).is(sagaId)))).map(this::toStoredSaga);
        });
    }

    // An instance found at read concern majority is acted on through a compare-and-set on its version, which a stale
    // version loses. An absence has no version to compare, and an input addressed to no instance is skipped without a
    // write. Right after a failover the new primary's majority view can be older than a write the old primary
    // acknowledged, so an absence is read again at read concern linearizable, on the primary, by _id and with a
    // maxTimeMS. MongoDB documents that such a read returns every write a majority acknowledged before the read
    // started, see
    // https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/
    // A standalone server has no failover and refuses read concern linearizable, so there the majority read stands
    private @Nullable Document findInstance(String sagaId, Supplier<Query> query) {
        if (joinsCallersTransaction()) {
            return findInCallersTransaction(sagaId, query.get());
        }
        Document found = retried(() -> readInstance(null, query.get(), ReadConcern.MAJORITY));
        return found != null || deployment == MongoDeployment.OTHER ? found : retried(() -> readInstance(null, query.get(), ReadConcern.LINEARIZABLE));
    }

    // From the primary, on the database MongoDatabaseUtils hands out, which is bound to no session here. A template with
    // SessionSynchronization.ALWAYS would start a transaction for the read in a Spring transaction of another resource,
    // and the read would see that transaction's snapshot. A linearizable read waits at most the write timeout, as MongoDB
    // advises, since it can't complete without a majority. In session, when one is given
    private @Nullable Document readInstance(@Nullable ClientSession session, Query query, ReadConcern readConcern) {
        try {
            MongoCollection<Document> instances = instances(MongoDatabaseUtils.getDatabase(databaseFactory)).withReadConcern(readConcern).withReadPreference(ReadPreference.primary());
            FindIterable<Document> found = (session == null ? instances.find(query.getQueryObject()) : instances.find(session, query.getQueryObject())).projection(query.getFieldsObject());
            return (readConcern == ReadConcern.LINEARIZABLE ? found.maxTime(WRITE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) : found).first();
        } catch (RuntimeException e) {
            throw translated(e);
        }
    }

    // Inside a transaction the caller started, a read sees the transaction's snapshot, and no read in the transaction
    // sees a write made after the snapshot began. So the server decides an absence instead. The store inserts a document
    // with the saga's _id and deletes it again, both in the caller's transaction, which together leave nothing behind
    // when it commits. An instance another writer wrote after the snapshot began, committed or not, makes the insert
    // fail with a WriteConflict that has the TransientTransactionError label, and the caller's next attempt reads it
    // with a new snapshot. An absence the transaction made itself, by deleting the instance, lets the insert through.
    // The read and both writes take the database from MongoDatabaseUtils, as inTransaction does, so they share one
    // session whatever session synchronization the template has. Nothing is retried, since only the caller can run its
    // transaction again
    private @Nullable Document findInCallersTransaction(String sagaId, Query query) {
        MongoCollection<Document> instances = instances(MongoDatabaseUtils.getDatabase(databaseFactory));
        try {
            Document found = instances.find(query.getQueryObject()).projection(query.getFieldsObject()).first();
            if (found == null) {
                instances.insertOne(new Document(ID, sagaId));
                instances.deleteOne(Filters.eq(ID, sagaId));
            }
            return found;
        } catch (RuntimeException e) {
            throw translated(e);
        }
    }

    // True exactly when MongoDatabaseUtils hands out a database bound to a session with an active transaction. That holds
    // for a factory bound to such a session, as MongoTemplate.withSession gives, and for a Spring transaction that
    // synchronizes its resources, a MongoDB one or another that a template with SessionSynchronization.ALWAYS has
    // already started a MongoDB transaction in
    private boolean joinsCallersTransaction() {
        return databaseFactory.isTransactionActive()
               || TransactionSynchronizationManager.isSynchronizationActive() && MongoDatabaseUtils.isTransactionActive(databaseFactory);
    }

    // Outside a MongoDB transaction an operation that fails is sent again, with the store's RetryStrategy. Inside one,
    // MongoDB aborts the transaction on the server when an operation in it fails, so the same operation sent again in it
    // fails too, and the failure goes to the caller as it was thrown. The predicate reads inMongoTransaction()
    // again after the failure, since a template with SessionSynchronization.ALWAYS starts the transaction on its first
    // operation
    private <T extends @Nullable Object> T retried(Supplier<T> operation) {
        if (inMongoTransaction()) {
            return operation.get();
        }
        // A server that refuses the durability the store needs refuses it on every attempt, so it fails at once. So does
        // an operation that failed inside a MongoDB transaction, which the server has aborted
        return retryingOnly(retryStrategy, failure -> !(failure instanceof RefusedByServer) && !inMongoTransaction(), operation);
    }

    // A transaction is bound to the factory, the caller's, the store's own or one a template with
    // SessionSynchronization.ALWAYS started, or the factory is bound to a session with an active transaction, as
    // MongoTemplate.withSession gives. A bound transaction counts after the server aborted it, since its owner still has
    // to end it
    private boolean inMongoTransaction() {
        return databaseFactory.isTransactionActive() || TransactionSynchronizationManager.hasResource(databaseFactory);
    }

    // A MongoDB transaction bound to the factory while synchronization is off, as a MongoTransactionManager with
    // SYNCHRONIZATION_NEVER runs it, hands out a database bound to no session, so an operation would read and write
    // outside that transaction. The store's own transaction then takes part in the caller's and turns synchronization
    // on for the operation, so every database it takes, the template's too, is bound to the caller's session, and a
    // failure marks the caller's transaction rollback-only
    private <T extends @Nullable Object> T joiningCallersTransaction(Supplier<T> operation) {
        if (databaseFactory.isTransactionActive() || TransactionSynchronizationManager.isSynchronizationActive()
            || !MongoDatabaseUtils.isTransactionActive(databaseFactory)) {
            return operation.get();
        }
        return transactions.execute(status -> operation.get());
    }

    @Override
    public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(saga, "saga cannot be null");
            Document document = toDocument(saga);
            if (expectedVersion == 0) {
                // Caught inside the retried supplier, because another writer getting there first is an outcome and
                // retrying it would lose the same race nine more times. A failure arriving after MongoDB already stored
                // the document hits this catch on the next attempt and reports the save as lost. The caller reads again,
                // and find reads an absence again at read concern linearizable. MongoDB documents that such a read may wait
                // for writes in progress to reach a majority before it returns, and the maxTimeMS bounds that wait. With
                // the document read back, the caller skips an input its watermarks already cover. A failover that rolls the
                // document back lets the next insert through. An input that has no redelivery key has no watermark, so it
                // re-dispatches, which is the multiplicity a lost compare-and-set already has.
                Supplier<Boolean> insert = () -> {
                    try {
                        writeDurably(collectionName, collection -> collection.insertOne(document));
                        return true;
                    } catch (DuplicateKeyException e) {
                        return false;
                    }
                };
                return retried(insert);
            }
            // A lost compare-and-set matches nothing rather than throwing, so the retry never sees it
            return retried(() -> writeDurably(collectionName, collection -> collection.replaceOne(versionIs(saga.sagaId(), expectedVersion), document)).getMatchedCount() > 0);
        });
    }

    @Override
    public Optional<StoredSaga> findWithoutState(String sagaId) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(sagaId, "sagaId cannot be null");
            return Optional.ofNullable(findInstance(sagaId, () -> {
                Query query = Query.query(where(ID).is(sagaId));
                project(query, SagaCodec.fieldsWithoutState());
                return query;
            })).map(this::toStoredSaga);
        });
    }

    @Override
    public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(saga, "saga cannot be null");
            if (expectedVersion == 0) {
                throw new IllegalArgumentException("compareAndSaveWithoutState cannot insert saga " + saga.sagaId() + ", since it holds no state. Insert it with compareAndSave.");
            }
            Bson update = withoutState(saga);
            return retried(() -> writeDurably(collectionName, collection -> collection.updateOne(versionIs(saga.sagaId(), expectedVersion), update)).getMatchedCount() > 0);
        });
    }

    private static Bson withoutState(StoredSaga saga) {
        Document document = toDocument(saga);
        List<Bson> updates = new ArrayList<>();
        for (String field : SagaCodec.fieldsExceptState()) {
            // Unset rather than left alone, so a field the new instance has no value for goes away instead of keeping
            // whatever the previous write left there. An instance failing on an event from a store that assigns no
            // global position replaces a record that had one, and that record's failurePosition has to go with it.
            updates.add(document.containsKey(field) ? Updates.set(field, document.get(field)) : Updates.unset(field));
        }
        return Updates.combine(updates);
    }

    private static Bson versionIs(String sagaId, long expectedVersion) {
        return Filters.and(Filters.eq(ID, sagaId), Filters.eq(VERSION, expectedVersion));
    }

    @Override
    public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
        return joiningCallersTransaction(() -> {
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
            Supplier<List<Document>> read = () -> mongoOperations.find(majorityRead(query), Document.class, collectionName);
            return retried(read).stream().map(this::toStoredSaga).toList();
        });
    }

    @Override
    public List<StoredSaga> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        return joiningCallersTransaction(() -> {
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
            // Projected like the due-timer query, so a page doesn't read the state of every instance it lists
            project(query, SagaCodec.instanceFields());
            Supplier<List<Document>> read = () -> mongoOperations.find(majorityRead(query), Document.class, collectionName);
            return retried(read).stream().map(this::toStoredSaga).toList();
        });
    }

    @Override
    public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
        return joiningCallersTransaction(() -> {
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
            Supplier<List<Document>> read = () -> mongoOperations.find(majorityRead(query), Document.class, collectionName);
            return retried(read).stream().map(document -> document.getString(ID)).toList();
        });
    }

    // ACTIVE and QUARANTINED are each stored as either of two strings, see SagaCodec.storedStatuses
    private static Criteria statusIs(SagaStatus status) {
        return where(STATUS).in(SagaCodec.storedStatuses(status));
    }

    @Override
    public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
        return joiningCallersTransaction(() -> {
            if (limit < 1) {
                // A Mongo limit of 0 means no limit
                throw new IllegalArgumentException("limit must be positive, was " + limit);
            }
            Query query = Query.query(where(SKIPPED_SAGA_ID).is(Objects.requireNonNull(sagaId, "sagaId cannot be null"))
                            .and(SKIPPED_QUARANTINE_ID).is(Objects.requireNonNull(quarantineId, "quarantineId cannot be null")))
                    .with(Sort.by(Sort.Direction.ASC, SKIPPED_SEQ, SKIPPED_KEY))
                    .limit(limit);
            Supplier<List<Document>> read = () -> mongoOperations.find(majorityRead(query), Document.class, skippedInputsCollectionName);
            return retried(read).stream().map(SpringMongoSagaStateStore::toSkippedInput).toList();
        });
    }

    // On a replica set or a mongos both reads run in one transaction of the store's, at read concern snapshot, so they
    // see the instance and its records as one write left them, on one shard or on two. Two reads at read concern majority
    // can each go to another primary across a failover. A standalone server refuses the transactions every write of a
    // record takes, so its records don't change and two plain reads do. A transaction of the caller's is refused, see
    // CALLERS_TRANSACTION_MESSAGE
    @Override
    public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        // A Spring transaction of another resource is refused too, since a template with SessionSynchronization.ALWAYS
        // starts a MongoDB transaction in it at the release's first write, and the template doesn't say how it
        // synchronizes. This is the first call of a release, so the refusal comes before it reads or writes anything
        if (MongoDatabaseUtils.isTransactionActive(databaseFactory) || TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new SagaReadRefusedException(CALLERS_TRANSACTION_MESSAGE);
        }
        Query byId = Query.query(where(ID).is(sagaId));
        if (deployment == MongoDeployment.OTHER) {
            Document found = retried(() -> readInstance(null, byId, ReadConcern.MAJORITY));
            return withFirstSkippedInput(found, document -> retried(() -> firstSkippedInput(
                    MongoDatabaseUtils.getDatabase(databaseFactory).withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary()), sagaId, document)));
        }
        Optional<StoredSagaWithFirstSkippedInput> found = inSnapshot(sagaId, byId, null);
        if (found.isPresent()) {
            return found;
        }
        // An absence in the snapshot is read again at read concern linearizable, as find does. An instance found then is
        // read in a new snapshot that starts after that read, since a new primary right after an election can still hand
        // out a snapshot from before it. The session isn't causally consistent, since the driver would send the operation
        // time of a failed attempt as afterClusterTime on the next, which linearizable refuses. It still keeps the
        // operation time of every reply, and the new snapshot starts after the latest
        try (ClientSession linearizable = databaseFactory.getSession(ClientSessionOptions.builder().causallyConsistent(false).build())) {
            if (retried(() -> readInstance(linearizable, byId, ReadConcern.LINEARIZABLE)) == null) {
                return Optional.empty();
            }
            return inSnapshot(sagaId, byId, linearizable);
        }
    }

    // With after, the snapshot starts after every read made in that session
    private Optional<StoredSagaWithFirstSkippedInput> inSnapshot(String sagaId, Query byId, @Nullable ClientSession after) {
        // Decoded after the transaction, so a stored instance that no longer reads back throws once instead of being retried
        Document[] read = retried(() -> {
            try {
                return Objects.requireNonNull(transactions.execute(status -> {
                    if (after != null) {
                        CommitRetryingTransactionManager.startAfter(status, after);
                    }
                    MongoDatabase database = MongoDatabaseUtils.getDatabase(databaseFactory);
                    Document found = instances(database).find(byId.getQueryObject()).first();
                    return new Document[]{found, found == null ? null : firstSkippedInput(database, sagaId, found)};
                }));
            } catch (RuntimeException e) {
                throw translated(e);
            }
        });
        return withFirstSkippedInput(read[0], document -> read[1]);
    }

    private Optional<StoredSagaWithFirstSkippedInput> withFirstSkippedInput(@Nullable Document instance, Function<Document, @Nullable Document> firstSkippedInput) {
        if (instance == null) {
            return Optional.empty();
        }
        Document first = firstSkippedInput.apply(instance);
        return Optional.of(new StoredSagaWithFirstSkippedInput(toStoredSaga(instance), first == null ? null : toSkippedInput(first)));
    }

    private @Nullable Document firstSkippedInput(MongoDatabase database, String sagaId, Document instance) {
        String quarantineId = instance.getString(FAILURE_QUARANTINE_ID);
        if (quarantineId == null) {
            return null;
        }
        try {
            return skippedInputs(database).find(Filters.and(Filters.eq(SKIPPED_SAGA_ID, sagaId), Filters.eq(SKIPPED_QUARANTINE_ID, quarantineId)))
                    .sort(Sorts.ascending(SKIPPED_SEQ, SKIPPED_KEY)).limit(1).first();
        } catch (RuntimeException e) {
            throw translated(e);
        }
    }

    // The downgrade in the upgrade guide and the runbook writes this down as the way to rebuild an event from a dump of the
    // records, and SkippedInputExportTest checks that the two give the same event, so a change here is made there too
    private static SkippedInput toSkippedInput(Document document) {
        CloudEventBuilder event = CloudEventBuilder.from(CLOUD_EVENT_JSON_FORMAT.deserialize(document.getString(SKIPPED_EVENT).getBytes(StandardCharsets.UTF_8)));
        Document extensions = document.get(SKIPPED_EXTENSIONS, Document.class);
        if (extensions != null) {
            extensions.forEach((name, value) -> withHeldExtension(event, name, value));
        }
        Object data = document.get(SKIPPED_DATA);
        byte[] bytes = data == null ? null : data instanceof Binary binary ? binary.getData() : (byte[]) data;
        Document dataDocument = document.get(SKIPPED_DATA_DOCUMENT, Document.class);
        if (dataDocument != null) {
            byte[] delivered = bytes == null ? dataDocument.toJson().getBytes(StandardCharsets.UTF_8) : bytes;
            event.withData(PojoCloudEventData.wrap(dataDocument, value -> delivered));
        } else if (bytes != null) {
            event.withData(bytes);
        }
        return new SkippedInput(((Number) document.get(SKIPPED_SEQ)).longValue(), document.getString(SKIPPED_KEY), event.build());
    }

    // Adds the event to the record the way a converter read it on delivery. A map is held as a document, with its bytes
    // beside it unless they are the document's own JSON, and other data as its bytes in BSON binary, which keeps binary
    // data at its own size. Fields are added as they are built, so the record has the value that failed when building
    // it throws
    private static void addHeldEvent(Document held, CloudEvent event) {
        held.append(SKIPPED_EVENT, new String(CLOUD_EVENT_JSON_FORMAT.serialize(attributesOf(event)), StandardCharsets.UTF_8));
        Document extensions = new Document();
        for (String name : event.getExtensionNames()) {
            extensions.append(name, heldExtension(Objects.requireNonNull(event.getExtension(name))));
        }
        if (!extensions.isEmpty()) {
            held.append(SKIPPED_EXTENSIONS, extensions);
        }
        CloudEventData data = event.getData();
        if (data == null) {
            return;
        }
        if (data instanceof PojoCloudEventData<?> pojo && pojo.getValue() instanceof Map<?, ?> map) {
            // A key that isn't a String fails when the document is encoded
            @SuppressWarnings("unchecked")
            Document document = new Document((Map<String, Object>) map);
            held.append(SKIPPED_DATA_DOCUMENT, document);
            byte[] bytes = data.toBytes();
            if (!Arrays.equals(bytes, document.toJson().getBytes(StandardCharsets.UTF_8))) {
                held.append(SKIPPED_DATA, bytes);
            }
            return;
        }
        held.append(SKIPPED_DATA, data.toBytes());
    }

    // The attributes alone, since CloudEventBuilder.from copies the extensions too and throws for a class it doesn't know
    private static CloudEvent attributesOf(CloudEvent event) {
        CloudEventBuilder attributes = CloudEventBuilder.fromSpecVersion(event.getSpecVersion()).withId(event.getId()).withSource(event.getSource()).withType(event.getType());
        if (event.getDataContentType() != null) {
            attributes.withDataContentType(event.getDataContentType());
        }
        if (event.getDataSchema() != null) {
            attributes.withDataSchema(event.getDataSchema());
        }
        if (event.getSubject() != null) {
            attributes.withSubject(event.getSubject());
        }
        if (event.getTime() != null) {
            attributes.withTime(event.getTime());
        }
        return attributes.build();
    }

    // BSON reads a URI, an OffsetDateTime, a Byte, a Short, a Float, a BigInteger and a BigDecimal back as another class,
    // so they are stored as a string tagged with the class to restore. Any other value is stored as it is
    private static Object heldExtension(Object value) {
        return switch (value) {
            case URI uri -> taggedExtension(EXTENSION_URI, uri.toString());
            case OffsetDateTime time -> taggedExtension(EXTENSION_TIMESTAMP, time.toString());
            case Byte number -> taggedExtension(EXTENSION_BYTE, number.toString());
            case Short number -> taggedExtension(EXTENSION_SHORT, number.toString());
            // Float.valueOf reads Float.toString back as the same Float, NaN and both infinities included
            case Float number -> taggedExtension(EXTENSION_FLOAT, number.toString());
            case BigInteger number -> taggedExtension(EXTENSION_BIG_INTEGER, number.toString());
            case BigDecimal number -> taggedExtension(EXTENSION_BIG_DECIMAL, number.toString());
            default -> value;
        };
    }

    private static Document taggedExtension(String type, String value) {
        return new Document(EXTENSION_TYPE, type).append(EXTENSION_VALUE, value);
    }

    private static void withHeldExtension(CloudEventBuilder event, String name, Object value) {
        switch (value) {
            case String string -> event.withExtension(name, string);
            case Integer number -> event.withExtension(name, number);
            case Long number -> event.withExtension(name, number);
            case Double number -> event.withExtension(name, number);
            case Boolean bool -> event.withExtension(name, bool);
            case Binary binary -> event.withExtension(name, binary.getData());
            case byte[] bytes -> event.withExtension(name, bytes);
            case Document tagged -> {
                String tag = tagged.getString(EXTENSION_VALUE);
                switch (tagged.getString(EXTENSION_TYPE)) {
                    case EXTENSION_URI -> event.withExtension(name, URI.create(tag));
                    case EXTENSION_TIMESTAMP -> event.withExtension(name, OffsetDateTime.parse(tag));
                    case EXTENSION_BYTE -> event.withExtension(name, Byte.valueOf(tag));
                    case EXTENSION_SHORT -> event.withExtension(name, Short.valueOf(tag));
                    case EXTENSION_FLOAT -> event.withExtension(name, Float.valueOf(tag));
                    case EXTENSION_BIG_INTEGER -> event.withExtension(name, new BigInteger(tag));
                    case EXTENSION_BIG_DECIMAL -> event.withExtension(name, new BigDecimal(tag));
                    default -> throw new IllegalStateException("Extension '" + name + "' of a skipped input has the unknown type '" + tagged.getString(EXTENSION_TYPE) + "'");
                }
            }
            default -> throw new IllegalStateException("Extension '" + name + "' of a skipped input has a value of the unknown type " + value.getClass().getName());
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * The record holds the event's attributes as a CloudEvents JSON string, its extensions, and its data. Data delivered
     * as a map, which is how the MongoDB event store delivers a JSON object, is held as a document, with its bytes beside
     * it unless they are the document's own JSON. Other data is held as its bytes in BSON binary, and a release hands it
     * to the converter as a {@link BytesCloudEventData}.
     * <p>
     * Before it writes anything, this encodes the record to BSON the way MongoDB stores it, decodes it, and reads the
     * event back the way a release reads it. It throws an {@link IllegalStateException} that names the instance, the
     * input and, when it's known, where in the event the difference is, when:
     * <ul>
     *     <li>an attribute, an extension, or a value at any depth in the data comes back as another value or as a value
     *     of another class. A {@code BigDecimal} in a map comes back as a {@code Decimal128}, and a {@code String} with
     *     an unpaired surrogate comes back as another {@code String}. A map or a list may come back as another
     *     implementation of {@code Map} or {@code List}.</li>
     *     <li>the data comes back as another class or as other bytes. A {@code JsonCloudEventData} comes back as a
     *     {@link BytesCloudEventData}.</li>
     *     <li>encoding, decoding or reading the record back throws, such as for a map at any depth with a key that isn't
     *     a {@code String}.</li>
     *     <li>the record goes over the 16 MiB MongoDB stores in one, which the saga id, the quarantine id, the key and
     *     the attributes share with the data. A map from another feed than the MongoDB event store is held both as a
     *     document and as its bytes, so it fits only up to about half that.</li>
     * </ul>
     * A subscription that retries the event stops on it.
     */
    @Override
    public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(saga, "saga cannot be null");
            Objects.requireNonNull(input, "input cannot be null");
            // An expected version of 0 inserts the instance whole, which is how an instance whose first input failed is
            // quarantined. Written as an upsert that only sets on insert, so an instance that exists already is a lost
            // compare-and-set and not a duplicate key error, which would abort the transaction
            Function<MongoDatabase, Boolean> writeInstance = expectedVersion == 0
                    ? database -> instances(database).updateOne(Filters.eq(ID, saga.sagaId()), setOnInsert(toDocument(saga)), new UpdateOptions().upsert(true)).getUpsertedId() != null
                    : database -> instances(database).updateOne(versionIs(saga.sagaId(), expectedVersion), withoutState(saga)).getMatchedCount() > 0;
            Document filter = skippedInputFilter(saga.sagaId(), quarantineId, input.key());
            // Set on insert, so a record of the key keeps its first seq and event
            Document record = new Document("$setOnInsert", checkedRecord(saga.sagaId(), filter, input, skippedInputCodecs()));
            return transition(writeInstance, database -> skippedInputs(database).updateOne(filter, record, new UpdateOptions().upsert(true)));
        });
    }

    @Override
    public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(saga, "saga cannot be null");
            Objects.requireNonNull(replayed, "replayed cannot be null");
            Document document = toDocument(saga);
            Document filter = skippedInputFilter(saga.sagaId(), quarantineId, replayed.key()).append(SKIPPED_SEQ, replayed.seq());
            // A record already gone was applied by another release, so applying it again here would apply it twice. A
            // release that deletes it meanwhile writes the instance too, so the replaceOne then conflicts and the
            // transaction runs again. A replayed input that completes the instance ends its quarantine, so every record
            // under it is deleted
            boolean completes = SagaStatus.COMPLETED.name().equals(saga.index().storedStatus());
            return transition(database -> skippedInputs(database).find(filter).first() != null
                                          && instances(database).replaceOne(versionIs(saga.sagaId(), expectedVersion), document).getMatchedCount() > 0,
                    database -> completes
                            ? skippedInputs(database).deleteMany(quarantineFilter(saga.sagaId(), quarantineId))
                            : skippedInputs(database).deleteOne(filter));
        });
    }

    @Override
    public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
        return joiningCallersTransaction(() -> {
            Objects.requireNonNull(saga, "saga cannot be null");
            Document document = toDocument(saga);
            Document filter = quarantineFilter(saga.sagaId(), quarantineId);
            // The quarantine ends only with no record left, checked in the transaction against everything the primary
            // has, whatever the release read before. A record written after that check changes the instance too, so the
            // replaceOne then conflicts and the transaction runs again
            return transition(database -> skippedInputs(database).find(filter).first() == null
                                          && instances(database).replaceOne(versionIs(saga.sagaId(), expectedVersion), document).getMatchedCount() > 0,
                    database -> skippedInputs(database).deleteMany(filter));
        });
    }

    /**
     * Whether a release can write {@code candidate}. MongoDB stores a document of at most {@link #MAX_DOCUMENT_SIZE}
     * bytes of BSON, and the driver sends a write only when its statement, the compare-and-set filter together with the
     * replacement or the update, takes at most {@link #MAX_STATEMENT_SIZE} bytes. A release replaces the instance whole
     * or updates every field but the state, so this checks the document and both statements. The filter holds the saga
     * id, so an instance of exactly 16 MiB fits only when its id takes less than about 16 KiB. The check encodes the
     * writes the way the driver does and reads nothing from the database.
     */
    @Override
    public boolean fits(StoredSaga candidate) {
        Objects.requireNonNull(candidate, "candidate cannot be null");
        CodecRegistry codecs = instances(databaseFactory.getMongoDatabase()).getCodecRegistry();
        RawBsonDocument document = new RawBsonDocument(toDocument(candidate), codecs.get(Document.class));
        // The write compares the stored version, a long like the candidate's, so the filter encodes to the same size
        BsonDocument filter = versionIs(candidate.sagaId(), candidate.version()).toBsonDocument(BsonDocument.class, codecs);
        BsonDocument update = withoutState(candidate).toBsonDocument(BsonDocument.class, codecs);
        return document.getByteBuffer().remaining() <= MAX_DOCUMENT_SIZE
               && encodedSize(statement(filter, document), codecs) <= MAX_STATEMENT_SIZE
               && encodedSize(statement(filter, update), codecs) <= MAX_STATEMENT_SIZE;
    }

    // Looked up before the transaction, so a failure to get the database is translated the way a failure inside it is
    private CodecRegistry skippedInputCodecs() {
        try {
            return skippedInputs(databaseFactory.getMongoDatabase()).getCodecRegistry();
        } catch (RuntimeException e) {
            throw translated(e);
        }
    }

    // The fields the upsert sets on insert, refused before the transaction unless a release would restore the event as it
    // was delivered. The record is encoded the way the upsert stores it, decoded, and read back by toSkippedInput, the
    // method a release reads it with. Checked here, since the driver throws exceptions that name neither the instance nor
    // the input, and a retry would send the same record again
    private static Document checkedRecord(String sagaId, Document filter, SkippedInput input, CodecRegistry codecs) {
        Codec<Document> codec = codecs.get(Document.class);
        Document held = new Document(SKIPPED_SEQ, input.seq());
        int storedSize;
        int statementSize;
        String difference;
        try {
            addHeldEvent(held, input.event());
            // An upsert stores the fields of the filter, the fields it sets on insert and an ObjectId as the _id
            Document stored = new Document(ID, new ObjectId());
            stored.putAll(filter);
            stored.putAll(held);
            RawBsonDocument encoded = new RawBsonDocument(stored, codec);
            storedSize = encoded.getByteBuffer().remaining();
            statementSize = encodedSize(statement(filter.toBsonDocument(BsonDocument.class, codecs),
                    new Document("$setOnInsert", held).toBsonDocument(BsonDocument.class, codecs)), codecs);
            difference = difference(input.event(), toSkippedInput(encoded.decode(codec)).event());
        } catch (RuntimeException e) {
            throw cannotRecord(sagaId, input.key(), "Its record can't be written to BSON and read back" + failingValue(held, codecs) + ", since " + e
                    + ".");
        }
        if (storedSize > MAX_DOCUMENT_SIZE || statementSize > MAX_STATEMENT_SIZE) {
            throw cannotRecord(sagaId, input.key(), "Its record takes " + storedSize + " bytes of BSON, where MongoDB stores at most " + MAX_DOCUMENT_SIZE
                    + ", and the write that sends it takes " + statementSize + " bytes, where the driver sends at most " + MAX_STATEMENT_SIZE
                    + ". The record holds the event's data together with its attributes, the saga id, the quarantine id and the key, so data "
                    + "that close to MongoDB's limit leaves no room for them.");
        }
        if (difference != null) {
            throw cannotRecord(sagaId, input.key(), difference + ", so a release would hand the converter another event than the one it was delivered.");
        }
        return held;
    }

    // Where in the record BSON fails, as the first extension or data value that fails to encode on its own and its class,
    // checking the values inside a map or a list before the map or the list. Empty when no value fails on its own
    private static String failingValue(Document held, CodecRegistry codecs) {
        if (held.get(SKIPPED_EXTENSIONS) instanceof Document extensions) {
            for (Map.Entry<String, Object> extension : extensions.entrySet()) {
                Change failing = failingPath("", extension.getValue(), codecs);
                if (failing != null) {
                    return " at its extension '" + extension.getKey() + "'" + failing.how();
                }
            }
        }
        Change failing = failingPath("", held.get(SKIPPED_DATA_DOCUMENT), codecs);
        return failing == null ? "" : (failing.path().isEmpty() ? " at its data" : " at '" + failing.path() + "' of its data") + failing.how();
    }

    private static @Nullable Change failingPath(String path, @Nullable Object value, CodecRegistry codecs) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Change failing = failingPath(path.isEmpty() ? String.valueOf(entry.getKey()) : path + "." + entry.getKey(), entry.getValue(), codecs);
                if (failing != null) {
                    return failing;
                }
            }
        } else if (value instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                Change failing = failingPath(path + "[" + i + "]", list.get(i), codecs);
                if (failing != null) {
                    return failing;
                }
            }
        }
        try {
            new RawBsonDocument(new Document("value", value), codecs.get(Document.class));
            return null;
        } catch (RuntimeException e) {
            return new Change(path, ", " + described(value));
        }
    }

    // How the restored event differs from the delivered one in an attribute, an extension or its data, at any depth, or
    // null when every value is equal and of the same class. A map or a list may come back as another implementation of
    // Map or List
    private static @Nullable String difference(CloudEvent delivered, CloudEvent restored) {
        if (delivered.getSpecVersion() != restored.getSpecVersion()) {
            return "Its spec version " + delivered.getSpecVersion() + " comes back as " + restored.getSpecVersion();
        }
        Set<String> attributes = new LinkedHashSet<>(delivered.getAttributeNames());
        attributes.addAll(restored.getAttributeNames());
        for (String attribute : attributes) {
            Change change = change("", delivered.getAttribute(attribute), restored.getAttribute(attribute));
            if (change != null) {
                return "Its attribute '" + attribute + "'" + change.how();
            }
        }
        Set<String> extensions = new LinkedHashSet<>(delivered.getExtensionNames());
        extensions.addAll(restored.getExtensionNames());
        for (String extension : extensions) {
            Change change = change("", delivered.getExtension(extension), restored.getExtension(extension));
            if (change != null) {
                return "Its extension '" + extension + "'" + change.how();
            }
        }
        CloudEventData deliveredData = delivered.getData();
        CloudEventData restoredData = restored.getData();
        if (deliveredData == null || restoredData == null || deliveredData.getClass() != restoredData.getClass()) {
            Change change = change("", deliveredData, restoredData);
            return change == null ? null : "Its data" + change.how();
        }
        if (deliveredData instanceof PojoCloudEventData<?> deliveredPojo && restoredData instanceof PojoCloudEventData<?> restoredPojo) {
            Change change = change("", deliveredPojo.getValue(), restoredPojo.getValue());
            if (change != null) {
                return (change.path().isEmpty() ? "Its data" : "The value at '" + change.path() + "' of its data") + change.how();
            }
        }
        return Arrays.equals(deliveredData.toBytes(), restoredData.toBytes()) ? null : "Its data comes back as other bytes";
    }

    // A value at its path inside a map or a list, and how it changed or which class failed
    private record Change(String path, String how) {
    }

    private static @Nullable Change change(String path, @Nullable Object delivered, @Nullable Object restored) {
        if (delivered instanceof Map<?, ?> deliveredMap && restored instanceof Map<?, ?> restoredMap) {
            if (!deliveredMap.keySet().equals(restoredMap.keySet())) {
                return new Change(path, " comes back with the keys " + restoredMap.keySet() + " where it was delivered with " + deliveredMap.keySet());
            }
            for (Map.Entry<?, ?> entry : deliveredMap.entrySet()) {
                Change change = change(path.isEmpty() ? String.valueOf(entry.getKey()) : path + "." + entry.getKey(), entry.getValue(), restoredMap.get(entry.getKey()));
                if (change != null) {
                    return change;
                }
            }
            return null;
        }
        if (delivered instanceof List<?> deliveredList && restored instanceof List<?> restoredList) {
            if (deliveredList.size() != restoredList.size()) {
                return new Change(path, " comes back with " + restoredList.size() + " elements where it was delivered with " + deliveredList.size());
            }
            for (int i = 0; i < deliveredList.size(); i++) {
                Change change = change(path + "[" + i + "]", deliveredList.get(i), restoredList.get(i));
                if (change != null) {
                    return change;
                }
            }
            return null;
        }
        if (delivered == null || restored == null || delivered.getClass() != restored.getClass()) {
            return delivered == restored ? null : new Change(path, ", " + described(delivered) + ", comes back as " + described(restored));
        }
        return Objects.deepEquals(delivered, restored) ? null : new Change(path, ", " + described(delivered) + ", comes back as another value");
    }

    private static String described(@Nullable Object value) {
        return value == null ? "null" : "a " + value.getClass().getName();
    }

    private static IllegalStateException cannotRecord(String sagaId, String key, String reason) {
        return new IllegalStateException("Saga instance '" + sagaId + "' cannot record the input '" + key + "' it skips. " + reason
                + " Nothing is written for the input.");
    }

    // An update or replacement as the driver writes it into the update command, see SplittablePayload
    private static BsonDocument statement(BsonDocument filter, BsonDocument updateOrReplacement) {
        return new BsonDocument("q", filter).append("u", updateOrReplacement);
    }

    private static int encodedSize(BsonDocument document, CodecRegistry codecs) {
        return new RawBsonDocument(document, codecs.get(BsonDocument.class)).getByteBuffer().remaining();
    }

    // The largest document MongoDB stores, its maxBsonObjectSize
    private static final int MAX_DOCUMENT_SIZE = 16 * 1024 * 1024;
    // The largest statement the driver sends in an update command, maxBsonObjectSize plus its 16 KiB of headroom
    private static final int MAX_STATEMENT_SIZE = MAX_DOCUMENT_SIZE + 16 * 1024;

    private static Bson setOnInsert(Document document) {
        List<Bson> updates = new ArrayList<>();
        document.forEach((field, value) -> {
            if (!ID.equals(field)) {
                updates.add(Updates.setOnInsert(field, value));
            }
        });
        return Updates.combine(updates);
    }

    private static Document skippedInputFilter(String sagaId, String quarantineId, String key) {
        return quarantineFilter(sagaId, quarantineId).append(SKIPPED_KEY, Objects.requireNonNull(key, "key cannot be null"));
    }

    private static Document quarantineFilter(String sagaId, String quarantineId) {
        return new Document(SKIPPED_SAGA_ID, Objects.requireNonNull(sagaId, "sagaId cannot be null"))
                .append(SKIPPED_QUARANTINE_ID, Objects.requireNonNull(quarantineId, "quarantineId cannot be null"));
    }

    // The instance first, so a write that loses its compare-and-set has written nothing, and a second transition on the
    // same instance conflicts with the first on that document before either touches a skipped input
    private boolean transition(Function<MongoDatabase, Boolean> compareAndSet, Function<MongoDatabase, ?> writeSkippedInputs) {
        return inTransaction(database -> {
            if (!compareAndSet.apply(database)) {
                return false;
            }
            writeSkippedInputs.apply(database);
            return true;
        });
    }

    // Run again from the start when the failure has the TransientTransactionError label, which says the transaction did
    // not commit, so the attempt left nothing behind. Inside a transaction the caller started, a failure is the caller's
    // to retry, since this store cannot run the caller's earlier writes again. The database comes from
    // MongoDatabaseUtils rather than the template, so it is bound to the transaction's session whatever session
    // synchronization the template has
    private boolean inTransaction(Function<MongoDatabase, Boolean> work) {
        boolean joinsCallersTransaction = joinsCallersTransaction();
        if (databaseFactory.isTransactionActive()) {
            // The store's transaction manager would start a second transaction on a new session of the factory, so the
            // work runs on the database of the session the factory is bound to. The store can't abort that transaction
            // after a failure between the writes, so its owner has to
            try {
                return work.apply(databaseFactory.getMongoDatabase());
            } catch (RuntimeException e) {
                throw translated(e);
            }
        }
        Supplier<Boolean> attempt = () -> {
            try {
                return Objects.requireNonNull(transactions.execute(status -> {
                    boolean done = work.apply(MongoDatabaseUtils.getDatabase(databaseFactory));
                    if (!done && status.isNewTransaction()) {
                        status.setRollbackOnly();
                    }
                    return done;
                }));
            } catch (RuntimeException e) {
                throw translated(e);
            }
        };
        // A transaction is run again only when MongoDB labels its failure transient, as the class javadoc says
        return joinsCallersTransaction ? attempt.get()
                : retryingOnly(retryStrategy, failure -> !(failure instanceof RefusedByServer) && hasCause(failure, SpringMongoSagaStateStore::isTransientTransactionError), attempt);
    }

    private MongoCollection<Document> instances(MongoDatabase database) {
        return database.getCollection(collectionName);
    }

    private MongoCollection<Document> skippedInputs(MongoDatabase database) {
        return database.getCollection(skippedInputsCollectionName);
    }

    // A write outside a transaction at w: majority and j: true, see the class javadoc. Inside a transaction the caller
    // started, the driver leaves the write concern out and the commit's write concern applies. execute(..) translates
    // exceptions
    private <T> T writeDurably(String collectionName, Function<MongoCollection<Document>, T> write) {
        try {
            return Objects.requireNonNull(mongoOperations.execute(collectionName, collection -> write.apply(durable(collection))));
        } catch (RuntimeException e) {
            throw translated(e);
        }
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        MongoCollection<Document> majority = collection.withWriteConcern(DURABLE_WRITE_CONCERN);
        // A client timeoutMS makes the driver drop the wtimeout, and a timeoutMS of 0 waits forever
        Long timeoutMillis = majority.getTimeout(TimeUnit.MILLISECONDS);
        return timeoutMillis != null && timeoutMillis == 0 ? majority.withTimeout(WRITE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) : majority;
    }

    // A server that cannot give the durability the store needs is named, and a driver exception that Spring can translate
    // is translated, as the template does
    private RuntimeException translated(RuntimeException e) {
        if (hasCause(e, SpringMongoSagaStateStore::isJournalNotSupported)) {
            return new RefusedByServer(JOURNALING_REQUIRED_MESSAGE, e);
        }
        if (hasCause(e, SpringMongoSagaStateStore::isTransactionNotSupported)) {
            return new RefusedByServer(REPLICA_SET_REQUIRED_MESSAGE, e);
        }
        if (e instanceof MongoException mongoException) {
            DataAccessException translated = databaseFactory.getExceptionTranslator().translateExceptionIfPossible(mongoException);
            return translated == null ? e : translated;
        }
        return e;
    }

    // Runs operation with retryStrategy and retries only a failure that retryable accepts. Any other failure is returned
    // out of the retried call instead of thrown, and is thrown once execute(..) has returned, so a strategy implemented
    // elsewhere, which runs its own loop and retries what it retries, cannot send the operation again
    private static <T extends @Nullable Object> T retryingOnly(RetryStrategy retryStrategy, Predicate<Throwable> retryable, Supplier<T> operation) {
        Attempt<T> attempt = retryStrategy.execute(() -> {
            try {
                return new Attempt<T>(operation.get(), null);
            } catch (RuntimeException e) {
                if (retryable.test(e)) {
                    throw e;
                }
                return new Attempt<T>(null, e);
            }
        });
        RuntimeException failure = attempt.failure();
        if (failure != null) {
            throw failure;
        }
        return attempt.result();
    }

    // What one run of a retried operation gave, its result or the failure no attempt is to retry
    private record Attempt<T extends @Nullable Object>(@Nullable T result, @Nullable RuntimeException failure) {
    }

    private static boolean isTransientTransactionError(Throwable cause) {
        return cause instanceof MongoException mongoException && mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
    }

    // The server refuses what the store needs, and refuses it again on every attempt
    private static final class RefusedByServer extends IllegalStateException {
        private RefusedByServer(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static boolean hasCause(Throwable throwable, Predicate<Throwable> matches) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (matches.test(cause)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTransactionNotSupported(Throwable cause) {
        return cause instanceof MongoServerException serverException && serverException.getCode() == TRANSACTIONS_NOT_SUPPORTED_ERROR_CODE
               && serverException.getMessage() != null && serverException.getMessage().contains(TRANSACTIONS_NOT_SUPPORTED_ERROR_MESSAGE);
    }

    private static boolean isJournalNotSupported(Throwable cause) {
        return cause instanceof MongoServerException serverException && serverException.getCode() == JOURNAL_NOT_SUPPORTED_ERROR_CODE
               && serverException.getMessage() != null && serverException.getMessage().contains(JOURNAL_NOT_SUPPORTED_ERROR_MESSAGE);
    }

    private static void project(Query query, List<String> fields) {
        query.fields().include(ID);
        fields.forEach(field -> query.fields().include(field));
    }

    @Override
    public void delete(String sagaId) {
        joiningCallersTransaction(() -> {
            Objects.requireNonNull(sagaId, "sagaId cannot be null");
            // An instance without a quarantine id has no skipped inputs, so it goes in a write of its own, which needs no
            // replica set. A retry after a timed out attempt that deleted the document on the primary deletes nothing, and
            // MongoDB still waits for a majority to have everything the primary has before it returns, that deletion included
            Bson withoutQuarantineId = Filters.and(Filters.eq(ID, sagaId), Filters.eq(FAILURE_QUARANTINE_ID, null));
            if (retried(() -> writeDurably(collectionName, collection -> collection.deleteOne(withoutQuarantineId)).getDeletedCount()) > 0) {
                return null;
            }
            // Read at the local read concern, so an instance whose insert is on the primary alone is still deleted
            Query exists = Query.query(where(ID).is(sagaId));
            exists.fields().include(ID);
            if (retried(() -> mongoOperations.findOne(primary(exists), Document.class, collectionName)) == null) {
                return null;
            }
            FindOneAndDeleteOptions quarantineIdOnly = new FindOneAndDeleteOptions().projection(Projections.include(FAILURE_QUARANTINE_ID));
            inTransaction(database -> {
                Document removed = instances(database).findOneAndDelete(Filters.eq(ID, sagaId), quarantineIdOnly);
                String quarantineId = removed == null ? null : removed.getString(FAILURE_QUARANTINE_ID);
                if (quarantineId != null) {
                    skippedInputs(database).deleteMany(quarantineFilter(sagaId, quarantineId));
                }
                return true;
            });
            return null;
        });
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

    // Commits again when the driver cannot tell whether a commit took effect, as the javadoc of
    // MongoTransactionManager.doCommit shows. MongoDB commits a transaction at most once, so a commit that took effect
    // and is sent again changes nothing. Only a transaction this store started commits here
    private static final class CommitRetryingTransactionManager extends MongoTransactionManager {
        private final RetryStrategy retryStrategy;

        private CommitRetryingTransactionManager(MongoDatabaseFactory databaseFactory, TransactionOptions options, RetryStrategy retryStrategy) {
            super(databaseFactory, options);
            this.retryStrategy = retryStrategy;
        }

        private static boolean hasUnknownCommitResult(Throwable cause) {
            return cause instanceof MongoException mongoException && mongoException.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        }

        // A Spring session is causally consistent, so the first command of the transaction asks for a snapshot at or
        // after the operation time of earlier, which holds every write the reads of earlier saw
        private static void startAfter(TransactionStatus status, ClientSession earlier) {
            ClientSession session = Objects.requireNonNull(((MongoTransactionObject) ((DefaultTransactionStatus) status).getTransaction()).getSession(), "the transaction's session");
            session.advanceClusterTime(earlier.getClusterTime());
            session.advanceOperationTime(Objects.requireNonNull(earlier.getOperationTime(), "the server sent no operationTime for the read the snapshot starts after"));
        }

        @Override
        protected void doCommit(MongoTransactionObject transactionObject) {
            retryingOnly(retryStrategy, failure -> hasCause(failure, CommitRetryingTransactionManager::hasUnknownCommitResult), () -> {
                transactionObject.commitTransaction();
                return null;
            });
        }
    }

    // Every read is from the primary, whatever read preference the MongoOperations has. A release turns an instance
    // ACTIVE once its read of the skipped-input records finds none, and a secondary that lags can find none while a
    // record is written, so that input, acknowledged when it was skipped, would never be applied. The instance is read
    // from the primary too, so it and its records come from the same server
    private static Query primary(Query query) {
        return query.withReadPreference(ReadPreference.primary());
    }

    // A read the executor acts on returns only what a majority has. A write whose majority acknowledgement failed is on
    // the primary alone, and a failover can roll it back. Read at the local read concern, the executor would see that
    // write, its own or another node's, and acknowledge an input a failover then loses. Read at majority the write is
    // absent, so the executor writes again and loses its compare-and-set until the write is on a majority or rolled
    // back. Inside a transaction the caller started, the driver sends that transaction's read concern instead
    private static Query majorityRead(Query query) {
        return primary(query).withReadConcern(ReadConcern.MAJORITY);
    }

}
