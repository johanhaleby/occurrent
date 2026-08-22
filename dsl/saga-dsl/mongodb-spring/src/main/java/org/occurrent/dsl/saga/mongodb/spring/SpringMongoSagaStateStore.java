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

import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * A {@link SagaStateStore} backed by MongoDB. Each saga instance is one document keyed by its {@code _id} (the saga id),
 * carrying the user {@code state}, the lifecycle {@code status}, the optimistic-lock {@code version}, the pending timers,
 * and the dedup watermarks. A top-level indexed {@code nextTimerFiresAt} (the earliest pending timer) makes
 * {@link #findWithDueTimers(Instant, int)} an indexed query.
 * <p>
 * State serialization: a core saga's state is written with the application's {@code MongoConverter}, like the
 * snapshot store. A flow saga's state ({@code FlowState}) is written field by field, with its received domain events
 * serialized as CloudEvents through the supplied {@link CloudEventConverter} so they round-trip by their stable
 * {@code CloudEventTypeMapper} type rather than a Java class name. A domain event can therefore move to a different
 * package without breaking in-flight flow-saga state. Pass the converter (via the four-argument constructor) for a flow
 * saga, leave it out for a core saga.
 * <p>
 * {@link #compareAndSave} is atomic: a new instance is inserted (a duplicate {@code _id} loses), and an update replaces
 * the document only when its stored {@code version} still equals the expected one, via a single {@code findAndReplace}.
 *
 * @param <S> the user state type
 */
@NullMarked
public final class SpringMongoSagaStateStore<S extends @Nullable Object> implements SagaStateStore<S>, SagaStateStoreQueries<S> {

    private static final Logger log = LoggerFactory.getLogger(SpringMongoSagaStateStore.class);

    private static final String ID = "_id";
    private static final String STATE = "state";
    private static final String STATUS = "status";
    private static final String VERSION = "version";
    private static final String TIMERS = "timers";
    private static final String TIMER_NAME = "name";
    private static final String TIMER_FIRES_AT = "firesAtEpochMilli";
    private static final String NEXT_TIMER_FIRES_AT = "nextTimerFiresAt";
    private static final String CURRENT_STEP = "currentStep";
    private static final String STREAM_WATERMARKS = "streamWatermarks";
    private static final String POSITION_WATERMARK = "positionWatermark";
    private static final String CREATED_AT = "createdAt";
    private static final String UPDATED_AT = "updatedAt";
    private static final String COMPLETED_AT = "completedAt";
    private static final String STARTED = "started";
    // The failure record is flattened into top-level fields rather than a sub-document, for the same reason currentStep
    // is one: both enumeration queries project the SagaInstance members and neither may decode the state, and a
    // quarantined instance whose state no longer decodes is exactly the instance somebody is looking for.
    private static final String FAILURE_INPUT = "failureInput";
    private static final String FAILURE_POSITION = "failurePosition";
    private static final String FAILURE_FIRST_FAILED_AT = "failureFirstFailedAt";
    private static final String FAILURE_TYPE = "failureType";
    private static final String FAILURE_MESSAGE = "failureMessage";
    private static final String FAILURE_RELEASED_AT = "failureReleasedAt";

    // Field names inside a persisted FlowState document.
    private static final String FLOW_CURRENT_STEP = "currentStep";
    private static final String FLOW_WINDOW_START = "windowStart";
    private static final String FLOW_STEP_ENTRY_INDEX = "stepEntryIndex";
    private static final String FLOW_COMPLETED = "completed";
    private static final String FLOW_PREVIOUS_STEP = "previousStep";
    private static final String FLOW_PREVIOUS_STEP_ENTRY_INDEX = "previousStepEntryIndex";
    private static final String FLOW_LAST_ACTION = "lastAction";
    private static final String FLOW_MATCHED_BRANCH_INDEX = "matchedBranchIndex";
    private static final String FLOW_RECEIVED = "received";
    private static final String FLOW_STEP_CONDITION_PROGRESS = "stepConditionProgress";
    private static final String FLOW_LEAF_FINGERPRINT = "leafFingerprint";
    private static final String FLOW_MATCH_COUNTS = "matchCounts";

    private static final EventFormat CLOUD_EVENT_JSON_FORMAT = Objects.requireNonNull(
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE), "CloudEvents JSON format must be on the classpath");

    // A flow saga's retained events sit inside the same document as the rest of its state, so an instance that keeps
    // accumulating them (an unbounded stepWindow, a step that never transitions, or a stepWindow-capped step reached
    // through a narrowingFilter/replacementFilter or CloudEventTypeMapper wider than the flow's own declared types,
    // whose events stepWindow does not bound, see ADR 129) heads toward MongoDB's 16 MB document limit with no signal
    // before the write itself starts failing. This is an early warning, not an enforced cap. The unbounded default
    // stays, ADR 123 already gives the remedy for the common case (a stepWindow cap), and this only makes the growth
    // visible, including the growth stepWindow cannot reach.
    // A round number one to two orders of magnitude below the document limit for typical CloudEvent sizes (a few hundred
    // bytes to a few KB each): high enough that a normal, short-lived flow never sees it, low enough to warn well before a
    // runaway instance is anywhere near failing to save. Not made configurable: it is a diagnostic tripwire, not a
    // behavioural limit, and the actual remedy (stepWindow) is already configurable per saga.
    // Package-private rather than private: a test asserting the edge-triggering behaviour builds exactly this many events
    // rather than hardcoding a duplicate of the number here.
    static final int RETAINED_EVENT_WARNING_THRESHOLD = 1_000;

    // Bounds the latch's total SIZE. An entry is removed when its instance drops below the threshold, or explicitly on
    // delete(String), but SagaStateStore.delete's own javadoc says the recommended default is to let a completed
    // instance expire via MongoDB TTL instead, which happens entirely inside the database with no call this store ever
    // sees. So an instance that stays above the threshold and is retired that way keeps its entry until something else
    // reclaims the slot. That reclaiming is the eviction the latch does on access order (see the field below): whichever
    // saga id has gone the longest without being checked is what gets evicted when a new one arrives at capacity. That
    // is a statement about recency of check, not about which instance is still active: a continuously active instance
    // that is not saved again for RETAINED_EVENT_WARNING_LATCH_CAPACITY other distinct ids' worth of checks is evicted
    // just like an abandoned one would be, and re-warns on its next save. Package-private for the same reason as the
    // threshold above: a test exercising the capacity backstop builds exactly this many tracked instances instead of
    // duplicating the number.
    static final int RETAINED_EVENT_WARNING_LATCH_CAPACITY = 10_000;

    // Edge-triggered, least-recently-checked latch, keyed by saga id: present and true means "already warned while at
    // or above the threshold". An instance is removed the moment it drops back below the threshold (typically after a
    // stepWindow trim) or is deleted, so a later re-crossing warns again. Access-ordered (see the constructor) and
    // capped at RETAINED_EVENT_WARNING_LATCH_CAPACITY, protecting the RETAINED_EVENT_WARNING_LATCH_CAPACITY most
    // recently checked saga ids from eviction, not every instance that will eventually be saved again. A saga id not
    // checked again before that many OTHER distinct ids have been checked is evicted regardless of whether it is
    // still active, and re-warns on its own next save. This is an accepted trade-off for keeping a hard memory bound
    // rather than an unbounded, idle-expiry-only cache: the cost is a spurious re-warn under high churn, never a
    // missed one. Not a ConcurrentHashMap: LinkedHashMap's access-order mode is what gives the LRU property, and every
    // access to this field goes through the synchronized block in warnIfRetainedSizeCrossesThreshold or
    // delete(String).
    private final Map<String, Boolean> retainedEventWarningLatch = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > RETAINED_EVENT_WARNING_LATCH_CAPACITY;
        }
    };

    private final MongoOperations mongoOperations;
    private final String collectionName;
    private final Class<S> stateType;
    // Non-null only for a flow saga: used to serialize FlowState.received as CloudEvents (stable types, package-independent).
    private final @Nullable CloudEventConverter<Object> cloudEventConverter;

    /**
     * Creates a store for a core saga, whose state serializes with the application's {@code MongoConverter}.
     *
     * @param mongoOperations the {@link MongoOperations} used to read and write instance documents
     * @param collectionName  the collection the instances are stored in
     * @param stateType       the user state type, needed to read the stored state back into an object
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType) {
        this(mongoOperations, collectionName, stateType, null);
    }

    /**
     * Creates a store that, when {@code cloudEventConverter} is supplied, serializes a flow saga's {@code FlowState}
     * received events as CloudEvents so they round-trip by their stable CloudEvent type. Pass {@code null} for a
     * core saga.
     *
     * @param mongoOperations     the {@link MongoOperations} used to read and write instance documents
     * @param collectionName      the collection the instances are stored in
     * @param stateType           the user state type, needed to read the stored state back into an object
     * @param cloudEventConverter the converter used to (de)serialize a flow saga's received events, or {@code null}
     */
    @SuppressWarnings("unchecked")
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType, @Nullable CloudEventConverter<?> cloudEventConverter) {
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName cannot be null");
        this.stateType = Objects.requireNonNull(stateType, "stateType cannot be null");
        if (stateType == FlowState.class && cloudEventConverter == null) {
            // A flow saga's FlowState holds domain events, and this store serializes them as CloudEvents through the
            // converter so they round-trip by their stable CloudEvent type (package-independent). Without it, the field-by-
            // field FlowState path cannot run. The annotation path always supplies the converter; a hand-built store that
            // omits it would silently lose that package independence, so fail loud here instead.
            throw new IllegalArgumentException("a CloudEventConverter is required to store a flow saga's FlowState; use the four-argument constructor and pass the application's CloudEventConverter");
        }
        // Safe: the converter only ever sees domain events read out of a FlowState, whose element type is erased anyway.
        this.cloudEventConverter = (CloudEventConverter<Object>) cloudEventConverter;
        mongoOperations.getCollection(collectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(NEXT_TIMER_FIRES_AT)));
        mongoOperations.getCollection(collectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(UPDATED_AT)));
    }

    @Override
    public Optional<SagaEnvelope<S>> find(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        Document document = mongoOperations.findById(sagaId, Document.class, collectionName);
        return Optional.ofNullable(document).map(this::toEnvelope);
    }

    @Override
    public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        Objects.requireNonNull(envelope, "envelope cannot be null");
        Document document = toDocument(sagaId, envelope);
        if (expectedVersion == 0) {
            try {
                mongoOperations.insert(document, collectionName);
                return true;
            } catch (DuplicateKeyException e) {
                return false;
            }
        }
        Query query = Query.query(where(ID).is(sagaId).and(VERSION).is(expectedVersion));
        Document replaced = mongoOperations.findAndReplace(query, document, collectionName);
        return replaced != null;
    }

    @Override
    public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
        Objects.requireNonNull(now, "now cannot be null");
        Query query = Query.query(where(STATUS).is(SagaStatus.ACTIVE.name()).and(NEXT_TIMER_FIRES_AT).lte(now.toEpochMilli()))
                .limit(limit);
        // Project only the fields the poller needs to decide which timers are due. This deliberately excludes the state
        // (a flow saga's received log can be large), so the poll never pays to decode it. The executor re-loads the full
        // document with find(sagaId) before it processes a timer, which is the authoritative read the fire acts on.
        // The timestamps and currentStep are included even though the poller ignores them: an envelope is also a
        // SagaInstance, and every member of that view must be populated on any envelope a store hands back. They are
        // three longs and a string, and decode no state, so the cost the exclusion above protects against is untouched.
        projectEverySagaInstanceMember(query);
        return mongoOperations.find(query, Document.class, collectionName).stream().map(this::toEnvelope).toList();
    }

    @Override
    public List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
        Objects.requireNonNull(status, "status cannot be null");
        Objects.requireNonNull(updatedBefore, "updatedBefore cannot be null");
        if (limit < 1) {
            // A Mongo limit of 0 means "no limit", so a caller passing 0 would get the whole collection instead of
            // nothing. Reject it here rather than let that through as a surprise full scan.
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        Query query = Query.query(where(STATUS).is(status.name()).and(UPDATED_AT).lt(updatedBefore.toEpochMilli()))
                .with(Sort.by(Sort.Direction.ASC, UPDATED_AT))
                .limit(limit);
        // Projected exactly like the due-timer query: every SagaInstance member and no state. currentStep is a top-level
        // field, so observing a flow saga never decodes its received log. That also means this query cannot fail on an
        // instance whose state no longer decodes, because it never decodes any.
        projectEverySagaInstanceMember(query);
        return mongoOperations.find(query, Document.class, collectionName).stream().map(this::toEnvelope).toList();
    }

    // The fields backing SagaInstance, which is the whole observable surface of an instance: enough for both enumeration
    // queries and deliberately excluding the state, whose decode is the expensive and failure-prone part.
    private static void projectEverySagaInstanceMember(Query query) {
        query.fields().include(ID).include(STATUS).include(VERSION).include(TIMERS).include(NEXT_TIMER_FIRES_AT)
                .include(CURRENT_STEP).include(CREATED_AT).include(UPDATED_AT).include(COMPLETED_AT).include(STARTED)
                .include(FAILURE_INPUT).include(FAILURE_POSITION).include(FAILURE_FIRST_FAILED_AT).include(FAILURE_TYPE)
                .include(FAILURE_MESSAGE).include(FAILURE_RELEASED_AT);
    }

    @Override
    public void delete(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        mongoOperations.remove(Query.query(where(ID).is(sagaId)), collectionName);
        synchronized (retainedEventWarningLatch) {
            retainedEventWarningLatch.remove(sagaId);
        }
    }

    private Document toDocument(String sagaId, SagaEnvelope<S> envelope) {
        Document document = new Document(ID, sagaId)
                .append(STATUS, envelope.status().name())
                .append(VERSION, envelope.version());
        S state = envelope.state();
        if (state != null) {
            document.append(STATE, toStateValue(sagaId, state));
        }
        List<Document> timers = new ArrayList<>();
        for (TimerEntry timer : envelope.timers()) {
            timers.add(new Document(TIMER_NAME, timer.name()).append(TIMER_FIRES_AT, timer.firesAtEpochMilli()));
        }
        document.append(TIMERS, timers);
        envelope.earliestTimerFiresAtEpochMilli().ifPresent(next -> document.append(NEXT_TIMER_FIRES_AT, next));
        // Denormalized beside nextTimerFiresAt and for the same reason: it lets a query answer SagaInstance.currentStep()
        // without decoding the state, which for a flow saga means not decoding its received log.
        if (envelope.currentStep() != null) {
            document.append(CURRENT_STEP, envelope.currentStep());
        }
        document.append(STREAM_WATERMARKS, new Document(new LinkedHashMap<>(envelope.streamWatermarks())));
        if (envelope.positionWatermark() != null) {
            document.append(POSITION_WATERMARK, envelope.positionWatermark());
        }
        appendInstant(document, CREATED_AT, envelope.createdAt());
        appendInstant(document, UPDATED_AT, envelope.updatedAt());
        appendInstant(document, COMPLETED_AT, envelope.completedAt());
        // Written only when false, which is the rare case: an instance that failed before it ever started. A document
        // written before 0.34.0 has no such field, and a missing field reads back as true, which is what every one of
        // them is.
        if (!envelope.started()) {
            document.append(STARTED, false);
        }
        SagaFailure failure = envelope.failure();
        if (failure != null) {
            document.append(FAILURE_INPUT, failure.input())
                    .append(FAILURE_POSITION, failure.position())
                    .append(FAILURE_FIRST_FAILED_AT, failure.firstFailedAt().toEpochMilli())
                    .append(FAILURE_TYPE, failure.failureType());
            if (failure.failureMessage() != null) {
                document.append(FAILURE_MESSAGE, failure.failureMessage());
            }
            appendInstant(document, FAILURE_RELEASED_AT, failure.releasedAt());
        }
        return document;
    }

    // Serialize the state. A flow saga's FlowState is written field by field with its received events as CloudEvents (see
    // flowStateToDocument), so events round-trip by their stable CloudEvent type. Any other state (a core saga's
    // own model) goes through convertToMongoType, exactly like the snapshot store: a scalar stays a scalar and a
    // POJO/record becomes a sub-document.
    private Object toStateValue(String sagaId, S state) {
        if (cloudEventConverter != null && state instanceof FlowStateImpl<?> flowState) {
            return flowStateToDocument(sagaId, flowState);
        }
        if (cloudEventConverter != null && state instanceof FlowState<?>) {
            // A flow saga's state is always the executor's FlowStateImpl, which the read path (readState) reconstructs field
            // by field. A different FlowState implementation would serialize generically here yet still be read back as a
            // flow document, corrupting the round-trip, so reject it rather than mis-serialize it silently.
            throw new IllegalArgumentException("a flow saga store can only persist the flow executor's FlowState (FlowStateImpl), got "
                    + state.getClass().getName());
        }
        return mongoOperations.getConverter().convertToMongoType(state);
    }

    private Document flowStateToDocument(String sagaId, FlowStateImpl<?> flowState) {
        warnIfRetainedSizeCrossesThreshold(sagaId, flowState.received().size());
        Document document = new Document();
        if (flowState.currentStep() != null) {
            document.append(FLOW_CURRENT_STEP, flowState.currentStep());
        }
        document.append(FLOW_WINDOW_START, flowState.windowStart());
        document.append(FLOW_STEP_ENTRY_INDEX, flowState.stepEntryIndex());
        document.append(FLOW_COMPLETED, flowState.completed());
        if (flowState.previousStep() != null) {
            document.append(FLOW_PREVIOUS_STEP, flowState.previousStep());
        }
        document.append(FLOW_PREVIOUS_STEP_ENTRY_INDEX, flowState.previousStepEntryIndex());
        document.append(FLOW_LAST_ACTION, flowState.lastAction().name());
        document.append(FLOW_MATCHED_BRANCH_INDEX, flowState.matchedBranchIndex());
        StepConditionProgress progress = flowState.stepConditionProgress();
        if (progress != null) {
            document.append(FLOW_STEP_CONDITION_PROGRESS, new Document(FLOW_LEAF_FINGERPRINT, progress.leafFingerprint())
                    .append(FLOW_MATCH_COUNTS, progress.matchCounts()));
        }
        List<String> received = new ArrayList<>();
        for (Object event : flowState.received()) {
            received.add(toCloudEventJson(requireConverter().toCloudEvent(event)));
        }
        document.append(FLOW_RECEIVED, received);
        return document;
    }

    // Edge-triggered: warns once when an instance's retained-event count crosses the threshold from below, stays silent
    // on every subsequent save while it remains at or above it, and warns again only after a later save has carried it
    // back below the threshold (typically a stepWindow trim) and it crosses again. See the latch fields' comments for
    // the memory-safety argument.
    // Package-private so a test can exercise the latch's edge-triggering and capacity backstop directly, with plain
    // counts, instead of building enough real retained events to cross the threshold thousands of times over.
    void warnIfRetainedSizeCrossesThreshold(String sagaId, int retainedEventCount) {
        boolean shouldWarn;
        // LinkedHashMap is not thread-safe, and SagaStateStore supports concurrent saves, so the whole read-modify
        // decision is locked rather than just the individual map calls. It costs a monitor per save, negligible next
        // to the Mongo round trip compareAndSave already pays.
        synchronized (retainedEventWarningLatch) {
            if (retainedEventCount < RETAINED_EVENT_WARNING_THRESHOLD) {
                retainedEventWarningLatch.remove(sagaId);
                return;
            }
            // get(), not containsKey(): get() is what LinkedHashMap's access-order mode uses to mark this instance as
            // recently used, so an already-tracked instance that keeps being saved is never the eviction target below.
            if (retainedEventWarningLatch.get(sagaId) != null) {
                shouldWarn = false;
            } else {
                // put() may evict the least-recently-used entry via removeEldestEntry if the latch is at capacity.
                retainedEventWarningLatch.put(sagaId, Boolean.TRUE);
                shouldWarn = true;
            }
        }
        if (shouldWarn) {
            log.warn("Flow saga instance '{}' has retained {} received events, at or above the warning threshold of {}. " +
                            "Consider capping the flow's step with stepWindow(...) to trim what it retains, or the document " +
                            "risks growing toward MongoDB's 16 MB document limit.",
                    sagaId, retainedEventCount, RETAINED_EVENT_WARNING_THRESHOLD);
        }
    }

    // Package-private so a test can assert the capacity backstop's actual guarantee, that the latch never grows past
    // RETAINED_EVENT_WARNING_LATCH_CAPACITY. Synchronized like every other access to this LinkedHashMap.
    int retainedEventWarningLatchSize() {
        synchronized (retainedEventWarningLatch) {
            return retainedEventWarningLatch.size();
        }
    }

    private SagaEnvelope<S> toEnvelope(Document document) {
        String sagaId = document.getString(ID);
        S state = readState(document.get(STATE));
        SagaStatus status = SagaStatus.valueOf(document.getString(STATUS));
        long version = document.getLong(VERSION);

        List<TimerEntry> timers = new ArrayList<>();
        List<Document> timerDocuments = document.getList(TIMERS, Document.class, List.of());
        for (Document timer : timerDocuments) {
            timers.add(new TimerEntry(timer.getString(TIMER_NAME), timer.getLong(TIMER_FIRES_AT)));
        }

        Map<String, Long> streamWatermarks = new LinkedHashMap<>();
        Document watermarksDocument = document.get(STREAM_WATERMARKS, Document.class);
        if (watermarksDocument != null) {
            for (Map.Entry<String, Object> entry : watermarksDocument.entrySet()) {
                streamWatermarks.put(entry.getKey(), ((Number) entry.getValue()).longValue());
            }
        }
        Long positionWatermark = document.containsKey(POSITION_WATERMARK) ? document.getLong(POSITION_WATERMARK) : null;

        // A document written before 0.34.0 carries no started flag, and every instance in one had started, so a missing
        // field means true rather than false.
        boolean started = !document.containsKey(STARTED) || document.getBoolean(STARTED);

        // currentStep is only honoured when the state was projected away; with a state present the envelope re-derives it.
        return new SagaEnvelope<>(sagaId, state, status, version, timers, streamWatermarks, positionWatermark,
                readInstant(document, CREATED_AT), readInstant(document, UPDATED_AT), readInstant(document, COMPLETED_AT),
                document.getString(CURRENT_STEP), started, toFailure(document));
    }

    private static @Nullable SagaFailure toFailure(Document document) {
        String input = document.getString(FAILURE_INPUT);
        if (input == null) {
            return null;
        }
        return new SagaFailure(input,
                ((Number) Objects.requireNonNull(document.get(FAILURE_POSITION), "failurePosition")).longValue(),
                Objects.requireNonNull(readInstant(document, FAILURE_FIRST_FAILED_AT), "failureFirstFailedAt"),
                Objects.requireNonNull(document.getString(FAILURE_TYPE), "failureType"),
                document.getString(FAILURE_MESSAGE),
                readInstant(document, FAILURE_RELEASED_AT));
    }

    @SuppressWarnings("unchecked")
    private @Nullable S readState(@Nullable Object stateField) {
        if (stateField == null) {
            return null;
        }
        if (cloudEventConverter != null && stateField instanceof Document flowDocument) {
            return (S) flowStateFromDocument(flowDocument);
        }
        MongoConverter converter = mongoOperations.getConverter();
        if (stateField instanceof Document stateDocument) {
            return converter.read(stateType, stateDocument);
        }
        return (S) converter.getConversionService().convert(stateField, stateType);
    }

    private FlowStateImpl<Object> flowStateFromDocument(Document document) {
        List<Object> received = new ArrayList<>();
        for (String json : document.getList(FLOW_RECEIVED, String.class, List.of())) {
            received.add(requireConverter().toDomainEvent(fromCloudEventJson(json)));
        }
        return new FlowStateImpl<>(
                document.getString(FLOW_CURRENT_STEP),
                received,
                // A document written before this field existed never dropped anything, so its tail always started right
                // after the pinned initiating event, position 1, not 0 (0 only holds for a never-started FlowStateImpl.initial()).
                document.getInteger(FLOW_WINDOW_START, 1),
                document.getInteger(FLOW_STEP_ENTRY_INDEX, 0),
                document.getBoolean(FLOW_COMPLETED, false),
                document.getString(FLOW_PREVIOUS_STEP),
                // -1 is the record's own "not known" value, so a document written before this field existed reads back as an
                // instance whose window-condition reaction falls back to the whole retained history, which is what such a
                // reaction saw when that document was written.
                document.getInteger(FLOW_PREVIOUS_STEP_ENTRY_INDEX, -1),
                ActionKind.valueOf(document.getString(FLOW_LAST_ACTION)),
                document.getInteger(FLOW_MATCHED_BRANCH_INDEX, -1),
                readStepConditionProgress(document));
    }

    // Absent counts read back as null, the record's own "not known" value, so the flow lowering counts the step's window
    // instead. That is what a document written before this field existed gets, and it is also why a sub-document missing its
    // fingerprint is read as absent rather than thrown on, since the counts cannot be matched to a declaration without it.
    private static @Nullable StepConditionProgress readStepConditionProgress(Document document) {
        Document progress = document.get(FLOW_STEP_CONDITION_PROGRESS, Document.class);
        if (progress == null) {
            return null;
        }
        String fingerprint = progress.getString(FLOW_LEAF_FINGERPRINT);
        return fingerprint == null ? null
                : new StepConditionProgress(fingerprint, progress.getList(FLOW_MATCH_COUNTS, Integer.class, List.of()));
    }

    private CloudEventConverter<Object> requireConverter() {
        return Objects.requireNonNull(cloudEventConverter, "cloudEventConverter is required to (de)serialize a flow saga's received events");
    }

    private static String toCloudEventJson(CloudEvent cloudEvent) {
        return new String(CLOUD_EVENT_JSON_FORMAT.serialize(cloudEvent), StandardCharsets.UTF_8);
    }

    private static CloudEvent fromCloudEventJson(String json) {
        return CLOUD_EVENT_JSON_FORMAT.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }

    private static void appendInstant(Document document, String field, @Nullable Instant instant) {
        if (instant != null) {
            document.append(field, instant.toEpochMilli());
        }
    }

    private static @Nullable Instant readInstant(Document document, String field) {
        return document.containsKey(field) ? Instant.ofEpochMilli(document.getLong(field)) : null;
    }
}
