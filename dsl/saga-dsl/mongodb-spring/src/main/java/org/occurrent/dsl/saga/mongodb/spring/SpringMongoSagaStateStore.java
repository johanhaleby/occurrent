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
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
    private static final String STREAM_WATERMARKS = "streamWatermarks";
    private static final String POSITION_WATERMARK = "positionWatermark";
    private static final String CREATED_AT = "createdAt";
    private static final String UPDATED_AT = "updatedAt";
    private static final String COMPLETED_AT = "completedAt";

    // Field names inside a persisted FlowState document.
    private static final String FLOW_CURRENT_STEP = "currentStep";
    private static final String FLOW_WINDOW_START = "windowStart";
    private static final String FLOW_STEP_ENTRY_INDEX = "stepEntryIndex";
    private static final String FLOW_COMPLETED = "completed";
    private static final String FLOW_PREVIOUS_STEP = "previousStep";
    private static final String FLOW_LAST_ACTION = "lastAction";
    private static final String FLOW_MATCHED_BRANCH_INDEX = "matchedBranchIndex";
    private static final String FLOW_RECEIVED = "received";

    private static final EventFormat CLOUD_EVENT_JSON_FORMAT = Objects.requireNonNull(
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE), "CloudEvents JSON format must be on the classpath");

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
        // The timestamps are included even though the poller ignores them: an envelope is also a SagaInstance, whose
        // lifecycle accessors would otherwise read null here while the in-memory store (which cannot project) returns
        // them, making one SPI method hand back differently-populated instances per store. They are three longs and
        // decode no state, so the cost the exclusion above protects against is untouched.
        query.fields().include(ID).include(STATUS).include(TIMERS).include(NEXT_TIMER_FIRES_AT).include(VERSION)
                .include(CREATED_AT).include(UPDATED_AT).include(COMPLETED_AT);
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
        // No field projection here, unlike findWithDueTimers: SagaInstance.currentStep() is read off the state, so an
        // instance has to come back whole. The {status, updatedAt} index still serves the predicate and the sort.
        return mongoOperations.find(query, Document.class, collectionName).stream()
                .map(this::toEnvelopeToleratingUndecodableState)
                .toList();
    }

    @Override
    public void delete(String sagaId) {
        Objects.requireNonNull(sagaId, "sagaId cannot be null");
        mongoOperations.remove(Query.query(where(ID).is(sagaId)), collectionName);
    }

    private Document toDocument(String sagaId, SagaEnvelope<S> envelope) {
        Document document = new Document(ID, sagaId)
                .append(STATUS, envelope.status().name())
                .append(VERSION, envelope.version());
        S state = envelope.state();
        if (state != null) {
            document.append(STATE, toStateValue(state));
        }
        List<Document> timers = new ArrayList<>();
        for (TimerEntry timer : envelope.timers()) {
            timers.add(new Document(TIMER_NAME, timer.name()).append(TIMER_FIRES_AT, timer.firesAtEpochMilli()));
        }
        document.append(TIMERS, timers);
        envelope.earliestTimerFiresAtEpochMilli().ifPresent(next -> document.append(NEXT_TIMER_FIRES_AT, next));
        document.append(STREAM_WATERMARKS, new Document(new LinkedHashMap<>(envelope.streamWatermarks())));
        if (envelope.positionWatermark() != null) {
            document.append(POSITION_WATERMARK, envelope.positionWatermark());
        }
        appendInstant(document, CREATED_AT, envelope.createdAt());
        appendInstant(document, UPDATED_AT, envelope.updatedAt());
        appendInstant(document, COMPLETED_AT, envelope.completedAt());
        return document;
    }

    // Serialize the state. A flow saga's FlowState is written field by field with its received events as CloudEvents (see
    // flowStateToDocument), so events round-trip by their stable CloudEvent type. Any other state (a core saga's
    // own model) goes through convertToMongoType, exactly like the snapshot store: a scalar stays a scalar and a
    // POJO/record becomes a sub-document.
    private Object toStateValue(S state) {
        if (cloudEventConverter != null && state instanceof FlowStateImpl<?> flowState) {
            return flowStateToDocument(flowState);
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

    private Document flowStateToDocument(FlowStateImpl<?> flowState) {
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
        document.append(FLOW_LAST_ACTION, flowState.lastAction().name());
        document.append(FLOW_MATCHED_BRANCH_INDEX, flowState.matchedBranchIndex());
        List<String> received = new ArrayList<>();
        for (Object event : flowState.received()) {
            received.add(toCloudEventJson(requireConverter().toCloudEvent(event)));
        }
        document.append(FLOW_RECEIVED, received);
        return document;
    }

    private SagaEnvelope<S> toEnvelope(Document document) {
        return toEnvelope(document, readState(document.get(STATE)));
    }

    /**
     * Like {@link #toEnvelope(Document)}, but a state that can no longer be decoded yields an envelope with a
     * {@code null} state instead of throwing.
     * <p>
     * Only {@code findByStatus} uses this, because observation must not be the thing that breaks when an instance goes
     * bad. A stored received event whose class was renamed away, or a state document that no longer matches its type,
     * would otherwise make one poisoned instance throw for every caller enumerating the collection, taking the whole
     * progress view down exactly when someone is looking for what is wrong, and leaving no way out except deleting the
     * document through the SPI this feature exists to avoid using. The degraded row still answers every
     * {@code SagaInstance} member except {@code currentStep()}, and in a stuck-instance report it is likely the most
     * interesting row of all. This mirrors how the Mongo snapshot stores degrade rather than fail a command.
     * <p>
     * {@code find(sagaId)} deliberately keeps throwing: the executor loads an instance in order to fold and save it, and
     * silently handing it a null state there would restart the process from its initial state and re-dispatch commands.
     */
    private SagaEnvelope<S> toEnvelopeToleratingUndecodableState(Document document) {
        S state;
        try {
            // Scoped to the decode alone. The documents are already materialized by the query above, so nothing in here
            // does I/O and a connectivity failure still propagates from the find itself.
            state = readState(document.get(STATE));
        } catch (RuntimeException e) {
            log.warn("Could not decode the state of saga instance '{}' in collection '{}', reporting it without state. Its lifecycle is still observable, but currentStep() reads null.",
                    document.getString(ID), collectionName, e);
            state = null;
        }
        return toEnvelope(document, state);
    }

    private SagaEnvelope<S> toEnvelope(Document document, S state) {
        String sagaId = document.getString(ID);
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

        return new SagaEnvelope<>(sagaId, state, status, version, timers, streamWatermarks, positionWatermark,
                readInstant(document, CREATED_AT), readInstant(document, UPDATED_AT), readInstant(document, COMPLETED_AT));
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
                ActionKind.valueOf(document.getString(FLOW_LAST_ACTION)),
                document.getInteger(FLOW_MATCHED_BRANCH_INDEX, -1));
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
