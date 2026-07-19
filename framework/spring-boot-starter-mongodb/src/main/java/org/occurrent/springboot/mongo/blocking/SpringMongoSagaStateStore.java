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

package org.occurrent.springboot.mongo.blocking;

import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.Status;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

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
 * carrying the user {@code state} (serialized with the application's {@code MongoConverter}, like the snapshot store), the
 * lifecycle {@code status}, the optimistic-lock {@code version}, the pending timers, and the dedup watermarks. A top-level
 * indexed {@code nextTimerFiresAt} (the earliest pending timer) makes {@link #findWithDueTimers(Instant, int)} an indexed
 * query.
 * <p>
 * {@link #compareAndSave} is atomic: a new instance is inserted (a duplicate {@code _id} loses), and an update replaces
 * the document only when its stored {@code version} still equals the expected one, via a single {@code findAndReplace}.
 *
 * @param <S> the user state type
 */
@NullMarked
public final class SpringMongoSagaStateStore<S extends @Nullable Object> implements SagaStateStore<S> {

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

    private final MongoOperations mongoOperations;
    private final String collectionName;
    private final Class<S> stateType;

    /**
     * @param mongoOperations the {@link MongoOperations} used to read and write instance documents
     * @param collectionName  the collection the instances are stored in
     * @param stateType       the user state type, needed to read the stored state back into an object
     */
    public SpringMongoSagaStateStore(MongoOperations mongoOperations, String collectionName, Class<S> stateType) {
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName cannot be null");
        this.stateType = Objects.requireNonNull(stateType, "stateType cannot be null");
        mongoOperations.getCollection(collectionName).createIndex(Indexes.compoundIndex(Indexes.ascending(STATUS), Indexes.ascending(NEXT_TIMER_FIRES_AT)));
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
        Query query = Query.query(where(STATUS).is(Status.ACTIVE.name()).and(NEXT_TIMER_FIRES_AT).lte(now.toEpochMilli()))
                .limit(limit);
        return mongoOperations.find(query, Document.class, collectionName).stream().map(this::toEnvelope).toList();
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
            document.append(STATE, mongoOperations.getConverter().convertToMongoType(state));
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

    private SagaEnvelope<S> toEnvelope(Document document) {
        String sagaId = document.getString(ID);
        Status status = Status.valueOf(document.getString(STATUS));
        long version = document.getLong(VERSION);
        S state = readState(document.get(STATE));

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
        MongoConverter converter = mongoOperations.getConverter();
        if (stateField instanceof Document stateDocument) {
            return converter.read(stateType, stateDocument);
        }
        return (S) converter.getConversionService().convert(stateField, stateType);
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
