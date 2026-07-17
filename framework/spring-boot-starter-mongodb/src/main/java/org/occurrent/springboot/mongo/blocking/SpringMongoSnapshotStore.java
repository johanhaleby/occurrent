/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.blocking;

import org.bson.Document;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Objects;
import java.util.Optional;

/**
 * A {@link SnapshotStore} backed by MongoDB. Each snapshot is stored as one document in the store's collection, keyed by
 * the snapshot key, carrying the folded {@code state}, the {@code version} it was folded up to, and the
 * {@code schemaVersion}. The state is serialized with the same {@code MongoConverter} the rest of the application uses,
 * so any type Spring Data can map is supported.
 *
 * @param <S> The snapshot state type.
 */
@NullMarked
public final class SpringMongoSnapshotStore<S extends @Nullable Object> implements SnapshotStore<S> {

    private static final String VERSION = "version";
    private static final String SCHEMA_VERSION = "schemaVersion";
    private static final String STATE = "state";

    private final MongoOperations mongoOperations;
    private final Class<S> stateType;
    private final String collectionName;

    /**
     * Create a MongoDB-backed snapshot store.
     *
     * @param mongoOperations The {@link MongoOperations} used to read and write snapshot documents.
     * @param stateType       The state type, needed to read the stored state back into an object.
     * @param collectionName  The collection the snapshots are stored in.
     */
    public SpringMongoSnapshotStore(MongoOperations mongoOperations, Class<S> stateType, String collectionName) {
        this.mongoOperations = Objects.requireNonNull(mongoOperations, "mongoOperations cannot be null");
        this.stateType = Objects.requireNonNull(stateType, "stateType cannot be null");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName cannot be null");
    }

    @Override
    public Optional<Snapshot<S>> findLatest(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        Document document = mongoOperations.findById(key, Document.class, collectionName);
        if (document == null) {
            return Optional.empty();
        }
        long version = document.getLong(VERSION);
        int schemaVersion = document.getInteger(SCHEMA_VERSION);
        Object stateField = document.get(STATE);
        S state = stateField instanceof Document stateDocument ? mongoOperations.getConverter().read(stateType, stateDocument) : null;
        return Optional.of(new Snapshot<>(state, version, schemaVersion));
    }

    @Override
    public void save(String key, Snapshot<S> snapshot) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(snapshot, "snapshot cannot be null");
        Document document = new Document("_id", key)
                .append(VERSION, snapshot.version())
                .append(SCHEMA_VERSION, snapshot.schemaVersion());
        S state = snapshot.state();
        if (state != null) {
            Document stateDocument = new Document();
            mongoOperations.getConverter().write(state, stateDocument);
            document.append(STATE, stateDocument);
        }
        mongoOperations.save(document, collectionName);
    }

    @Override
    public void delete(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        mongoOperations.remove(Query.query(Criteria.where("_id").is(key)), collectionName);
    }
}
